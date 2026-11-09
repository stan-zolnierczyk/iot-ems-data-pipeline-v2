"""
Silver Layer ETL for the IoT Energy Management System.

Transforms raw Bronze-layer telemetry from InfluxDB 3 (SQL) into a validated,
cleaned dataset on a regular 1-minute grid, writing results to the
power_clean, energy_clean and battery_soc_clean tables.

Validation rules (per series, grouped by device + measurement_type):
  - domain-aware range filtering,
  - spike rejection (rolling median on counters, max step on battery SoC),
  - daily monotonicity of energy counters, with the reset to 0 allowed
    only around local midnight (Europe/Warsaw).
Rejected samples and missing minutes are filled: power by linear
interpolation, counters, daily peak power and SoC with the previous valid value.
Power and energy consumption are derived from the cleaned components.

The script uses incremental loading driven by the last processed timestamp of each
Silver table, making it immune to a random-frequency runs (the script is run
non-deterministically, despite being scheduled to run every 5 minutes via GitHub Actions).
Each run reprocesses and overwrites a short lookback window (writes are idempotent),
and starts the filters from the last cleaned value of each series, so validation
state carries over between runs.
"""

import sys
import os
import numpy as np
import pandas as pd
from influxdb_client_3 import InfluxDBClient3

# Ensure the script can import local modules when running in CI/CD environments
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import INFLUX_HOST, INFLUX_TOKEN, INFLUX_DATABASE, INFLUX_ORG

# A counter value exceeding the rolling median by more than this (Wh) is a spike.
SPIKE_THRESHOLD_WH = 1000

# Counter value (Wh) or daily peak power (W) treated as "close to 0" after the midnight reset.
RESET_MAX_VALUE = 1000

# Maximum plausible battery SoC change per minute (%). Physical limit is ~0.8 %/min (5 kW / 10 kWh).
SOC_MAX_STEP_PER_MIN = 2

# Already processed window that each run reprocesses and overwrites with full context
LOOKBACK = pd.Timedelta(minutes=15)

# How far back to look for the last cleaned value of a series (filters' starting point)
SEED_MAX_AGE = pd.Timedelta(days=1)

# Daily counters reset at local midnight (inverter and CLU clocks may differ by a few minutes)
LOCAL_TZ = 'Europe/Warsaw'

# Domain ranges per (device, measurement_type)
POWER_RANGES = {
    ('SUN2000', 'activePower'): (0, 12000),               # PV production (inverter AC + LUNA2000)
    ('SUN2000', 'peakPower'): (0, 12000),
    ('LUNA2000', 'chargeDischargePower'): (-5000, 5000),  # >0 charging, <0 discharging
    ('DTSU666', 'powerBalance'): (-40000, 12000),         # >0 export, <0 import (40 kW grid connection)
}
ENERGY_RANGES = {
    ('SUN2000', 'production'): (0, 120000),
    ('DTSU666', 'import'): (0, 120000),
    ('DTSU666', 'export'): (0, 120000),
    ('LUNA2000', 'currentDayCharge'): (0, 25000),
    ('LUNA2000', 'currentDayDischarge'): (0, 25000),
}
SOC_RANGE = (0, 100)

CALCULATED_DEVICE = 'EMS_Calculated'


def to_sql_time(ts):
    """Formats a UTC timestamp as an SQL time literal."""
    return ts.strftime('%Y-%m-%dT%H:%M:%SZ')


def query_silver(client, query):
    """
    Runs a query on a Silver table. Returns an empty DataFrame only if the table
    does not exist yet (first run); any other error is raised.
    """
    try:
        return client.query(query, mode='pandas')
    except Exception as e:
        if 'not found' not in str(e).lower():
            raise
        print(f"Silver table not found yet: {e}")
        return pd.DataFrame()


def get_last_timestamp(client, clean_table):
    """
    Queries InfluxDB for the last processed record in a Silver table
    to enable Incremental Loading.
    """
    query = f'''
    SELECT MAX(time) AS last_time
    FROM {clean_table}
    WHERE time > now() - INTERVAL '7 days'
    '''
    df = query_silver(client, query)
    if df.empty or pd.isna(df['last_time'].iloc[0]):
        return None
    return pd.to_datetime(df['last_time'], utc=True).iloc[0]


def fetch_seed(client, clean_table, start):
    """
    Fetches the last cleaned value of each series before start. Prepended to the Bronze rows,
    it gives the filters a validated starting point carried over from the previous run.
    Derived series are skipped: they are recalculated from the cleaned components.
    """
    query = f'''
    SELECT device, measurement_type,
           selector_last(value, time)['time'] AS time,
           selector_last(value, time)['value'] AS value
    FROM {clean_table}
    WHERE time < '{to_sql_time(start)}'
      AND time >= '{to_sql_time(start - SEED_MAX_AGE)}'
      AND device <> '{CALCULATED_DEVICE}'
    GROUP BY device, measurement_type
    '''
    df = query_silver(client, query)
    if not df.empty:
        df['time'] = pd.to_datetime(df['time'], utc=True)
    return df


def fetch_bronze(client, table, start, exclude_types=()):
    """
    Fetches raw Bronze rows (time, device, measurement_type, value) since start.
    Rows timestamped in the future (wrong device clock) are ignored.
    """
    exclude = ''.join(f" AND measurement_type <> '{t}'" for t in exclude_types)
    query = f'''
    SELECT time, device, measurement_type, value
    FROM "{table}"
    WHERE time >= '{to_sql_time(start)}' AND time <= now(){exclude}
    ORDER BY time
    '''
    df = client.query(query, mode='pandas')
    if not df.empty:
        df['time'] = pd.to_datetime(df['time'], utc=True)
    return df


def to_minute_grid(df):
    """
    Pivots long rows to wide format, one column per (device, measurement_type),
    on a regular 1-minute grid. Missing minutes become NaN.
    """
    wide = df.assign(time=df['time'].dt.floor('1min')).pivot_table(
        index='time', columns=['device', 'measurement_type'], values='value', aggfunc='last')
    return wide.asfreq('1min')


def filter_range(series, low, high):
    """Replaces values outside the domain range with NaN."""
    return series.where(series.between(low, high))


def counter_cycles(series):
    """
    Numbers the cycles of a daily counter; a new cycle starts at the midnight reset.
    A drop is a reset only if the local date changed since the previous valid value,
    or the sample is within 23:50-00:15 local time and close to 0.
    """
    valid = series.dropna()
    local_index = valid.index.tz_convert(LOCAL_TZ)
    minutes_of_day = local_index.hour * 60 + local_index.minute
    in_reset_window = pd.Series((minutes_of_day >= 23 * 60 + 50) | (minutes_of_day <= 15), index=valid.index)
    local_dates = pd.Series(local_index.date, index=valid.index)

    dropped = valid < valid.shift()
    is_reset = dropped & ((local_dates != local_dates.shift()) | (in_reset_window & (valid <= RESET_MAX_VALUE)))
    return is_reset.reindex(series.index, fill_value=False).cumsum()


def filter_upward_spikes(series, cycles, window=5, threshold=SPIKE_THRESHOLD_WH):
    """
    Rejects (NaN) upward spikes: values exceeding the centered rolling median by more than
    threshold. The median is computed within each counter cycle, so it does not mix values
    from before and after the midnight reset. Downward drops are handled by enforce_daily_monotonic.
    """
    rolling_median = (series.groupby(cycles).rolling(window, center=True, min_periods=1)
                      .median().droplevel(0))
    return series.mask(series - rolling_median > threshold)


def filter_steps(series, max_step_per_min):
    """
    Rejects (NaN) values that differ from the previous valid value by more than
    max_step_per_min multiplied by the minutes elapsed since that value.
    """
    cleaned = series.copy()
    last_time, last_value = None, None
    for ts, value in series.items():
        if pd.isna(value):
            continue
        if last_value is not None:
            minutes = (ts - last_time) / pd.Timedelta(minutes=1)
            if abs(value - last_value) > max_step_per_min * minutes:
                cleaned.loc[ts] = np.nan
                continue
        last_time, last_value = ts, value
    return cleaned


def enforce_daily_monotonic(series, cycles):
    """
    Daily counters must not decrease within a cycle: values below the highest
    previous value of the same cycle are rejected (NaN).
    """
    running_max = series.groupby(cycles).cummax().groupby(cycles).ffill()
    previous_max = running_max.groupby(cycles).shift()
    return series.mask(series < previous_max)


def to_long(wide, write_from):
    """
    Melts the wide grid back to long format (time index; value, device, measurement_type),
    keeping only rows from write_from onwards.
    """
    long = wide[wide.index >= write_from].melt(ignore_index=False, value_name='value')
    long.index.name = 'time'
    return long.dropna(subset=['value'])[['value', 'device', 'measurement_type']]


def write_clean(client, df, table):
    """Writes a long-format DataFrame to a Silver table."""
    if df.empty:
        return
    client.write(record=df, data_frame_measurement_name=table,
                 data_frame_tag_columns=['device', 'measurement_type'])
    print(f"Processed {len(df)} records into {table}.")


def process_power(df):
    """Validates power series and derives instantaneous consumption."""
    wide = to_minute_grid(df)
    for col, (low, high) in POWER_RANGES.items():
        if col not in wide.columns:
            wide[col] = np.nan
        wide[col] = filter_range(wide[col], low, high)

    # Peak power is a daily maximum: same reset rule as the energy counters
    peak = ('SUN2000', 'peakPower')
    peak_power = enforce_daily_monotonic(wide[peak], counter_cycles(wide[peak])).ffill()

    # Fill rejected samples and missing minutes: power linearly, daily peak with the previous value
    wide = wide[list(POWER_RANGES)].interpolate(method='linear')
    wide[peak] = peak_power

    # Consumption = inverter AC output (PV - battery flow) - grid balance; cannot be negative
    ac_power = wide[('SUN2000', 'activePower')] - wide[('LUNA2000', 'chargeDischargePower')]
    wide[(CALCULATED_DEVICE, 'powerConsumption')] = (ac_power - wide[('DTSU666', 'powerBalance')]).clip(lower=0)
    return wide


def process_energy(df):
    """Validates daily energy counters and derives daily consumption."""
    wide = to_minute_grid(df)
    for col, (low, high) in ENERGY_RANGES.items():
        if col not in wide.columns:
            wide[col] = np.nan
        series = filter_range(wide[col], low, high)
        cycles = counter_cycles(series)
        wide[col] = enforce_daily_monotonic(filter_upward_spikes(series, cycles), cycles)

    # Fill rejected samples and missing minutes with the previous valid value
    wide = wide[list(ENERGY_RANGES)].ffill()

    # Consumption = inverter AC energy (includes battery discharge) + import - export.
    # Derived from already cleaned counters, so no extra spike filtering is needed.
    wide[(CALCULATED_DEVICE, 'consumption')] = (wide[('SUN2000', 'production')] + wide[('DTSU666', 'import')]
                                                - wide[('DTSU666', 'export')])
    return wide


def process_battery_soc(df):
    """Validates battery state of charge."""
    wide = to_minute_grid(df)
    col = ('LUNA2000', 'stateOfCharge')
    if col not in wide.columns:
        return wide.iloc[0:0]
    series = filter_range(wide[col], *SOC_RANGE)
    wide[col] = filter_steps(series, SOC_MAX_STEP_PER_MIN)
    return wide[[col]].ffill()


# (Bronze table, Silver table, Bronze types derived in Silver instead, processing function)
SILVER_TABLES = [
    ('power', 'power_clean', ['powerConsumption'], process_power),
    ('energy', 'energy_clean', ['consumption'], process_energy),
    ('battery_soc', 'battery_soc_clean', [], process_battery_soc),
]


def process_table(client, bronze_table, clean_table, exclude_types, process):
    """Incrementally validates one Bronze table into its Silver table."""
    # 1. INCREMENTAL LOAD LOGIC
    last_ts = get_last_timestamp(client, clean_table)

    if last_ts is not None:
        start = last_ts - LOOKBACK
        print(f"{clean_table}: incremental load, reprocessing since {to_sql_time(start)}")
    else:
        start = pd.Timestamp.now(tz='UTC') - pd.Timedelta(hours=1)
        print(f"{clean_table}: no previous data found. Performing full load from last 1 hour.")

    # 2. FETCH (Bronze rows, preceded by the last cleaned value of each series)
    df_bronze = fetch_bronze(client, bronze_table, start, exclude_types)
    if df_bronze.empty:
        print(f"{clean_table}: no new data found in Bronze layer.")
        return
    df_seed = fetch_seed(client, clean_table, start)
    df = pd.concat([df_seed, df_bronze], ignore_index=True) if not df_seed.empty else df_bronze

    # 3. VALIDATE & WRITE (rows before start are context only)
    write_clean(client, to_long(process(df), start), clean_table)


def process_silver_layer():
    """
    ETL Process: Bronze to Silver Layer.
    Handles data validation, cleaning, and normalization for IoT power, energy and battery metrics.
    """
    client = InfluxDBClient3(host=INFLUX_HOST, token=INFLUX_TOKEN,
                             database=INFLUX_DATABASE, org=INFLUX_ORG)
    try:
        for bronze_table, clean_table, exclude_types, process in SILVER_TABLES:
            process_table(client, bronze_table, clean_table, exclude_types, process)
        print("Silver layer update successful.")
    finally:
        client.close()

if __name__ == "__main__":
    process_silver_layer()
