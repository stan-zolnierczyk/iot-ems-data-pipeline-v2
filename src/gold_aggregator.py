"""
Gold Layer ETL — hourly aggregation of EMS metrics.

Reads cleaned Silver-layer data (power_clean, energy_clean, battery_soc_clean) from
InfluxDB 3 (SQL), resamples it to clock-aligned hourly buckets, and writes aggregates
back to InfluxDB:

  - power_hourly:       mean power per hour for each power metric (PV production,
                        battery charge/discharge, grid power balance, consumption).
                        Daily peak power is taken at the end of the hour, as it is
                        a running daily maximum.
  - energy_hourly:      energy per hour for each daily counter (production, import,
                        export, battery charge and discharge), computed as the sum
                        of per-minute counter increments. A decrease marks a midnight
                        reset: the counter restarted from 0, so the increment in that
                        minute equals the value after the reset. Energy recorded
                        before and after a reset within the same hour (e.g. reset at
                        23:56) is therefore fully counted.
                        Consumption is derived from hourly increments:
                        production + import - export (clipped to >= 0). It is not
                        taken from the Silver consumption level, which mixes counters
                        from two days between the inverter and controller resets.
  - battery_soc_hourly: battery state of charge at the end of each hour.

All hourly buckets are labelled with the start of the hour (13:00 = 13:00-14:00).

Designed to run every 5 minutes, but aggregation is deterministic,
so re-processing the same window produces identical output and safely
overwrites prior records.
"""

import sys
import os
import pandas as pd
from influxdb_client_3 import InfluxDBClient3

# Ensure the script can import local modules in CI/CD environments
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import INFLUX_HOST, INFLUX_TOKEN, INFLUX_DATABASE, INFLUX_ORG

# Number of full clock hours re-aggregated on each run
AGGREGATION_HOURS = 24

# Extra Silver history fetched before the window, so the first minute has an increment
ENERGY_CONTEXT = pd.Timedelta(minutes=5)

PEAK_POWER = ('SUN2000', 'peakPower')
PRODUCTION = ('SUN2000', 'production')
IMPORT = ('DTSU666', 'import')
EXPORT = ('DTSU666', 'export')
CONSUMPTION = ('EMS_Calculated', 'consumption')


def fetch_silver(client, table, start):
    """
    Fetches a Silver table since start, pivoted to one column per (device, measurement_type).
    Returns an empty DataFrame if the table does not exist yet.
    """
    query = f'''
    SELECT time, device, measurement_type, value
    FROM {table}
    WHERE time >= '{start.strftime('%Y-%m-%dT%H:%M:%SZ')}' AND time <= now()
    '''
    try:
        df = client.query(query, mode='pandas')
    except Exception as e:
        if 'not found' not in str(e).lower():
            raise
        print(f"Silver table {table} not found: {e}")
        return pd.DataFrame()
    if df.empty:
        return df
    df['time'] = pd.to_datetime(df['time'], utc=True)
    return df.pivot_table(index='time', columns=['device', 'measurement_type'], values='value').sort_index()


def counter_increments(counters):
    """
    Per-minute increments of daily counters. After a reset the counter restarts from 0,
    so a negative difference is replaced by the value after the reset.
    """
    inc = counters.diff()
    return inc.mask(inc < 0, counters)


def write_gold(client, wide, table):
    """Unpivots hourly aggregates (one column per series) and writes them to a Gold table."""
    long = wide.melt(ignore_index=False, value_name='value').dropna(subset=['value'])
    if long.empty:
        return
    long.index.name = 'time'
    client.write(record=long[['value', 'device', 'measurement_type']], data_frame_measurement_name=table,
                 data_frame_tag_columns=['device', 'measurement_type'])
    print(f"Processed {len(long)} hourly records into {table}.")


def process_aggregation():
    """
    Gold Layer ETL: Aggregates Silver data into hourly business metrics.
    Ensures clock-aligned resampling (top of the hour) and maintains data lineage.
    """
    client = InfluxDBClient3(host=INFLUX_HOST, token=INFLUX_TOKEN,
                             database=INFLUX_DATABASE, org=INFLUX_ORG)

    # Window starts at a full hour, so the first bucket is complete
    start = pd.Timestamp.now(tz='UTC').floor('1h') - pd.Timedelta(hours=AGGREGATION_HOURS)

    try:
        # --- AVERAGE POWER HOURLY ---
        power = fetch_silver(client, 'power_clean', start)
        if not power.empty:
            print(f"Power series available for aggregation: {power.columns.tolist()}")
            power_hourly = power.drop(columns=[PEAK_POWER], errors='ignore').resample('1h', label='left').mean()
            if PEAK_POWER in power.columns:
                power_hourly[PEAK_POWER] = power[PEAK_POWER].resample('1h', label='left').last()
            write_gold(client, power_hourly, 'power_hourly')

        # --- INCREMENTED ENERGY HOURLY ---
        energy = fetch_silver(client, 'energy_clean', start - ENERGY_CONTEXT)
        if not energy.empty:
            print(f"Energy series available for aggregation: {energy.columns.tolist()}")
            inc = counter_increments(energy.drop(columns=[CONSUMPTION], errors='ignore'))
            energy_hourly = inc[inc.index >= start].resample('1h', label='left').sum(min_count=1)
            if all(c in energy_hourly.columns for c in (PRODUCTION, IMPORT, EXPORT)):
                energy_hourly[CONSUMPTION] = (energy_hourly[PRODUCTION] + energy_hourly[IMPORT]
                                              - energy_hourly[EXPORT]).clip(lower=0)
            write_gold(client, energy_hourly, 'energy_hourly')

        # --- BATTERY STATE OF CHARGE HOURLY ---
        soc = fetch_silver(client, 'battery_soc_clean', start)
        if not soc.empty:
            write_gold(client, soc.resample('1h', label='left').last(), 'battery_soc_hourly')

        print("Aggregation completed successfully.")
    finally:
        client.close()

if __name__ == "__main__":
    process_aggregation()
