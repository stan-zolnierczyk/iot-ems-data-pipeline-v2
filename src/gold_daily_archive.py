"""
Gold Layer ETL — daily CSV archive of EMS metrics.

Aggregates the previous complete local day (00:00 -> 24:00 Europe/Warsaw, the same day
boundary as the device energy counters; 23 or 25 hours on DST change days) into a single
row appended to data/daily_ems_report.csv in the GitHub repository. Provides a long-term,
git-tracked record with infinite retention.

Columns:
  - date:                   local calendar day (YYYY-MM-DD).
  - production_kwh, import_kwh, export_kwh, consumption_kwh,
    battery_charge_kwh, battery_discharge_kwh:
                            sum of hourly energy from energy_hourly (Wh -> kWh).
  - daily_savings_pln:      sum of hourly selfConsumptionSavings and exportRevenue from
                            savings_hourly. Hours without an RCE price contribute no export
                            revenue; a warning is printed.
  - daily_irradiation:      integral of the ELSNER irradiation reading (Bronze table
                            irradiation) over the day: sum of 1-minute values, in the sensor's
                            own units (value x minutes, no unit conversion). Readings outside
                            0..IRRADIATION_MAX and missing minutes are linearly interpolated.

Idempotent — re-running for the same day does not produce duplicate rows.

Scheduled to run once per day shortly after midnight UTC via GitHub Actions
(after the local day has ended), which then commits and pushes the updated CSV.
"""

import sys
import os
import pandas as pd
from influxdb_client_3 import InfluxDBClient3

# Ensure the script can import local modules in CI/CD environments
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import INFLUX_HOST, INFLUX_TOKEN, INFLUX_DATABASE, INFLUX_ORG
from gold_aggregator import fetch_silver

CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'daily_ems_report.csv')
LOCAL_TZ = 'Europe/Warsaw'

# CSV column -> energy_hourly measurement_type (Wh)
ENERGY_COLUMNS = {
    'production_kwh': 'production',
    'import_kwh': 'import',
    'export_kwh': 'export',
    'consumption_kwh': 'consumption',
    'battery_charge_kwh': 'currentDayCharge',
    'battery_discharge_kwh': 'currentDayDischarge',
}
SAVINGS_COMPONENTS = ['selfConsumptionSavings', 'exportRevenue']

IRRADIATION = ('ELSNER', 'irradiation')
IRRADIATION_MAX = 500
# Readings fetched around the day, so interpolation across midnight has neighbours
IRRADIATION_CONTEXT = pd.Timedelta(hours=1)

CSV_COLUMNS = ['date', *ENERGY_COLUMNS, 'daily_savings_pln', 'daily_irradiation']


def daily_energy(energy_hourly):
    """Daily totals [kWh] per CSV column; None if a series has no data."""
    sums = energy_hourly.droplevel('device', axis=1).sum(min_count=1)
    return {col: round(sums[mt] / 1000, 3) if pd.notna(sums.get(mt)) else None
            for col, mt in ENERGY_COLUMNS.items()}


def daily_savings(savings_hourly):
    """Daily savings [PLN] from the hourly components; None if there is no data."""
    if savings_hourly.empty:
        return None
    components = savings_hourly.droplevel('device', axis=1).reindex(columns=SAVINGS_COMPONENTS)
    missing = int(components['exportRevenue'].isna().sum())
    if missing:
        print(f"WARNING: {missing} hours without exportRevenue (no RCE price); "
              f"daily_savings_pln includes self-consumption only for these hours.")
    total = components.sum().sum(min_count=1)
    return round(total, 2) if pd.notna(total) else None


def daily_irradiation(irradiation, day_start, day_end):
    """Integral of irradiation over [day_start, day_end): sum of linearly interpolated 1-minute values."""
    if irradiation.empty or IRRADIATION not in irradiation.columns:
        return None
    s = irradiation[IRRADIATION]
    s = s.where((s >= 0) & (s <= IRRADIATION_MAX))
    s = s.groupby(s.index.floor('1min')).last()
    grid = pd.date_range(day_start, day_end, freq='1min', inclusive='left')
    s = s.reindex(s.index.union(grid)).interpolate(method='time', limit_area='inside').reindex(grid)
    missing = int(s.isna().sum())
    if missing == len(grid):
        return None
    if missing:
        print(f"WARNING: {missing} minutes of irradiation outside the available readings (counted as 0).")
    return int(round(s.sum()))


def process_daily_archive():
    """Aggregates the previous local day into one row of data/daily_ems_report.csv."""
    today_local = pd.Timestamp.now(tz=LOCAL_TZ).normalize()
    day_start_local = (today_local - pd.Timedelta(days=1)).normalize()
    date_label = day_start_local.strftime('%Y-%m-%d')
    day_start = day_start_local.tz_convert('UTC')
    day_end = today_local.tz_convert('UTC')
    print(f"Processing daily archive for {date_label} ({day_start} -> {day_end})")

    if os.path.exists(CSV_PATH) and date_label in pd.read_csv(CSV_PATH, dtype={'date': str})['date'].values:
        print(f"Entry for {date_label} already exists. Skipping.")
        return

    client = InfluxDBClient3(host=INFLUX_HOST, token=INFLUX_TOKEN,
                             database=INFLUX_DATABASE, org=INFLUX_ORG)
    try:
        energy_hourly = fetch_silver(client, 'energy_hourly', day_start, day_end)
        savings_hourly = fetch_silver(client, 'savings_hourly', day_start, day_end)
        irradiation = fetch_silver(client, 'irradiation', day_start - IRRADIATION_CONTEXT,
                                   day_end + IRRADIATION_CONTEXT)
    finally:
        client.close()

    expected_hours = int((day_end - day_start) / pd.Timedelta(hours=1))
    if len(energy_hourly) != expected_hours:
        print(f"WARNING: energy_hourly has {len(energy_hourly)} of {expected_hours} hours for {date_label}.")

    row = {'date': date_label,
           **(daily_energy(energy_hourly) if not energy_hourly.empty else dict.fromkeys(ENERGY_COLUMNS)),
           'daily_savings_pln': daily_savings(savings_hourly),
           'daily_irradiation': daily_irradiation(irradiation, day_start, day_end)}

    os.makedirs(os.path.dirname(os.path.abspath(CSV_PATH)), exist_ok=True)
    file_exists = os.path.exists(CSV_PATH) and os.path.getsize(CSV_PATH) > 0
    pd.DataFrame([row], columns=CSV_COLUMNS).to_csv(CSV_PATH, mode='a', header=not file_exists, index=False)
    print(f"Daily archive entry written for {date_label}: {row}")


if __name__ == "__main__":
    process_daily_archive()
