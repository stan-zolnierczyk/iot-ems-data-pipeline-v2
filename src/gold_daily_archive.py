"""
Gold Layer ETL — daily CSV archive of EMS metrics.

Aggregates the previous complete day (00:00:00 -> 23:59:59 UTC) from
the Gold layer in InfluxDB into a single row appended to
data/daily_ems_report.csv in the GitHub repository. Provides a long-term,
git-tracked record with infinite retention.

Columns:    date, production_kwh, import_kwh, export_kwh, consumption_kwh,
            import_simulated_kwh, export_simulated_kwh, daily_savings_pln.

Source-aware aggregation: 
SUM of hourly deltas for energy_hourly metrics, 
MAX for daily-cumulative counters in battery_simulation.
Idempotent — re-running on the same day will not produce duplicate rows.

Scheduled to run once per day shortly after midnight UTC via GitHub
Actions, which then commits and pushes the updated CSV.
"""

import sys
import os
import pandas as pd
from datetime import datetime, timezone, timedelta
from influxdb_client import InfluxDBClient

# Ensure the script can import local modules in CI/CD environments
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET

CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'daily_ems_report.csv')

def process_daily_archive():
    """
    Gold Layer ETL: Aggregates previous day's data into a daily CSV report.
    Appends one row per day to data/daily_ems_report.csv in the repository.
    Designed to run once per day at 00:05 UTC via GitHub Actions.

    Sources:
      - energy_hourly: production, import, export, consumption — summed to daily totals (kWh)
      - battery_simulation: import_simulated, export_simulated — last value of day (cumulative day counter → kWh)
                            daily_savings_pln — last value of day (cumulative day savings → PLN)
    """
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()

    # Define previous complete day boundaries (UTC): yesterday 00:00:00 → 23:59:59
    today_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_utc - timedelta(days=1)
    yesterday_end = yesterday_start.replace(hour=23, minute=59, second=59)
    date_label = yesterday_start.strftime('%Y-%m-%d')
    start = yesterday_start.strftime('%Y-%m-%dT%H:%M:%SZ')
    stop = yesterday_end.strftime('%Y-%m-%dT%H:%M:%SZ')

    print(f"Processing daily archive for {date_label} ({start} -> {stop})")

    # --- ENERGY HOURLY: 
    energy_query = f'''
from(bucket: "{INFLUX_BUCKET}")
    |> range(start: {start}, stop: {stop})
    |> filter(fn: (r) => r._measurement == "energy_hourly")
    |> filter(fn: (r) => r._field == "value")
'''

    # --- BATTERY SIMULATION: 
    battery_query = f'''
from(bucket: "{INFLUX_BUCKET}")
    |> range(start: {start}, stop: {stop})
    |> filter(fn: (r) => r._measurement == "battery_simulation")
    |> filter(fn: (r) => r._field == "value")
    |> filter(fn: (r) => r.measurement_type == "import_simulated" or r.measurement_type == "export_simulated" or r.measurement_type == "daily_savings_pln")
'''

    # Fetch and aggregate energy_hourly
    energy_daily = {}
    try:
        raw = query_api.query_data_frame(energy_query)
        df_e = pd.concat(raw, ignore_index=True) if isinstance(raw, list) else raw
        if not df_e.empty:
            sums = df_e.groupby('measurement_type')['_value'].sum()
            for mt in ['production', 'import', 'export', 'consumption']:
                energy_daily[mt] = round(float(sums.get(mt, 0)) / 1000, 3)  # Wh -> kWh
    except Exception as e:
        print(f"Warning: energy_hourly fetch failed: {e}")

    # Fetch and aggregate battery_simulation
    battery_daily = {}
    try:
        raw = query_api.query_data_frame(battery_query)
        df_b = pd.concat(raw, ignore_index=True) if isinstance(raw, list) else raw
        if not df_b.empty:
            maxes = df_b.groupby('measurement_type')['_value'].max()
            battery_daily['import_simulated'] = round(float(maxes.get('import_simulated', 0)) / 1000, 3)
            battery_daily['export_simulated'] = round(float(maxes.get('export_simulated', 0)) / 1000, 3)
            battery_daily['daily_savings_pln'] = round(float(maxes.get('daily_savings_pln', 0)), 2)
    except Exception as e:
        print(f"Warning: battery_simulation fetch failed: {e}")

    row = {
        'date': date_label,
        'production_kwh': energy_daily.get('production'),
        'import_kwh': energy_daily.get('import'),
        'export_kwh': energy_daily.get('export'),
        'consumption_kwh': energy_daily.get('consumption'),
        'import_simulated_kwh': battery_daily.get('import_simulated'),
        'export_simulated_kwh': battery_daily.get('export_simulated'),
        'daily_savings_pln': battery_daily.get('daily_savings_pln'),
    }

    os.makedirs(os.path.dirname(os.path.abspath(CSV_PATH)), exist_ok=True)
    df_row = pd.DataFrame([row])
    file_exists = os.path.exists(CSV_PATH)
    if file_exists:
        df_existing = pd.read_csv(CSV_PATH)
        if date_label in df_existing['date'].values:
            print(f"Entry for {date_label} already exists. Skipping.")
            client.close()
            return
    df_row.to_csv(CSV_PATH, mode='a', header=not file_exists, index=False)

    print(f"Daily archive entry written for {date_label}: {row}")
    client.close()

if __name__ == "__main__":
    process_daily_archive()
