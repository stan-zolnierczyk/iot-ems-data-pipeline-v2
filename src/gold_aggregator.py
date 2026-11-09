"""
Gold Layer ETL — hourly aggregation of EMS metrics.

Reads cleaned Silver-layer data (power_clean, energy_clean) from InfluxDB,
resamples it to clock-aligned hourly buckets, and writes aggregates back to InfluxDB:

  - power_hourly:   mean instantaneous power (PV panels) and power balance 
                    (smart meter) per hour.
  - energy_hourly:  incremental energy delta per hour for each metric
                    (production, import, export, consumption). Negative
                    diffs caused by midnight counter resets are clamped
                    to zero.

Designed to run every 5 minutes, but aggregation is deterministic,
so re-processing the same window produces identical output and safely
overwrites prior records.
"""

import sys
import os
import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

# Ensure the script can import local modules in CI/CD environments
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET

def process_aggregation():
    """
    Gold Layer ETL: Aggregates Silver data into hourly business metrics.
    Ensures clock-aligned resampling (top of the hour) and maintains data lineage.
    """
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # FETCH DATA (Silver Layer)
    # Fetch last 24h to ensure there is enough data for full hourly buckets
    query = f'''
    import "influxdata/influxdb/schema"

    from(bucket: "{INFLUX_BUCKET}")

        |> range(start: -24h)
        |> filter(fn: (r) => r._measurement == "power_clean" or r._measurement == "energy_clean")
    '''

    raw_data = query_api.query_data_frame(query)
    df = pd.concat(raw_data, ignore_index=True) if isinstance(raw_data, list) else raw_data

    if df.empty:
        print("No Silver data found for aggregation. Exiting.")
        client.close()
        return

    # PIVOT IN PANDAS (Instead of Flux) - creates columns from 'measurement_type' and uses '_value' as data
    df = df.pivot_table(index='_time', columns='measurement_type', values='_value')

    # Data Preparation
    df.index = pd.to_datetime(df.index, utc=True)
    df = df.sort_index()
    print(f"Columns available for aggregation: {df.columns.tolist()}")

    # --- AVERAGE POWER HOURLY ---
    power_fields = ['instantaneousPower', 'powerBalance']
    available_power = df.columns.intersection(power_fields)

    if not available_power.empty:
        # Define device mapping for power metrics to maintain metadata consistency
        power_device_map = {
            'instantaneousPower': 'SUN2000',
            'powerBalance': 'DTSU666'
        }

        # Resample to full clock hours using mean
        df_power_gold = df[available_power].resample('1H', label='left').mean()

        # Unpivot and re-assign metadata
        df_power_gold = df_power_gold.melt(ignore_index=False, var_name='measurement_type', value_name='value').dropna()
        df_power_gold['device'] = df_power_gold['measurement_type'].map(power_device_map).fillna('Unknown_Device')

        if not df_power_gold.empty:
            write_api.write(bucket=INFLUX_BUCKET, record=df_power_gold,
                            data_frame_measurement_name='power_hourly',
                            data_frame_tag_columns=['device', 'measurement_type'])
            print(f"Processed {len(df_power_gold)} hourly power records.")

    # --- INCREMENTED ENERGY HOURLY ---
    energy_fields = ['production', 'import', 'export', 'consumption']
    available_energy = df.columns.intersection(energy_fields)

    if not available_energy.empty:
        # Define source-to-target device mapping to maintain data lineage across layers
        energy_device_map = {
            'production': 'SUN2000',
            'import': 'DTSU666',
            'export': 'DTSU666',
            'consumption': 'EMS_Calculated'
        }

        # Resample and calculate delta (consumption within the hour)
        # Note: sorting before diff is critical for cumulative counters
        df_energy_hourly = df[available_energy].sort_index().resample('1H', label='left').last().diff()

        # Data Quality: drop initial NaN from diff and prevent negative values from resets
        df_energy_hourly = df_energy_hourly.dropna()
        df_energy_hourly[df_energy_hourly < 0] = 0

        # Unpivot and re-assign original device tags based on lineage map
        df_energy_gold = df_energy_hourly.melt(ignore_index=False, var_name='measurement_type', value_name='value')
        df_energy_gold['device'] = df_energy_gold['measurement_type'].map(energy_device_map).fillna('Unknown_Device')

        if not df_energy_gold.empty:
            write_api.write(bucket=INFLUX_BUCKET, record=df_energy_gold,
                            data_frame_measurement_name='energy_hourly',
                            data_frame_tag_columns=['device', 'measurement_type'])
            print(f"Processed {len(df_energy_gold)} hourly energy records.")

    print("Aggregation completed successfully.")
    client.close()

if __name__ == "__main__":
    process_aggregation()
