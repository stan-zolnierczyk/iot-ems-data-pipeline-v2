"""
Gold Layer ETL — 10 kWh battery storage digital twin.

The script simulates a hypothetical 10 kWh battery. It operates on the 
Silver-layer import/export energy stream and writes four series to the
battery_simulation measurement:

  - stored_energy:     current energy stored in the battery (Wh).
  - export_simulated:  cumulative energy exported to the grid after
                       charging the battery (Wh, daily).
  - import_simulated:  cumulative energy import after battery discharge
                       (Wh, daily).
  - daily_savings_pln: reduced import cost in PLN, computed from
                       (import - import_simulated) * price_per_kwh.

State (stored_energy, import_simulated, export_simulated) is persisted
across runs by re-reading the last value from InfluxDB, ensuring
multi-year continuity. Counters that reset at midnight in the source data 
are mirrored in the simulated counters.
"""

import sys
import os
import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

# Ensure the script can import local modules in CI/CD environments
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET

def process_battery_simulation():
    """
    Gold Layer ETL: Simulates a 10kWh battery storage unit using Silver energy data.
    Persists simulation state (stored_energy, import_simulated, export_simulated) across runs
    to ensure continuous, multi-year cumulative counters.
    """
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # FETCH DATA (Silver Layer - energy only)
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")

        |> range(start: -24h)
        |> filter(fn: (r) => r._measurement == "energy_clean")
    '''

    raw_data = query_api.query_data_frame(query)
    df = pd.concat(raw_data, ignore_index=True) if isinstance(raw_data, list) else raw_data

    if df.empty:
        print("No Silver energy data found for battery simulation. Exiting.")
        client.close()
        return

    # PIVOT IN PANDAS - creates columns from 'measurement_type' and uses '_value' as data
    df = df.pivot_table(index='_time', columns='measurement_type', values='_value')
    df.index = pd.to_datetime(df.index, utc=True)
    df = df.sort_index()

    if 'export' not in df.columns or 'import' not in df.columns:
        print("Missing 'export' or 'import' columns for battery simulation. Exiting.")
        client.close()
        return

    print("Starting 10kWh Battery Simulation...")

    # Calculate 1-minute increments (energy flow in each minute)
    df_batt = df[['export', 'import']].sort_index().diff().fillna(0)

    capacity_wh = 10000  # 10 kWh
    price_per_kwh = 0.98  # PLN

    # Persist all simulation counters between runs: read last known values from InfluxDB
    persistence_query = f'''
from(bucket: "{INFLUX_BUCKET}")
    |> range(start: -30d)
    |> filter(fn: (r) => r._measurement == "battery_simulation")
    |> filter(fn: (r) => r._field == "value")
    |> last()
'''
    try:
        last_batt_df = query_api.query_data_frame(persistence_query)
        if isinstance(last_batt_df, list):
            last_batt_df = pd.concat(last_batt_df, ignore_index=True) if last_batt_df else pd.DataFrame()
        if not last_batt_df.empty and 'measurement_type' in last_batt_df.columns:
            batt_last = last_batt_df.set_index('measurement_type')['_value']
            current_stored_energy = float(batt_last.get('stored_energy', 0))
            current_import_sim = float(batt_last.get('import_simulated', df['import'].iloc[0]))
            current_export_sim = float(batt_last.get('export_simulated', df['export'].iloc[0]))
        else:
            current_stored_energy = 0
            current_import_sim = df['import'].iloc[0]
            current_export_sim = df['export'].iloc[0]
    except Exception:
        current_stored_energy = 0
        current_import_sim = df['import'].iloc[0]
        current_export_sim = df['export'].iloc[0]
    print(f"Battery simulation starting with stored_energy={current_stored_energy:.1f} Wh, "
          f"import_simulated={current_import_sim:.1f} Wh, export_simulated={current_export_sim:.1f} Wh")

    stored_energy_history = []
    import_sim_history = []
    export_sim_history = []

    for _, row in df_batt.iterrows():
        m_export = row['export']
        m_import = row['import']

        # Counter reset at midnight: reset simulated counters to 0, skip increment
        if m_export < 0:
            current_export_sim = 0
            m_export = 0
        if m_import < 0:
            current_import_sim = 0
            m_import = 0

        # CHARGING: Use export to charge battery
        if m_export > 0:
            charge_amount = min(m_export, capacity_wh - current_stored_energy)
            current_stored_energy += charge_amount
            m_export -= charge_amount  # Remaining export after charging

        # DISCHARGING: Use battery to cover import
        elif m_import > 0:
            discharge_amount = min(m_import, current_stored_energy)
            current_stored_energy -= discharge_amount
            m_import -= discharge_amount  # Remaining import after battery support

        current_export_sim += m_export
        current_import_sim += m_import

        stored_energy_history.append(current_stored_energy)
        export_sim_history.append(current_export_sim)
        import_sim_history.append(current_import_sim)

    # Create results DataFrame
    df_sim = pd.DataFrame(index=df_batt.index)
    df_sim['stored_energy'] = stored_energy_history
    df_sim['export_simulated'] = export_sim_history
    df_sim['import_simulated'] = import_sim_history

    # Hypothetical savings: energy NOT imported from grid thanks to the battery (day-cumulative, resets at midnight)
    savings_wh = df['import'].values - df_sim['import_simulated'].values
    df_sim['daily_savings_pln'] = savings_wh / 1000 * price_per_kwh

    # Prepare for InfluxDB (unpivot)
    df_sim_gold = df_sim.melt(ignore_index=False, var_name='measurement_type', value_name='value')
    df_sim_gold['device'] = 'Battery_Simulator_10kWh'

    # Select relevant columns only
    final_sim_gold = df_sim_gold[['value', 'device', 'measurement_type']]

    write_api.write(bucket=INFLUX_BUCKET, record=final_sim_gold,
                    data_frame_measurement_name='battery_simulation',
                    data_frame_tag_columns=['device', 'measurement_type'])
    print(f"Battery simulation completed for {len(df_sim)} intervals.")

    print("Battery simulation update successful.")
    client.close()

if __name__ == "__main__":
    process_battery_simulation()
