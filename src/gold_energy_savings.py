"""
Gold Layer ETL — hourly savings from the PV installation with battery storage.

Reads Silver energy counters (energy_clean), Silver power (power_clean) and the RCE
export price (exported_energy_price), and writes hourly values in PLN to savings_hourly
(device = EMS_Calculated):

  - selfConsumptionSavings: avoided energy purchase,
                            (consumption - import) * IMPORTED_ENERGY_PRICE.
                            consumption - import = production - export, i.e. the inverter's
                            AC output used on site (direct PV and battery discharge).
                            The battery needs no separate terms: production includes battery
                            discharge and excludes energy used for charging, so stored energy
                            is counted when it is discharged, net of battery losses.
                            Valid only if the battery is charged from PV only (see below).
  - exportRevenue:          value of exported energy, sum over minutes of
                            export increment * max(RCE, 0) * EXPORT_PRICE_FACTOR.
                            Net-billing: a negative RCE gives zero remuneration, and RCE is
                            multiplied by 1.23 when credited to the prosumer deposit.
  - totalSavings:           selfConsumptionSavings + exportRevenue.

Energy per minute is taken from reset-aware counter increments (as in gold_aggregator).
exportRevenue (and totalSavings) are written only for hours with a price for every minute
with export; missing hours are filled by later runs once the price is available.

exportRevenue is the value credited to the prosumer deposit. It is not cash: the deposit
offsets future bills and only part of an unused surplus is refunded, so the figure is
a theoretical value.

Grid charging check: the formula would double count energy bought from the grid, stored
and discharged. A warning is printed if the inverter's AC power
(activePower - chargeDischargePower) stays below GRID_CHARGE_THRESHOLD_W for at least
GRID_CHARGE_MIN_MINUTES consecutive minutes, i.e. the battery charges from the grid.

All hourly buckets are labelled with the start of the hour. The last 24 full hours are
recomputed on each run; writes are idempotent.
"""

import sys
import os
import pandas as pd
from influxdb_client_3 import InfluxDBClient3

# Ensure the script can import local modules in CI/CD environments
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import INFLUX_HOST, INFLUX_TOKEN, INFLUX_DATABASE, INFLUX_ORG
from gold_aggregator import (fetch_silver, write_gold, counter_increments, AGGREGATION_HOURS,
                             ENERGY_CONTEXT, PRODUCTION, EXPORT)

IMPORTED_ENERGY_PRICE = 1.00  # PLN/kWh, gross, flat G11 tariff
EXPORT_PRICE_FACTOR = 1.23    # net-billing multiplier of RCE for the prosumer deposit
RCE_PERIOD = pd.Timedelta(minutes=15)
GRID_CHARGE_THRESHOLD_W = -100  # inverter AC below this value means charging from the grid
GRID_CHARGE_MIN_MINUTES = 5     # sustained condition only; single minutes are reading glitches

RCE = ('PSE', 'rce')
ACTIVE_POWER = ('SUN2000', 'activePower')
BATTERY_POWER = ('LUNA2000', 'chargeDischargePower')
DEVICE = 'EMS_Calculated'


def export_price(prices, index):
    """RCE per minute: price of the 15-minute period containing the minute, max(RCE, 0) * factor."""
    if prices.empty or RCE not in prices.columns:
        return pd.Series(float('nan'), index=index)
    rce = prices[RCE].reindex(index, method='ffill', tolerance=RCE_PERIOD - pd.Timedelta(minutes=1))
    return rce.clip(lower=0) * EXPORT_PRICE_FACTOR


def compute_savings(energy, prices, start):
    """Hourly savings [PLN] from Silver counters (1-minute grid) and RCE prices [PLN/MWh]."""
    inc = counter_increments(energy[[PRODUCTION, EXPORT]])
    inc = inc[inc.index >= start]
    production, export = inc[PRODUCTION], inc[EXPORT]

    self_wh = (production - export).resample('1h', label='left').sum(min_count=1)
    self_savings = (self_wh / 1000 * IMPORTED_ENERGY_PRICE).clip(lower=0)

    # Minutes without export need no price; a missing price for a minute with export
    # leaves the whole hour empty
    export_value = (export / 1e6 * export_price(prices, inc.index)).mask(export.fillna(0) == 0, 0.0)
    complete = export_value.isna().resample('1h', label='left').sum() == 0
    revenue = export_value.resample('1h', label='left').sum().where(complete)

    savings = pd.DataFrame({'selfConsumptionSavings': self_savings, 'exportRevenue': revenue})
    savings['totalSavings'] = savings['selfConsumptionSavings'] + savings['exportRevenue']
    savings.columns = pd.MultiIndex.from_product([[DEVICE], savings.columns],
                                                 names=['device', 'measurement_type'])
    return savings


def check_grid_charging(power):
    """Warns if the battery charged from the grid, which the savings formula does not support."""
    if ACTIVE_POWER not in power.columns or BATTERY_POWER not in power.columns:
        return
    inverter_ac = power[ACTIVE_POWER] - power[BATTERY_POWER]
    # Every minute of the trailing window is below the threshold
    sustained = inverter_ac.rolling(GRID_CHARGE_MIN_MINUTES).max() < GRID_CHARGE_THRESHOLD_W
    if sustained.any():
        first = sustained.idxmax() - pd.Timedelta(minutes=GRID_CHARGE_MIN_MINUTES - 1)
        print(f"WARNING: battery charging from the grid for >= {GRID_CHARGE_MIN_MINUTES} minutes "
              f"(first from {first}, min inverter AC {inverter_ac[sustained].min():.0f} W). "
              f"selfConsumptionSavings is overestimated for these hours.")


def process_savings():
    client = InfluxDBClient3(host=INFLUX_HOST, token=INFLUX_TOKEN,
                             database=INFLUX_DATABASE, org=INFLUX_ORG)
    start = pd.Timestamp.now(tz='UTC').floor('1h') - pd.Timedelta(hours=AGGREGATION_HOURS)
    try:
        energy = fetch_silver(client, 'energy_clean', start - ENERGY_CONTEXT)
        if energy.empty or PRODUCTION not in energy.columns or EXPORT not in energy.columns:
            print("No production/export data in energy_clean. Exiting.")
            return
        prices = fetch_silver(client, 'exported_energy_price', start - RCE_PERIOD)
        if prices.empty:
            print("WARNING: no RCE prices available; exportRevenue is not computed.")
        write_gold(client, compute_savings(energy, prices, start), 'savings_hourly')
        check_grid_charging(fetch_silver(client, 'power_clean', start))
        print("Savings calculation completed successfully.")
    finally:
        client.close()


if __name__ == "__main__":
    process_savings()
