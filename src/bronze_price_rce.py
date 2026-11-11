"""
Bronze Layer ingestion — market price of exported energy (RCE).

Fetches the Polish market energy price RCE (Rynkowa Cena Energii, PLN/MWh, net),
published by PSE for net-billing prosumers, and writes it to InfluxDB:

  - exported_energy_price: device = PSE, measurement_type = rce, field value [PLN/MWh].

Prices are published per 15-minute period, a day ahead. Each point is timestamped with
the start of its period in UTC (the API provides UTC timestamps, so DST is handled at
the source). Prices are stored as published, negative values included; net-billing
rules are applied in the Gold layer.

Each run fetches the business days from yesterday (local time) onwards, including
tomorrow once it is published. Writes are idempotent, so overlapping runs are safe.
"""

import sys
import os
import json
import urllib.parse
import urllib.request
import pandas as pd
from influxdb_client_3 import InfluxDBClient3

# Ensure the script can import local modules in CI/CD environments
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import INFLUX_HOST, INFLUX_TOKEN, INFLUX_DATABASE, INFLUX_ORG

RCE_API_URL = 'https://api.raporty.pse.pl/api/rce-pln'
RCE_PERIOD = pd.Timedelta(minutes=15)
LOCAL_TZ = 'Europe/Warsaw'
REQUEST_TIMEOUT_S = 30


def fetch_rce(since_date):
    """Returns RCE prices for business days >= since_date, indexed by period start (UTC)."""
    params = {'$filter': f"business_date ge '{since_date}'", '$select': 'dtime_utc,rce_pln'}
    url = f"{RCE_API_URL}?{urllib.parse.urlencode(params, quote_via=urllib.parse.quote)}"
    rows = []
    while url:  # the API returns pages of 100 rows with a nextLink
        with urllib.request.urlopen(url, timeout=REQUEST_TIMEOUT_S) as response:
            page = json.load(response)
        rows.extend(page.get('value', []))
        url = page.get('nextLink')
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # dtime_utc marks the end of the period
    df['time'] = pd.to_datetime(df['dtime_utc'], utc=True) - RCE_PERIOD
    return df.set_index('time')[['rce_pln']].rename(columns={'rce_pln': 'value'}).astype(float).sort_index()


def process_rce():
    since = (pd.Timestamp.now(tz=LOCAL_TZ) - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    prices = fetch_rce(since)
    if prices.empty:
        print(f"No RCE prices published since {since}.")
        return
    prices['device'] = 'PSE'
    prices['measurement_type'] = 'rce'
    client = InfluxDBClient3(host=INFLUX_HOST, token=INFLUX_TOKEN,
                             database=INFLUX_DATABASE, org=INFLUX_ORG)
    try:
        client.write(record=prices, data_frame_measurement_name='exported_energy_price',
                     data_frame_tag_columns=['device', 'measurement_type'])
    finally:
        client.close()
    print(f"Processed {len(prices)} RCE prices ({prices.index.min()} - {prices.index.max()}) "
          f"into exported_energy_price.")


if __name__ == "__main__":
    process_rce()
