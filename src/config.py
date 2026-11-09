import os
import sys
from dotenv import load_dotenv

load_dotenv() # Wczyta dane z pliku .env (lokalnie)

# InfluxDB 3 connection settings (influxdb3-python: InfluxDBClient3)
INFLUX_HOST = os.getenv("INFLUX_HOST")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_DATABASE = os.getenv("INFLUX_DATABASE")  # e.g. "EMS-2" (bucket == database in InfluxDB 3)
INFLUX_ORG = os.getenv("INFLUX_ORG")  # Optional for InfluxDB Cloud Serverless

# Fail fast with a clear message instead of passing None to the client
_missing = [name for name, value in {
    "INFLUX_HOST": INFLUX_HOST,
    "INFLUX_TOKEN": INFLUX_TOKEN,
    "INFLUX_DATABASE": INFLUX_DATABASE,
}.items() if not value]

if _missing:
    sys.exit(f"Missing required environment variables: {', '.join(_missing)}")
