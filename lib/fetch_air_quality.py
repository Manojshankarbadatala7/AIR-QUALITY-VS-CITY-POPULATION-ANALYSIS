import requests
import json
import os
from datetime import datetime, timezone
import pathlib

def fetch_air_quality():
    # Load city list
    base_dir = os.path.dirname(os.path.dirname(__file__))  # go up from lib/
    config_path = os.path.join(base_dir, "config", "cities.json")

    with open(config_path, "r") as f:
        cities = json.load(f)

    # Create output directory
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir = f"datalake/raw/air_quality/{today}"
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Fetch data for each city
    for city in cities:
        lat = city["latitude"]
        lon = city["longitude"]
        city_name = city["city"].replace(" ", "_")

        url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={lat}&longitude={lon}&hourly=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide"

        try:
            response = requests.get(url)
            data = response.json()

            with open(f"{output_dir}/{city_name}.json", "w") as outfile:
                json.dump(data, outfile)

            print(f"[✓] Saved data for {city_name}")

        except Exception as e:
            print(f"[X] Failed to fetch data for {city_name}: {e}")

if __name__ == "__main__":
    fetch_air_quality()
