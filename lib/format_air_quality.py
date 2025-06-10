import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path

def format_air_quality():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    raw_dir = f"datalake/raw/air_quality/{today}"
    output_dir = f"datalake/formatted/air_quality/{today}"
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    all_data = []

    for filename in os.listdir(raw_dir):
        if filename.endswith(".json"):
            city = filename.replace(".json", "").replace("_", " ")
            with open(os.path.join(raw_dir, filename)) as f:
                data = json.load(f)

                # Extract hourly data (timestamp aligned)
                try:
                    timestamps = data["hourly"]["time"]
                    pm10 = data["hourly"].get("pm10", [None]*len(timestamps))
                    pm2_5 = data["hourly"].get("pm2_5", [None]*len(timestamps))
                    no2 = data["hourly"].get("nitrogen_dioxide", [None]*len(timestamps))
                    o3 = data["hourly"].get("ozone", [None]*len(timestamps))

                    df = pd.DataFrame({
                        "city": city,
                        "timestamp": timestamps,
                        "pm10": pm10,
                        "pm2_5": pm2_5,
                        "no2": no2,
                        "o3": o3
                    })

                    all_data.append(df)
                except KeyError:
                    print(f"[!] Skipping {filename} due to missing structure")

    # Combine and save
    if all_data:
        result = pd.concat(all_data)
        result["timestamp"] = pd.to_datetime(result["timestamp"])
        result.to_parquet(os.path.join(output_dir, "air_quality.parquet"), index=False)
        print(f"[✓] Saved formatted data to {output_dir}")
    else:
        print("[X] No data formatted. Check input JSON files.")

if __name__ == "__main__":
    format_air_quality()
