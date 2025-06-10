import pandas as pd
from datetime import datetime, timezone
import os
from pathlib import Path

def format_population():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    input_file = "config/world_population.csv"
    output_dir = f"datalake/formatted/population/{today}"
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Read and clean CSV
    df = pd.read_csv(input_file)
    df["city"] = df["city"].str.strip()
    df["country"] = df["country"].str.strip()
    df["population"] = df["population"].astype(int)

    # Save as Parquet
    output_path = os.path.join(output_dir, "world_population.parquet")
    df.to_parquet(output_path, index=False)

    print(f"[✓] Saved formatted population data to {output_path}")

if __name__ == "__main__":
    format_population()
