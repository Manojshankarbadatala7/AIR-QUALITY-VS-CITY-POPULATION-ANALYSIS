import pandas as pd
from datetime import datetime, timezone
import os
from pathlib import Path

def combine_air_population():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Input paths
    air_quality_path = f"datalake/formatted/air_quality/{today}/air_quality.parquet"
    population_path = f"datalake/formatted/population/{today}/world_population.parquet"

    # Output path
    output_dir = f"datalake/usage/air_quality_vs_population/{today}"
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_file = os.path.join(output_dir, "combined.parquet")

    # Load data
    df_air = pd.read_parquet(air_quality_path)
    df_pop = pd.read_parquet(population_path)

    # Normalize city names (optional but safe)
    df_air["city"] = df_air["city"].str.strip().str.lower()
    df_pop["city"] = df_pop["city"].str.strip().str.lower()

    # Merge on city
    df_merged = pd.merge(df_air, df_pop, on="city", how="inner")

    # Add pollution-per-capita KPIs
    df_merged["pm2_5_per_million"] = df_merged["pm2_5"] / (df_merged["population"] / 1_000_000)
    df_merged["pm10_per_million"] = df_merged["pm10"] / (df_merged["population"] / 1_000_000)

    # Save result
    df_merged.to_parquet(output_file, index=False)
    print(f"[✓] Combined data saved to {output_file}")

if __name__ == "__main__":
    combine_air_population()
