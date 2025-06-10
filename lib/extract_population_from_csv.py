import pandas as pd
import os

# === Manually define the mapping of countries to cities ===
COUNTRY_CITY_MAP = {
    "France": "Paris",
    "India": "Delhi",
    "United States": "New York",
    "Japan": "Tokyo",
    "China": "Beijing",
    "United Kingdom": "London",
    "Brazil": "São Paulo",
    "Egypt": "Cairo"
}

def extract_population(input_csv, output_csv, year="2023"):
    df = pd.read_csv(input_csv)

    # Filter for "Population, total"
    df_filtered = df[df["Series Name"] == "Population, total"]

    # Rename year column to clean form if needed
    year_col = f"{year} [YR{year}]"

    if year_col not in df_filtered.columns:
        print(f"[X] Year column '{year_col}' not found in CSV.")
        return

    # Extract required fields and map cities
    df_filtered = df_filtered[["Country Name", year_col]].rename(
        columns={"Country Name": "country", year_col: "population"}
    )

    df_filtered["city"] = df_filtered["country"].map(COUNTRY_CITY_MAP)

    # Remove rows with no city match
    df_final = df_filtered.dropna(subset=["city"])[["city", "country", "population"]]

    # Round population to integers
    df_final["population"] = df_final["population"].round().astype(int)

    # Save final CSV
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df_final.to_csv(output_csv, index=False)

    print(f"[✓] Extracted and saved city-level population data to {output_csv}")

if __name__ == "__main__":
    input_path = "/mnt/c/Users/saiki_fdiq7b5/Downloads/d127e07a-a274-42ee-aec4-61d88e250840_Data.csv"
    output_path = "../config/world_population.csv"

    extract_population(input_path, output_path)
