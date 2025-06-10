import shutil
import os
from datetime import datetime
import pathlib

def load_population_data():
    # Path to your source CSV (downloaded manually or via script)
    source_csv = "config/world_population.csv"

    # Create today's output folder in the datalake
    today = datetime.utcnow().strftime("%Y-%m-%d")
    output_dir = f"datalake/raw/population/{today}"
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Destination path
    destination_file = os.path.join(output_dir, "world_population.csv")

    # Copy the CSV into raw zone
    shutil.copyfile(source_csv, destination_file)

    print(f"[✓] Population data copied to {destination_file}")

if __name__ == "__main__":
    load_population_data()
