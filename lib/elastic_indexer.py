import pandas as pd
from datetime import datetime, timezone
from elasticsearch import Elasticsearch, helpers
import os

def elastic_indexer():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    parquet_path = f"datalake/usage/air_quality_vs_population/{today}/combined.parquet"
    index_name = "air_quality_population"

    # Load the parquet file
    df = pd.read_parquet(parquet_path)

    # Connect to Elasticsearch
    es = Elasticsearch("http://localhost:9200")

    # Optional: Delete the index if it already exists (for repeat runs)
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"[i] Old index '{index_name}' deleted.")

    # Prepare data for bulk indexing
    actions = [
        {
            "_index": index_name,
            "_source": row.dropna().to_dict()
        }
        for _, row in df.iterrows()
    ]

    # Perform bulk indexing
    helpers.bulk(es, actions)
    print(f"[✓] Indexed {len(actions)} documents into '{index_name}'")

if __name__ == "__main__":
    elastic_indexer()
