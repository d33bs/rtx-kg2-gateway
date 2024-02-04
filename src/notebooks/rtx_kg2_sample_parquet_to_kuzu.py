# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # Generate RTX-KG2 Data Sample and Schema

# +
import gzip
import json
import pathlib
import shutil
from typing import Any, Dict, Generator, List

import awkward as ak
import ijson
import kuzu
import pyarrow as pa
import requests
from genson import SchemaBuilder
from pyarrow import parquet
from rtx_kg2_functions import (
    count_items_under_top_level_name,
    find_top_level_names,
    parse_items_by_topmost_item_name,
    parse_metadata_by_object_name,
)
# -

# set data to be used throughout notebook
chunk_size = 1
data_dir = "data"
parquet_dir = f"{data_dir}/"
source_data_url = "https://github.com/ncats/translator-lfs-artifacts/raw/main/files/kg2c_lite_2.8.4.json.gz"
target_extracted_sample_data = f"{data_dir}/{pathlib.Path(source_data_url).name.replace('.json.gz', '.sample.json')}"
parquet_dir = target_extracted_sample_data.replace(".json", ".dataset.parquet")
kuzu_dir = target_extracted_sample_data.replace(".json", ".dataset.kuzu")
target_extracted_sample_data_schema_file = target_extracted_sample_data.replace(
    ".json", ".schema.json"
)


pathlib.Path(kuzu_dir).mkdir(exist_ok=True)

db = kuzu.Database(f"{kuzu_dir}")
conn = kuzu.Connection(db)


def generate_cypher_table_create_stmt_from_parquet_file(parquet_file: str):
    parquet_schema = parquet.read_schema(parquet_file)

    # Map Parquet data types to Cypher data types
    cypher_type_mapping = {
        "STRING": "String",
        "INT32": "Integer",
        "INT64": "Integer",
        "FLOAT": "Float",
        "DOUBLE": "Float",
    }

    # Generate Cypher statements
    create_statements = []
    for field in parquet_schema:
        cypher_type = cypher_type_mapping.get(field.type.to_pandas_dtype().name)
        if cypher_type:
            create_statements.append(f"{field.name}: {cypher_type} }")


