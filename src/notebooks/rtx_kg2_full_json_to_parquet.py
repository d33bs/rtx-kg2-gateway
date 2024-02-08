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

# # Generate RTX-KG2 Full JSON to Parquet

# +
import gzip
import json
import pathlib
import shutil
from typing import Any, Dict, Generator, List

import awkward as ak
import ijson
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
chunk_size = 500000
data_dir = "data"
parquet_dir = f"{data_dir}/"
source_data_url = "https://github.com/ncats/translator-lfs-artifacts/raw/main/files/kg2c_lite_2.8.4.json.gz"
target_extracted_data = (
    f"{data_dir}/{pathlib.Path(source_data_url).name.replace('.json.gz', '.json')}"
)
parquet_dir = target_extracted_data.replace(".json", ".full.dataset.parquet")
target_extracted_sample_data_schema_file = target_extracted_data.replace(
    ".json", ".sample.schema.json"
)
with open(target_extracted_sample_data_schema_file, "r") as file:
    # read the schema and and compress spacing within new schema string
    target_extracted_sample_data_schema = json.dumps(json.loads(file.read()))


pathlib.Path(parquet_dir).mkdir(exist_ok=True)

# show the top level object names for json file
top_level_names = list(find_top_level_names(target_extracted_data))
print(top_level_names)

# count the number of items under each top level name
top_level_name_item_counts = {
    top_level_name: count_items_under_top_level_name(
        target_extracted_data, top_level_name
    )
    for top_level_name in top_level_names
}
print(top_level_name_item_counts)

# gather metadata names by inference from item count
metadata_top_level_names = [
    top_level_name
    for top_level_name, count in top_level_name_item_counts.items()
    if count == 0
]
metadata_top_level_names

# build a metadata dict
metadata_dict = dict(
    {
        metadata_top_level_name: parse_metadata_by_object_name(
            target_extracted_data, metadata_top_level_name
        )
        for metadata_top_level_name in metadata_top_level_names
    },
    **{"source_data_json_schema": target_extracted_sample_data_schema}
)
metadata_dict

# +
# build a sample of data using limited number of items
# 500,000 rows extraction, full extraction ~500 secs
# specify a map for from to specification
# to move these to first two cols of related table
edges_from_to_map = {"from": "subject", "to": "object"}

sample_items_dict = {}
for top_level_name in [
    name for name in top_level_names if name not in metadata_top_level_names
]:
    dataset_path = f"{parquet_dir}/{top_level_name}"
    pathlib.Path(dataset_path).mkdir(exist_ok=True)
    items = parse_items_by_topmost_item_name(
        target_extracted_data, top_level_name, chunk_size, 0
    )
    for idx, value in enumerate(items):
        if top_level_name == "nodes":
            parquet.write_table(
                table=pa.Table.from_pylist(list(value)).replace_schema_metadata(
                    metadata_dict
                ),
                where=f"{dataset_path}/{top_level_name}.{idx}.parquet",
            )
        elif top_level_name == "edges":
            table = pa.Table.from_pylist(list(value))
            table = table.select(
                [edges_from_to_map["from"], edges_from_to_map["to"]]
                + [
                    name
                    for name in table.schema.names
                    if name not in edges_from_to_map.values()
                ]
            )

            parquet.write_table(
                table=table.replace_schema_metadata(metadata_dict),
                where=f"{dataset_path}/{top_level_name}.{idx}.parquet",
            )

# +
# validate the counts to naively check that the data transferred properly

for top_level_name in [
    name for name in top_level_names if name not in metadata_top_level_names
]:
    # compare the count of objects from JSON vs Parquet
    if top_level_name_item_counts[top_level_name] != sum(
        # read the parquet dataset as a list of files
        # then count the num rows using file-based metadata
        # (avoids reading the entirety of dataset values)
        [
            parquet.read_metadata(parquet_file).num_rows
            for parquet_file in parquet.ParquetDataset(
                f"{parquet_dir}/{top_level_name}"
            ).files
        ]
    ):
        raise ValueError(
            f"Mismatch in row counts for top level object: {top_level_name}"
        )
    print(
        f"Row counts match from JSON object to Parquet for top level object: {top_level_name}"
    )
