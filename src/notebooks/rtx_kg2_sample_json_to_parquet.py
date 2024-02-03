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
import requests
from genson import SchemaBuilder
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
target_extracted_sample_data_schema_file = target_extracted_sample_data.replace(
    ".json", ".schema.json"
)
with open(target_extracted_sample_data_schema_file, "r") as file:
    # read the schema and and compress spacing within new schema string
    target_extracted_sample_data_schema = json.dumps(json.loads(file.read()))


pathlib.Path(parquet_dir).mkdir(exist_ok=True)

# show the top level object names for json file
top_level_names = list(find_top_level_names(target_extracted_sample_data))
print(top_level_names)

# count the number of items under each top level name
top_level_name_item_counts = {
    top_level_name: count_items_under_top_level_name(
        target_extracted_sample_data, top_level_name
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
            target_extracted_sample_data, metadata_top_level_name
        )
        for metadata_top_level_name in metadata_top_level_names
    },
    **{"source_data_json_schema": target_extracted_sample_data_schema}
)
metadata_dict

# build a sample of data using limited number of items
sample_items_dict = {}
for top_level_name in [
    name for name in top_level_names if name not in metadata_top_level_names
]:
    pathlib.Path(f"{parquet_dir}/{top_level_name}").mkdir(exist_ok=True)
    items = parse_items_by_topmost_item_name(
        target_extracted_sample_data, top_level_name, chunk_size, 1
    )
    for value in items:
        val = ak.to_arrow(ak.Array(list(value))).replace_schema_metadata(metadata_dict)
val
