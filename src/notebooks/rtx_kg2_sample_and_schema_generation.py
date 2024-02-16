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

import ijson
import requests
from genson import SchemaBuilder
from rtx_kg2_functions import (
    count_items_under_top_level_name,
    download_file,
    find_top_level_names,
    parse_items_by_topmost_item_name,
)

# +
# set data to be used throughout notebook
data_dir = "data"
source_data_url = "https://github.com/ncats/translator-lfs-artifacts/raw/main/files/kg2c_lite_2.8.4.json.gz"
target_data = f"{data_dir}/{pathlib.Path(source_data_url).name}"
target_extracted_data = f"{data_dir}/{pathlib.Path(source_data_url).stem}"
target_extracted_sample_data = target_extracted_data.replace(".json", ".sample.json")
target_extracted_sample_data_schema = target_extracted_data.replace(
    ".json", ".sample.schema.json"
)
chunk_size = 2

json_schema_init = {
    "$id": "https://github.com/CU-DBMI/rtx-kg2-gateway/src/notebooks/data/kg2c_lite_2.8.4.schema.json",
    "description": "Inferred JSON Schema from subset of RTX-KG2 JSON data for documenting data provenance.",
    "type": "object",
    "properties": {},
}


# -


# perform download of source data
download_file(source_data_url, data_dir)

# extract gz file
with open(target_extracted_data, "wb") as f_out, gzip.open(target_data, "rb") as f_in:
    shutil.copyfileobj(f_in, f_out)

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
top_level_name_item_counts

# gather metadata names by inference from item count
metadata_top_level_names = [
    top_level_name
    for top_level_name, count in top_level_name_item_counts.items()
    if count == 0
]
metadata_top_level_names

# build a sample of data using limited number of items
sample_items_dict = {}
for top_level_name in [
    name for name in top_level_names if name not in metadata_top_level_names
]:
    items = parse_items_by_topmost_item_name(
        target_extracted_data, top_level_name, chunk_size, 1
    )
    for value in items:
        sample_items_dict[top_level_name] = list(value)
sample_items_dict


def parse_metadata_by_object_name(
    json_file: str, metadata_object_name: str
) -> Generator[Any, None, None]:
    """
    Extract single value metadata from json file
    """

    with open(json_file, "r") as f:
        return next(ijson.items(f, metadata_object_name))


# build a metadata dict
metadata_dict = {
    metadata_top_level_name: parse_metadata_by_object_name(
        target_extracted_data, metadata_top_level_name
    )
    for metadata_top_level_name in metadata_top_level_names
}
metadata_dict

# combine the meta with the items for a full sample
full_sample_dict = dict(sample_items_dict, **metadata_dict)
print(full_sample_dict)

# write sample to file
with open(target_extracted_sample_data, "w") as file:
    file.write(json.dumps(full_sample_dict, indent=2))

# infer the schema from sample json using genson
builder = SchemaBuilder()
builder.add_schema(json_schema_init)
builder.add_object(full_sample_dict)
inferred_json_schema = builder.to_json(indent=2)
print(builder.to_json(indent=2))

# write sample schema to file
with open(target_extracted_sample_data_schema, "w") as file:
    file.write(inferred_json_schema)
