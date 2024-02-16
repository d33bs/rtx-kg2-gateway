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

# # Generate RTX-KG2 Metanames Parquet to Kuzu - Copy Data to Tables
#
# Note: may require large amount of memory to successfully perform ingest.
#
# GCP VM e2-standard-16 type was used showing 50% memory utilization at some points (32GB).

# +
import gzip
import json
import pathlib
import shutil
import time
from functools import partial
from typing import Any, Dict, Generator, List, Literal, Optional

import awkward as ak
import duckdb
import ijson
import kuzu
import pyarrow as pa
import requests
from genson import SchemaBuilder
from pyarrow import parquet
from rtx_kg2_functions import (
    count_items_under_top_level_name,
    drop_table_if_exists,
    find_top_level_names,
    generate_cypher_table_create_stmt_from_parquet_file,
    parse_items_by_topmost_item_name,
    parse_metadata_by_object_name,
)
# -

# set data to be used throughout notebook
chunk_size = 500000
data_dir = "data"
parquet_dir = f"{data_dir}/"
source_data_url = "https://github.com/ncats/translator-lfs-artifacts/raw/main/files/kg2c_lite_2.8.4.json.gz"
target_extracted_sample_data = (
    f"{data_dir}/{pathlib.Path(source_data_url).name.replace('.json.gz', '.json')}"
)
parquet_dir = target_extracted_sample_data.replace(".json", ".full.dataset.parquet")
parquet_metanames_dir = target_extracted_sample_data.replace(
    ".json", ".full.with-metanames.dataset.parquet"
)
kuzu_dir = target_extracted_sample_data.replace(
    ".json", ".full.with-metanames.dataset.kuzu"
)
target_extracted_sample_data_schema_file = target_extracted_sample_data.replace(
    ".json", ".schema.json"
)
dataset_name_to_cypher_table_type_map = {"nodes": "node", "edges": "rel"}
print(f"Kuzu dir: {kuzu_dir}")


# init a Kuzu database and connection
db = kuzu.Database(f"{kuzu_dir}")
kz_conn = kuzu.Connection(db)


def kz_execute_with_retries(
    kz_conn: kuzu.connection.Connection, kz_stmt: str, retry_count: int = 5
):
    """
    Retry running a kuzu execution up to retry_count number of times.
    """

    while retry_count > 1:

        try:
            kz_conn.execute(kz_stmt)
            break
        except RuntimeError as runexc:
            # catch previous copy work and immediately move on
            if (
                str(runexc)
                == "Copy exception: COPY commands can only be executed once on a table."
            ):
                print(runexc)
                break
            elif "Unable to find primary key value" in str(runexc):
                print(f"Retrying after primary key exception: {runexc}")
                # wait a half second before attempting again
                time.sleep(0.5)
                retry_count -= 1
            else:
                raise


# note: we provide specific ordering here to ensure nodes are created before edges
table_count = 1
sub_table_count = 1
for path in [f"{parquet_metanames_dir}/nodes", f"{parquet_metanames_dir}/edges"]:

    decoded_type = dataset_name_to_cypher_table_type_map[pathlib.Path(path).name]
    print(f"Working on kuzu ingest of parquet dataset: {path} ")
    for table in pathlib.Path(path).glob("*"):
        table_name = table.name
        if decoded_type == "node":
            # uses wildcard functionality for all files under parquet dataset dir
            # see: https://kuzudb.com/docusaurus/data-import/csv-import#copy-from-multiple-csv-files-to-a-single-table
            ingest_stmt = f'COPY {table_name} FROM "{table}/*.parquet"'
            print(ingest_stmt)
            table_count += 1
            print(f"Table count: {table_count}")
            kz_execute_with_retries(kz_conn=kz_conn, kz_stmt=ingest_stmt)
        elif decoded_type == "rel":
            rel_node_pairs = list(pathlib.Path(table).glob("*"))

            sub_table_count = 1
            for rel_node_pair in rel_node_pairs:
                rel_node_pair_name = rel_node_pair.name

                ingest_stmt = (
                    f'COPY {table_name} FROM "{rel_node_pair}/*.parquet"'
                    if len(rel_node_pairs) == 1
                    else f'COPY {table_name}_{rel_node_pair_name} FROM "{rel_node_pair}/*.parquet"'
                )
                print(ingest_stmt)
                print(f"Table count: {table_count}, Sub-table count: {sub_table_count}")
                sub_table_count += 1
                kz_execute_with_retries(kz_conn=kz_conn, kz_stmt=ingest_stmt)

            table_count += 1
print("Finished running Kuzu COPY statements.")


