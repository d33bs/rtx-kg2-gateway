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

# # Generate RTX-KG2 Metanames Parquet to Kuzu

# +
import gzip
import json
import pathlib
import shutil
from typing import Any, Dict, Generator, List, Literal

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
print(f"Kuzu dir: {kuzu_dir}")


# create path for the kuzu database to reside
if pathlib.Path(kuzu_dir).is_dir():
    shutil.rmtree(kuzu_dir)
pathlib.Path(kuzu_dir).mkdir(exist_ok=True)

# create path for parquet metanames to reside
if pathlib.Path(parquet_metanames_dir).is_dir():
    shutil.rmtree(parquet_metanames_dir)
pathlib.Path(parquet_metanames_dir).mkdir(exist_ok=True)

# init a Kuzu database and connection
db = kuzu.Database(f"{kuzu_dir}")
kz_conn = kuzu.Connection(db)


def gather_table_names_from_parquet_path(
    parquet_path: str,
    column_with_table_name: str = "id",
):
    # return distinct table types as set comprehension
    return set(
        # create a parquet dataset and read a single column as an array
        parquet.ParquetDataset(parquet_path)
        .read(columns=[column_with_table_name])[column_with_table_name]
        .to_pylist()
    )


for path, table_name_column in [
    [f"{parquet_dir}/nodes", "category"],
    [f"{parquet_dir}/edges", "predicate"],
]:

    # create object base dir
    parquet_metanames_object_base = f"{parquet_metanames_dir}/{pathlib.Path(path).name}"
    pathlib.Path(parquet_metanames_object_base).mkdir(exist_ok=True)

    for table_name in gather_table_names_from_parquet_path(
        parquet_path=path, column_with_table_name=table_name_column
    ):
        # create metanames / objects using cypher safe name and dir
        cypher_safe_table_name = table_name.split(":")[1]
        parquet_metanames_metaname_base = (
            f"{parquet_metanames_object_base}/{cypher_safe_table_name}"
        )
        pathlib.Path(parquet_metanames_metaname_base).mkdir(exist_ok=True)

        print(f"Exporting metaname tables for: {table_name}")

        # determine rowcount for offsetting parquet files by metaname
        with duckdb.connect() as ddb:
            rowcount = int(
                ddb.execute(
                    f"""
                    SELECT COUNT(*) as count
                    FROM read_parquet('{path}/*')
                    WHERE {table_name_column}='{table_name}';
                    """
                ).fetchone()[0]
            )

        # create a chunk offsets list for chunked parquet output by metaname
        chunk_offsets = list(
            range(
                0,
                # gather rowcount from table and use as maximum for range
                rowcount,
                # step through using chunk size
                chunk_size,
            )
        )

        # gather chunks of data by metaname and export to file using chunked offsets
        with duckdb.connect() as ddb:
            for idx, offset in enumerate(chunk_offsets):
                ddb.execute(
                    f"""
                    COPY (
                        SELECT *
                        FROM read_parquet('{path}/*')
                        WHERE {table_name_column}='{table_name}'
                        LIMIT {chunk_size} OFFSET {offset}
                    )
                    TO '{parquet_metanames_metaname_base}/{cypher_safe_table_name}.{idx}.parquet' (FORMAT PARQUET);
                    """
                )


# +
def gather_parquet_dataset_size_from_path(parquet_path: str):
    with duckdb.connect() as ddb:
        return int(
            ddb.execute(
                f"""
                SELECT COUNT(*) as count
                FROM read_parquet('{parquet_path}')
                """
            ).fetchone()[0]
        )


for source, target in [
    [
        "data/kg2c_lite_2.8.4.full.dataset.parquet/nodes/*",
        "data/kg2c_lite_2.8.4.full.with-metanames.dataset.parquet/nodes/**",
    ],
    [
        "data/kg2c_lite_2.8.4.full.dataset.parquet/edges/*",
        "data/kg2c_lite_2.8.4.full.with-metanames.dataset.parquet/edges/**",
    ],
]:
    if gather_parquet_dataset_size_from_path(
        source
    ) != gather_parquet_dataset_size_from_path(target):
        raise ValueError(
            f"Inequal number of rows from parquet source {source} to metanames target {target}."
        )
    else:
        print(f"Equal values from parquet source {source} to metanames target {target}")
