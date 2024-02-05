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

# # Generate RTX-KG2 Sample Parquet to Kuzu

# +
import gzip
import json
import pathlib
import shutil
from typing import Any, Dict, Generator, List, Literal

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
print(f"Kuzu dir: {kuzu_dir}")


# create path for the kuzu database to reside
shutil.rmtree(kuzu_dir)
pathlib.Path(kuzu_dir).mkdir(exist_ok=True)

# init a Kuzu database and connection
db = kuzu.Database(f"{kuzu_dir}")
kz_conn = kuzu.Connection(db)

type(kz_conn)


def generate_cypher_table_create_stmt_from_parquet_file(
    parquet_file: str,
    table_type: Literal["node", "rel"],
    table_name: str,
    table_pkey_parquet_field_name: str = "id",
    rel_table_field_mapping: Dict[str, str] = {"from": "Nodes", "to": "Nodes"},
):

    parquet_schema = parquet.read_schema(parquet_file)

    if table_pkey_parquet_field_name not in [field.name for field in parquet_schema]:
        raise LookupError(
            f"Unable to find field {table_pkey_parquet_field_name} in parquet file {parquet_file}."
        )

    # Map Parquet data types to Cypher data types
    parquet_to_cypher_type_mapping = {
        "string": "STRING",
        "int32": "INT32",
        "int64": "INT64",
        "number": "FLOAT",
        "float": "FLOAT",
        "double": "FLOAT",
        "boolean": "BOOLEAN",
        "object": "MAP",
        "array": "LIST",
        "list<element: string>": "INT64[]",
        "null": "NULL",
        "date": "DATE",
        "time": "TIME",
        "datetime": "DATETIME",
        "timestamp": "DATETIME",
        "any": "ANY",
    }

    # Generate Cypher field type statements
    cypher_fields_from_parquet_schema = ", ".join(
        [
            # note: we use string splitting here for nested types
            # for ex. list<element: string>
            f"{field.name} {parquet_to_cypher_type_mapping.get(str(field.type))}"
            for field in parquet_schema
        ]
    )

    # branch for creating node table
    if table_type == "node":
        return (
            f"CREATE NODE TABLE {table_name}"
            f"({cypher_fields_from_parquet_schema}, "
            f"PRIMARY KEY ({table_pkey_parquet_field_name}))"
        )

    # else we return for rel tables
    return (
        f"CREATE REL TABLE {table_name}"
        f"(FROM {rel_table_field_mapping['from']} TO {rel_table_field_mapping['to']}, "
        f"{cypher_fields_from_parquet_schema})"
    )


def drop_table_if_exists(kz_conn: kuzu.connection.Connection, table_name: str):
    try:
        kz_conn.execute(f"DROP TABLE {table_name}")
    except Exception as e:
        print(e)
        print("Warning: no need to drop table.")


# +
drop_table_if_exists(kz_conn=kz_conn, table_name="Relationships")
drop_table_if_exists(kz_conn=kz_conn, table_name="Nodes")


for path in pathlib.Path(parquet_dir).glob("*"):

    # use first file discovered as basis for schema
    first_pq_file = next(pathlib.Path(path).glob("*.parquet"))

    if first_pq_file.parent.name == "nodes":

        create_stmt = generate_cypher_table_create_stmt_from_parquet_file(
            parquet_file=first_pq_file,
            table_type="node",
            table_name="Nodes",
            table_pkey_parquet_field_name="id",
        )

    elif first_pq_file.parent.name == "edges":

        create_stmt = generate_cypher_table_create_stmt_from_parquet_file(
            parquet_file=first_pq_file,
            table_type="rel",
            table_name="Relationships",
            table_pkey_parquet_field_name="id",
        )

    print(f"Using the following create statement to create table:\n\n{create_stmt}\n\n")
    kz_conn.execute(create_stmt)
