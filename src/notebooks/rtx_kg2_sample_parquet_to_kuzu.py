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

[
    val[0].as_py()
    for val in pc.split_pattern(
        parquet.ParquetDataset(f"{parquet_dir}/nodes").read(columns=["id"])["id"], ":"
    )
]

# +
import pyarrow as pa
import pyarrow.compute as pc

# Create a sample string column
string_column = pa.array(
    ["biolink_download_source:biolink-model.owl.ttl", "biolink:overlaps"]
)

# Split the string on ":"
# -



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


edge_types = gather_table_names_from_parquet_path(
    parquet_path="data/kg2c_lite_2.8.4.full.dataset.parquet/edges",
    column_with_table_name="predicate",
)

list(edge_types)[:3]

with duckdb.connect() as ddb:
    result = ddb.execute(
        """
        WITH sub_and_preds AS (
            SELECT object, subject
            FROM read_parquet('data/kg2c_lite_2.8.4.full.dataset.parquet/edges/*')
            WHERE predicate='biolink:subclass_of'
        )
        SELECT distinct category
        FROM read_parquet('data/kg2c_lite_2.8.4.full.dataset.parquet/nodes/*'), sub_and_preds
        WHERE id = sub_and_preds.object
        """
    ).df()
    print(result)


def generate_cypher_table_create_stmt_from_parquet_path(
    parquet_path: str,
    table_type: Literal["node", "rel"],
    table_name: str,
    rel_table_field_mapping: Dict[str, str] = {"from": "nodes", "to": "nodes"},
    table_pkey_parquet_field_name: str = "id",
    edges_to_or_from_fieldnames: List[str] = ["subject", "object"],
):

    if pathlib.Path(parquet_path).is_dir():
        # use first file discovered as basis for schema
        parquet_path = next(pathlib.Path(path).glob("*.parquet"))

    parquet_schema = parquet.read_schema(parquet_path)

    if table_pkey_parquet_field_name not in [field.name for field in parquet_schema]:
        raise LookupError(
            f"Unable to find field {table_pkey_parquet_field_name} in parquet file {parquet_path}."
        )

    # Map Parquet data types to Cypher data types
    # more details here: https://kuzudb.com/docusaurus/cypher/data-types/
    parquet_to_cypher_type_mapping = {
        "string": "STRING",
        "int32": "INT32",
        "int64": "INT64",
        "number": "FLOAT",
        "float": "FLOAT",
        "double": "FLOAT",
        "boolean": "BOOLEAN",
        "object": "MAP",
        "array": "INT64[]",
        "list<element: string>": "STRING[]",
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
            for idx, field in enumerate(parquet_schema)
            if table_type == "node" or (table_type == "rel" and idx > 1)
        ]
    )

    # branch for creating node table
    if table_type == "nodes":
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


# +
# note: we provide specific ordering here to ensure nodes are created before edges
dataset_name_to_cypher_table_type_map = {"nodes": "node", "edges": "rel"}


for path, table_name_column, primary_key in [
    [f"{parquet_dir}/nodes", "category", "id"],
    # [f"{parquet_dir}/edges", "predicate", None],
]:

    for table_name in gather_table_names_from_parquet_path(
        parquet_path=path, column_with_table_name=table_name_column
    ):

        cypher_table_name = table_name.split(":")[1]

        drop_table_if_exists(kz_conn=kz_conn, table_name=cypher_table_name)

        create_stmt = generate_cypher_table_create_stmt_from_parquet_path(
            parquet_path=path,
            table_type=dataset_name_to_cypher_table_type_map[pathlib.Path(path).name],
            table_name=cypher_table_name,
            table_pkey_parquet_field_name=primary_key,
        )

        print(
            f"Using the following create statement to create table:\n\n{create_stmt}\n\n"
        )
        kz_conn.execute(create_stmt)
# -

# note: we provide specific ordering here to ensure nodes are created before edges
for path in [f"{parquet_dir}/nodes", f"{parquet_dir}/edges"]:

    print(f"Working on kuzu ingest of parquet dataset: {path} ")
    # uses wildcard functionality for all files under parquet dataset dir
    # see: https://kuzudb.com/docusaurus/data-import/csv-import#copy-from-multiple-csv-files-to-a-single-table
    kz_conn.execute(f'COPY {pathlib.Path(path).name} FROM "{path}/*.parquet"')
