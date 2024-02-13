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


# create path for the kuzu database to reside
if pathlib.Path(kuzu_dir).is_dir():
    shutil.rmtree(kuzu_dir)
pathlib.Path(kuzu_dir).mkdir(exist_ok=True)

# init a Kuzu database and connection
db = kuzu.Database(f"{kuzu_dir}")
kz_conn = kuzu.Connection(db)


def gather_table_names_from_parquet_path(
    parquet_path: str,
    column_with_table_name: str = "id",
):
    with duckdb.connect() as ddb:
        return [
            element[0]
            for element in ddb.execute(
                f"""
            SELECT DISTINCT {column_with_table_name}
            FROM read_parquet('{parquet_path}')
            """
            ).fetchall()
        ]


def generate_cypher_table_create_stmt_from_parquet_path(
    parquet_path: str,
    table_type: Literal["node", "rel"],
    table_name: str,
    rel_table_field_mapping: Optional[List[str]] = None,
    table_pkey_parquet_field_name: str = "id",
    edges_to_or_from_fieldnames: List[str] = ["subject", "object"],
):

    if pathlib.Path(parquet_path).is_dir():
        # use first file discovered as basis for schema
        parquet_path = next(pathlib.Path(parquet_path).rglob("*.parquet"))

    parquet_schema = parquet.read_schema(parquet_path)

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
    if table_type == "node":

        if table_pkey_parquet_field_name not in [
            field.name for field in parquet_schema
        ]:
            raise LookupError(
                f"Unable to find field {table_pkey_parquet_field_name} in parquet file {parquet_path}."
            )

        return (
            f"CREATE NODE TABLE {table_name}"
            f"({cypher_fields_from_parquet_schema}, "
            f"PRIMARY KEY ({table_pkey_parquet_field_name}))"
        )

    # else we return for rel tables
    # single or as a group, see here for more:
    # https://kuzudb.com/docusaurus/cypher/data-definition/create-table/#create-rel-table-group

    # compile string version of the node type possibilities for kuzu rel group
    subj_and_objs = ", ".join(
        [f"FROM {subj} TO {obj}" for subj, obj in rel_table_field_mapping]
    )

    rel_tbl_start = (
        f"CREATE REL TABLE {table_name}"
        if len(rel_table_field_mapping) == 1
        else f"CREATE REL TABLE GROUP {table_name}"
    )

    return f"{rel_tbl_start} ({subj_and_objs}, {cypher_fields_from_parquet_schema})"


def lookup_node_category_from_id(nodes_dataset_path: str, node_id: str):
    with duckdb.connect() as ddb:
        return ddb.execute(
            f"""
            SELECT DISTINCT category
            FROM read_parquet('{nodes_dataset_path}') node
            WHERE node.id = '{node_id}';
            """
        ).fetchone()[0]


# +
lookup_func = object()

for path, table_name_column, primary_key in [
    [f"{parquet_metanames_dir}/nodes", "category", "id"],
    [f"{parquet_metanames_dir}/edges", "predicate", None],
]:

    decoded_type = dataset_name_to_cypher_table_type_map[pathlib.Path(path).name]

    for table_name in gather_table_names_from_parquet_path(
        parquet_path=f"{path}/**", column_with_table_name=table_name_column
    ):
        # create metanames / objects using cypher safe name and dir
        cypher_safe_table_name = table_name.split(":")[1]
        parquet_metanames_metaname_base = f"{path}/{cypher_safe_table_name}"

        drop_table_if_exists(kz_conn=kz_conn, table_name=cypher_safe_table_name)

        if decoded_type == "node":
            create_stmt = generate_cypher_table_create_stmt_from_parquet_path(
                parquet_path=parquet_metanames_metaname_base,
                table_type=decoded_type,
                table_name=cypher_safe_table_name,
                table_pkey_parquet_field_name=primary_key,
            )
        elif decoded_type == "rel":

            create_stmt = generate_cypher_table_create_stmt_from_parquet_path(
                parquet_path=parquet_metanames_metaname_base,
                table_type=decoded_type,
                table_name=cypher_safe_table_name,
                table_pkey_parquet_field_name=primary_key,
                rel_table_field_mapping=[
                    str(pathlib.Path(element).name).split("_")
                    for element in list(
                        pathlib.Path(parquet_metanames_metaname_base).glob("*")
                    )
                ],
            )

        print(
            f"Using the following create statement to create table:\n\n{create_stmt}\n\n"
        )
        kz_conn.execute(create_stmt)
# -

# note: we provide specific ordering here to ensure nodes are created before edges
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
            # kz_conn.execute(ingest_stmt)
        elif decoded_type == "rel":
            rel_node_pairs = list(pathlib.Path(table).glob("*"))
            for rel_node_pair in rel_node_pairs:
                rel_node_pair_name = rel_node_pair.name
                ingest_stmt = (
                    f'COPY {table_name} FROM "{rel_node_pair}/*.parquet"'
                    if len(rel_node_pairs) == 1
                    else f'COPY {table_name}_{rel_node_pair_name} FROM "{rel_node_pair}/*.parquet"'
                )
                print(ingest_stmt)
                # kz_conn.execute(rel_ingest_stmt)


