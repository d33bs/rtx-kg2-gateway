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

# # RTX-KG2 Functions
#
# Convenience notebook for using functions from a single location.

# +
import gzip
import json
import pathlib
import shutil
from typing import Any, Dict, Generator, List, Literal

import ijson
import kuzu
import requests
from genson import SchemaBuilder
from pyarrow import parquet

# -


def download_file(url, download_dir):
    # referenced with modification from:
    # https://stackoverflow.com/a/16696317
    local_filename = url.split("/")[-1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(f"{download_dir}/{local_filename}", "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                # if chunk:
                f.write(chunk)
    return local_filename


def extract_tar_gz(
    tar_gz_path: str, output_dir: str, remove_tar_gz_after_extract: bool = True
):

    # Extract the tar.gz file
    print("Extracting tar gz.")
    with tarfile.open(tar_gz_path, "r:gz") as tar:
        tar.extractall(output_dir)

    if remove_tar_gz_after_extract:
        print("Removing source tar gz file.")
        # Remove the temporary tar.gz file
        pathlib.Path(tar_gz_path).unlink()

    return output_dir


def find_top_level_names(json_file: str) -> Generator[str, None, None]:
    """
    Find the topmost item names by way of streaming a json
    file through ijson.
    """
    with open(json_file, "r") as f:
        parser = ijson.parse(f)
        depth = 0
        for prefix, event, value in parser:
            if event == "start_map":
                depth += 1
            elif event == "end_map":
                depth -= 1
            elif event == "map_key" and depth == 1:
                yield value


def count_items_under_top_level_name(json_file: str, top_level_name: str):
    """
    Count items under a top level object name
    """
    count = 0
    with open(json_file, "rb") as f:
        parser = ijson.items(f, f"{top_level_name}.item")
        for item in parser:
            count += 1
    return count


def parse_items_by_topmost_item_name(
    json_file: str, topmost_item_name: str, chunk_size: int, limit: int = 0
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Parse items using a topmost object name.
    """
    with open(json_file, "r") as f:
        objects = ijson.items(f, f"{topmost_item_name}.item")
        chunk = []
        limit_count = 0
        for item in objects:
            if limit == 0 or limit_count < limit:
                chunk.append(item)
                if len(chunk) == chunk_size:
                    yield chunk
                    limit_count += 1
                    chunk = []
        # Yield the last chunk if there are remaining elements
        if chunk:
            yield chunk


def parse_metadata_by_object_name(
    json_file: str, metadata_object_name: str
) -> Generator[Any, None, None]:
    """
    Extract single value metadata from json file
    """

    with open(json_file, "r") as f:
        return next(ijson.items(f, metadata_object_name))


def generate_cypher_table_create_stmt_from_parquet_file(
    parquet_file: str,
    table_type: Literal["node", "rel"],
    table_name: str,
    table_pkey_parquet_field_name: str = "id",
    rel_table_field_mapping: Dict[str, str] = {"from": "nodes", "to": "nodes"},
    # specify a map for from to specification
    # to move these to first two cols of related table
    edges_to_or_from_fieldnames: List[str] = ["subject", "object"],
):

    parquet_schema = parquet.read_schema(parquet_file)

    if table_pkey_parquet_field_name not in [field.name for field in parquet_schema]:
        raise LookupError(
            f"Unable to find field {table_pkey_parquet_field_name} in parquet file {parquet_file}."
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
