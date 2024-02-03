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
