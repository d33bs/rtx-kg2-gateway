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

# # Example Cypher Read of RTX-KG2 with Kuzu
#
# Related example found online at: [https://colab.research.google.com/drive/1XGZf2xhFwvNOFCXVCi8pAPvpu0NOMmzi](https://colab.research.google.com/drive/1XGZf2xhFwvNOFCXVCi8pAPvpu0NOMmzi)

# +
import os
import pathlib
import tarfile

import kuzu
from rtx_kg2_functions import download_file, extract_tar_gz

# -

# set some variables for the work below
source_data_url = "https://github.com/CU-DBMI/rtx-kg2-gateway/releases/download/v0.0.1/kg2c_lite_2.8.4.full.with-metanames.dataset.kuzu.tar.gz"
target_dir = "data"
target_database_path = f"{target_dir}/kg2c_lite_2.8.4.full.with-metanames.dataset.kuzu"

pathlib.Path(target_dir).mkdir(exist_ok=True)

# niave check for existing database to avoid redownloading / extracting if possible
if not pathlib.Path(target_database_path).is_dir():
    downloaded_file = download_file(url=source_data_url, download_dir=target_dir)
    extract_dir = extract_tar_gz(
        tar_gz_path=f"{target_dir}/{downloaded_file}", output_dir=target_dir
    )

# init a Kuzu database and connection
db = kuzu.Database(target_database_path)
kz_conn = kuzu.Connection(db)

# show tables
kz_conn.execute("CALL SHOW_TABLES() RETURN *;").get_as_df()

# run an example query
kz_conn.execute(
    """
    MATCH (d:Disease)
    WHERE d.name = "Down syndrome"
    RETURN d.id, d.name, d.all_categories;
    """
).get_as_df()
