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

# +
import os
import pathlib
import tarfile

import gdown
import kuzu
# -

# set some variables for the work below
gdrive_url = "https://drive.google.com/uc?id=1H9_jiLgvjr_AjoYPu_Pi9C_47Zm1lIei"
target_dir = "data"
target_database_path = f"{target_dir}/kg2c_lite_2.8.4.full.with-metanames.dataset.kuzu"


def download_and_extract_gdrive(url, output_dir):

    temp_download_path = "temp.tar.gz"

    # Download the file from Google Drive
    print("Downloading file.")
    gdown.download(url, temp_download_path, quiet=True)

    # Extract the tar.gz file
    print("Extracting database.")
    with tarfile.open(temp_download_path, "r:gz") as tar:
        tar.extractall(output_dir)

    print("Cleaning up download.")
    # Remove the temporary tar.gz file
    pathlib.Path(temp_download_path).unlink()

    return output_dir


# niave check for existing database to avoid redownloading / extracting if possible
if not pathlib.Path(target_database_path).is_dir():
    download_and_extract_gdrive(gdrive_url, target_dir)

# init a Kuzu database and connection
db = kuzu.Database(target_database_path)
kz_conn = kuzu.Connection(db)

kz_conn.execute("CALL SHOW_TABLES() RETURN *;").df()
