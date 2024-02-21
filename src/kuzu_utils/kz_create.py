"""
Work towards Kuzu table creation based on Parquet datasets.
"""

import pathlib
from typing import Dict, List, Literal, Optional

import duckdb
import kuzu
from pyarrow import parquet


def generate_cypher_table_create_stmt_from_parquet_path(
    parquet_path: str,
    table_type: Literal["node", "rel"],
    table_name: str,
    rel_table_field_mapping: Optional[List[str]] = None,
    table_pkey_parquet_field_name: str = "id",
):
    """
    Generate Kuzu Cypher statement for table creation.
    """

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
                f"Unable to find field {table_pkey_parquet_field_name}"
                " in parquet file {parquet_path}."
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


def create_kuzu_tables(
    parquet_dir: str,
    dataset_name_to_cypher_table_type_map: Dict[str, str],
    kz_conn: kuzu.connection.Connection,
):
    """
    Create Kuzu tables based on a Parquet dataset.
    """

    for path, table_name_column, primary_key in [
        [f"{parquet_dir}/nodes", "category", "id"],
        [f"{parquet_dir}/edges", "predicate", None],
    ]:
        decoded_type = dataset_name_to_cypher_table_type_map[pathlib.Path(path).name]

        for table_name in gather_table_names_from_parquet_path(
            parquet_path=f"{path}/**", column_with_table_name=table_name_column
        ):
            # create metanames / objects using cypher safe name and dir
            cypher_safe_table_name = table_name.split(":")[1]
            parquet_metanames_metaname_base = f"{path}/{cypher_safe_table_name}"

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
