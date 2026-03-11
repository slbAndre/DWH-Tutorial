# transformation2 for crm_prd_info standardization.
# input: dwh.ingestion.crm_prd_info -> output: dwh.transformation.crm_prd_info.
from pathlib import Path
import sys

import pandas as pd
import pyodbc

# Ensure imports work when this file is run directly from the transformation folder.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.ingestion_ddl import get_database_connection


TARGET_SCHEMA = "transformation"
TARGET_TABLE = "crm_prd_info"


def load_crm_prd_info_dataframe(database_name: str = "DWH") -> pd.DataFrame:
    """Read ingestion.crm_prd_info into a pandas DataFrame."""
    conn = get_database_connection(database_name)
    try:
        query = "SELECT * FROM ingestion.crm_prd_info"
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()


def transform_crm_prd_info(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Apply product cleaning rules for the transformation layer."""
    transformed = dataframe.copy()

    transformed["prd_cost"] = pd.to_numeric(transformed["prd_cost"], errors="coerce")
    transformed["prd_start_dt"] = pd.to_datetime(
        transformed["prd_start_dt"], errors="coerce"
    )

    # 1) Set negative prd_cost to 0.
    transformed.loc[transformed["prd_cost"] < 0, "prd_cost"] = 0

    # 2) Break prd_key into two columns; format only first split part ('-' -> '_').
    transformed["cat_id"] = (
        transformed["prd_key"]
        .str.slice(0, 5)
        .str.replace("-", "_", regex=False)
    )
    transformed["prd_key"] = transformed["prd_key"].str.slice(6)

    # 3) end_dt = start_dt - 1 day for the previous product row within the same product key.
    transformed = transformed.sort_values(
        ["prd_key", "prd_start_dt", "prd_id"],
        kind="mergesort",
        na_position="last",
    )
    next_start_dt = transformed.groupby("prd_key")["prd_start_dt"].shift(-1)
    transformed["prd_end_dt"] = (next_start_dt - pd.Timedelta(days=1)).dt.date

    # 4) Change prd_line names: M->Mountain, R->Road, S->Sport, T->Touring.
    prd_line_codes = transformed["prd_line"].apply(
        lambda value: value.strip().upper() if isinstance(value, str) else None
    )
    transformed["prd_line"] = prd_line_codes.map(
        {
            "M": "Mountain",
            "R": "Road",
            "S": "Sport",
            "T": "Touring",
        }
    ).fillna("n/a")

    transformed["prd_start_dt"] = transformed["prd_start_dt"].dt.date

    return transformed


def write_transformed_to_sql(dataframe: pd.DataFrame, database_name: str = "DWH") -> None:
    """Write transformed product data into transformation.crm_prd_info."""
    conn = get_database_connection(database_name)
    cursor = conn.cursor()

    ddl_statements = [
        f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}",
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} (
            prd_id INTEGER,
            cat_id TEXT,
            prd_key TEXT,
            prd_nm TEXT,
            prd_cost NUMERIC(10, 2),
            prd_line TEXT,
            prd_start_dt DATE,
            prd_end_dt DATE
        )
        """,
        f"TRUNCATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE}",
    ]

    ordered_columns = [
        "prd_id",
        "cat_id",
        "prd_key",
        "prd_nm",
        "prd_cost",
        "prd_line",
        "prd_start_dt",
        "prd_end_dt",
    ]

    insert_statement = f"""
        INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE} (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    rows = [
        tuple(None if pd.isna(value) else value for value in row)
        for row in dataframe[ordered_columns].itertuples(index=False, name=None)
    ]

    try:
        for statement in ddl_statements:
            cursor.execute(statement)

        if rows:
            cursor.fast_executemany = True
            cursor.executemany(insert_statement, rows)

        conn.commit()
    except pyodbc.Error:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    source_dataframe = load_crm_prd_info_dataframe()
    transformed_dataframe = transform_crm_prd_info(source_dataframe)
    write_transformed_to_sql(transformed_dataframe)

    print(f"Source rows: {len(source_dataframe)}")
    print(f"Transformed rows: {len(transformed_dataframe)}")
    print(
        f"Loaded {len(transformed_dataframe)} rows into "
        f"{TARGET_SCHEMA}.{TARGET_TABLE}."
    )
