from pathlib import Path
import sys

import pandas as pd
import pyodbc

# Ensure imports work when this file is run directly from the Transformation folder.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.ingestion_DDL import get_database_connection


TARGET_SCHEMA = "transformation"
TARGET_TABLE = "crm_cust_info"


def load_crm_cust_info_dataframe(database_name: str = "DWH") -> pd.DataFrame:
    """Read ingestion.crm_cust_info into a pandas DataFrame."""
    conn = get_database_connection(database_name)
    try:
        query = "SELECT * FROM ingestion.crm_cust_info"
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()


def transform_crm_cust_info(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Apply customer cleaning rules for the transformation layer."""
    transformed = dataframe.copy()

    # 1) Drop rows where cst_id is null/non-numeric.
    transformed["cst_id"] = pd.to_numeric(transformed["cst_id"], errors="coerce")
    transformed = transformed[transformed["cst_id"].notna()].copy()
    transformed["cst_id"] = transformed["cst_id"].astype("int64")

    # 2) Deduplicate on cst_id, keeping the last record by cst_create_date.
    transformed["cst_create_date"] = pd.to_datetime(
        transformed["cst_create_date"], errors="coerce"
    ).dt.date
    transformed = transformed.sort_values(
        ["cst_id", "cst_create_date"],
        kind="mergesort",
        na_position="first",
    )
    transformed = transformed.drop_duplicates(subset=["cst_id"], keep="last")

    # 3) Trim leading/trailing whitespace from first/last names.
    for column in ("cst_firstname", "cst_lastname"):
        transformed[column] = transformed[column].apply(
            lambda value: value.strip() if isinstance(value, str) else value
        )

    # 4) Expand marital status codes to readable values.
    marital_status_codes = transformed["cst_marital_status"].apply(
        lambda value: value.strip().upper() if isinstance(value, str) else None
    )
    transformed["cst_marital_status"] = marital_status_codes.map(
        {
            "M": "Married",
            "S": "Single",
        }
    ).fillna("n/a")

    # 5) Expand gender codes to readable values.
    gender_codes = transformed["cst_gndr"].apply(
        lambda value: value.strip().upper() if isinstance(value, str) else None
    )
    transformed["cst_gndr"] = gender_codes.map(
        {
            "M": "Male",
            "F": "Female",
        }
    ).fillna("n/a")

    return transformed


def write_transformed_to_sql(dataframe: pd.DataFrame, database_name: str = "DWH") -> None:
    """Write transformed customer data into transformation.crm_cust_info."""
    conn = get_database_connection(database_name)
    cursor = conn.cursor()

    # Prepare target schema/table and refresh data each run.
    ddl_statements = [
        f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}",
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} (
            cst_id INTEGER,
            cst_key TEXT,
            cst_firstname TEXT,
            cst_lastname TEXT,
            cst_marital_status TEXT,
            cst_gndr TEXT,
            cst_create_date DATE
        )
        """,
        f"TRUNCATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE}",
    ]

    ordered_columns = [
        "cst_id",
        "cst_key",
        "cst_firstname",
        "cst_lastname",
        "cst_marital_status",
        "cst_gndr",
        "cst_create_date",
    ]

    insert_statement = f"""
        INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE} (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    # Convert pandas NaN/NaT to SQL NULL before insert.
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
    # End-to-end flow: read ingestion data -> transform -> write to transformation layer.
    source_dataframe = load_crm_cust_info_dataframe()
    transformed_dataframe = transform_crm_cust_info(source_dataframe)
    write_transformed_to_sql(transformed_dataframe)

    print(f"Source rows: {len(source_dataframe)}")
    print(f"Transformed rows: {len(transformed_dataframe)}")
    print(
        f"Loaded {len(transformed_dataframe)} rows into "
        f"{TARGET_SCHEMA}.{TARGET_TABLE}."
    )
