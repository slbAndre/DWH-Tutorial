import csv
import os
from pathlib import Path

import pyodbc

try:
    from .ingestion_DDL import ensure_database_exists, get_database_connection, run_ddl
except ImportError:
    from ingestion_DDL import ensure_database_exists, get_database_connection, run_ddl


RAW_DATA_ROOT = Path(
    os.getenv("RAW_DATA_ROOT", str(Path.home() / "Desktop" / "raw datasets"))
)


CSV_LOAD_CONFIG = [
    {
        "table_name": "crm_cust_info",
        "csv_path": RAW_DATA_ROOT / "source_crm" / "cust_info.csv",
        "columns": [
            "cst_id",
            "cst_key",
            "cst_firstname",
            "cst_lastname",
            "cst_marital_status",
            "cst_gndr",
            "cst_create_date",
        ],
    },
    {
        "table_name": "crm_prd_info",
        "csv_path": RAW_DATA_ROOT / "source_crm" / "prd_info.csv",
        "columns": [
            "prd_id",
            "prd_key",
            "prd_nm",
            "prd_cost",
            "prd_line",
            "prd_start_dt",
            "prd_end_dt",
        ],
    },
    {
        "table_name": "crm_sales_details",
        "csv_path": RAW_DATA_ROOT / "source_crm" / "sales_details.csv",
        "columns": [
            "sls_ord_num",
            "sls_prd_key",
            "sls_cust_id",
            "sls_order_dt",
            "sls_ship_dt",
            "sls_due_dt",
            "sls_sales",
            "sls_quantity",
            "sls_price",
        ],
    },
    {
        "table_name": "erp_cust_az12",
        "csv_path": RAW_DATA_ROOT / "source_erp" / "CUST_AZ12.csv",
        "columns": ["CID", "BDATE", "GEN"],
    },
    {
        "table_name": "erp_loc_a101",
        "csv_path": RAW_DATA_ROOT / "source_erp" / "LOC_A101.csv",
        "columns": ["CID", "CNTRY"],
    },
    {
        "table_name": "erp_px_cat_g1v2",
        "csv_path": RAW_DATA_ROOT / "source_erp" / "PX_CAT_G1V2.csv",
        "columns": ["ID", "CAT", "SUBCAT", "MAINTENANCE"],
    },
]


def truncate_ingestion_tables(conn: pyodbc.Connection) -> None:
    """Remove all existing rows from ingestion tables before reloading."""
    cursor = conn.cursor()
    try:
        for config in CSV_LOAD_CONFIG:
            cursor.execute(f"TRUNCATE TABLE ingestion.{config['table_name']}")
        conn.commit()
    except pyodbc.Error:
        conn.rollback()
        raise
    finally:
        cursor.close()


def _read_csv_rows(csv_path: Path, columns: list[str]) -> list[tuple[object, ...]]:
    """Read CSV rows and normalize blank values to NULL."""
    rows: list[tuple[object, ...]] = []
    resolved_csv_path = csv_path.expanduser().resolve(strict=False)

    if not resolved_csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {resolved_csv_path}")

    with resolved_csv_path.open(newline="", encoding="utf-8-sig") as file_handle:
        reader = csv.DictReader(file_handle)
        for row in reader:
            rows.append(tuple(_normalize_value(row[column]) for column in columns))

    return rows


def _normalize_value(value: str | None) -> object:
    """Trim whitespace and convert blanks to NULL for database inserts."""
    if value is None:
        return None

    stripped_value = value.strip()
    if stripped_value == "":
        return None

    return stripped_value


def load_csv_to_table(conn: pyodbc.Connection, table_name: str, csv_path: Path, columns: list[str]) -> int:
    """Bulk load one CSV file into its target ingestion table."""
    rows = _read_csv_rows(csv_path, columns)
    if not rows:
        return 0

    placeholders = ", ".join("?" for _ in columns)
    target_columns = ", ".join(column.lower() for column in columns)
    insert_statement = (
        f"INSERT INTO ingestion.{table_name} ({target_columns}) "
        f"VALUES ({placeholders})"
    )

    cursor = conn.cursor()
    try:
        cursor.fast_executemany = True
        cursor.executemany(insert_statement, rows)
        conn.commit()
        return len(rows)
    except pyodbc.Error:
        conn.rollback()
        raise
    finally:
        cursor.close()


def load_all_csvs(conn: pyodbc.Connection) -> dict[str, int]:
    """Load every configured CSV into the ingestion schema."""
    row_counts: dict[str, int] = {}
    for config in CSV_LOAD_CONFIG:
        row_counts[config["table_name"]] = load_csv_to_table(
            conn,
            config["table_name"],
            config["csv_path"],
            config["columns"],
        )
    return row_counts


if __name__ == "__main__":
    conn = None
    try:
        ensure_database_exists("DWH")
        conn = get_database_connection("DWH")
        run_ddl(conn)
        truncate_ingestion_tables(conn)
        row_counts = load_all_csvs(conn)
        print("Connected to PostgreSQL successfully.")
        print("Tables truncated successfully.")
        for table_name, row_count in row_counts.items():
            print(f"Loaded {row_count} rows into ingestion.{table_name}.")
    except (pyodbc.Error, RuntimeError, FileNotFoundError) as exc:
        print(f"Data load failed: {exc}")
    finally:
        if conn is not None:
            conn.close()
