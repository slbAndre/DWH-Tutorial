import os
from getpass import getpass

import pyodbc


DDL_STATEMENTS = [
    """
    CREATE SCHEMA IF NOT EXISTS ingestion;
    """,
    """
    CREATE TABLE IF NOT EXISTS ingestion.crm_cust_info (
        cst_id INTEGER,
        cst_key TEXT,
        cst_firstname TEXT,
        cst_lastname TEXT,
        cst_marital_status TEXT,
        cst_gndr TEXT,
        cst_create_date DATE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ingestion.crm_prd_info (
        prd_id INTEGER,
        prd_key TEXT,
        prd_nm TEXT,
        prd_cost NUMERIC(10, 2),
        prd_line TEXT,
        prd_start_dt DATE,
        prd_end_dt DATE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ingestion.crm_sales_details (
        sls_ord_num TEXT,
        sls_prd_key TEXT,
        sls_cust_id INTEGER,
        sls_order_dt INTEGER,
        sls_ship_dt INTEGER,
        sls_due_dt INTEGER,
        sls_sales NUMERIC(12, 2),
        sls_quantity INTEGER,
        sls_price NUMERIC(12, 2)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ingestion.erp_cust_az12 (
        cid TEXT,
        bdate DATE,
        gen TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ingestion.erp_loc_a101 (
        cid TEXT,
        cntry TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ingestion.erp_px_cat_g1v2 (
        id TEXT,
        cat TEXT,
        subcat TEXT,
        maintenance TEXT
    );
    """,
]


def _get_postgres_odbc_driver() -> str:
    """Return the configured PostgreSQL ODBC driver or fail with a useful error."""
    configured_driver = os.getenv("POSTGRES_ODBC_DRIVER")
    available_drivers = pyodbc.drivers()

    if configured_driver:
        if configured_driver not in available_drivers:
            raise RuntimeError(
                f"ODBC driver '{configured_driver}' is not installed. "
                f"Available drivers: {available_drivers or 'none'}"
            )
        return configured_driver

    for driver_name in ("PostgreSQL Unicode", "PostgreSQL ANSI", "psqlodbcw.so", "psqlodbca.so"):
        if driver_name in available_drivers:
            return driver_name

    raise RuntimeError(
        "No PostgreSQL ODBC driver is installed. "
        f"Available drivers: {available_drivers or 'none'}"
    )


def get_postgres_connection() -> pyodbc.Connection:
    """Create and return a PostgreSQL connection using environment variables."""
    return get_database_connection(os.getenv("POSTGRES_DB", "DWH"))


def get_database_connection(database_name: str) -> pyodbc.Connection:
    """Create and return a PostgreSQL connection for the requested database."""
    driver_name = _get_postgres_odbc_driver()
    password = os.getenv("POSTGRES_PASSWORD") or os.getenv("PGPASSWORD")

    if password is None:
        password = getpass("PostgreSQL password: ")

    connection_string = (
        f"DRIVER={{{driver_name}}};"
        f"SERVER={os.getenv('POSTGRES_HOST', 'localhost')};"
        f"PORT={os.getenv('POSTGRES_PORT', '5432')};"
        f"DATABASE={database_name};"
        f"UID={os.getenv('POSTGRES_USER', 'postgres')};"
        f"PWD={password};"
    )
    return pyodbc.connect(connection_string)


def ensure_database_exists(database_name: str) -> None:
    """Create the target database if it does not already exist."""
    admin_database = os.getenv("POSTGRES_ADMIN_DB", "postgres")
    conn = get_database_connection(admin_database)
    conn.autocommit = True
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = ?",
            database_name,
        )
        if cursor.fetchone() is None:
            cursor.execute(f'CREATE DATABASE "{database_name}"')
    finally:
        cursor.close()
        conn.close()


def run_ddl(conn: pyodbc.Connection) -> None:
    """Create the ingestion schema and source tables."""
    cursor = conn.cursor()
    try:
        for statement in DDL_STATEMENTS:
            cursor.execute(statement)
        conn.commit()
    except pyodbc.Error:
        conn.rollback()
        raise
    finally:
        cursor.close()


if __name__ == "__main__":
    conn = None
    try:
        target_database = os.getenv("POSTGRES_DB", "DWH")
        ensure_database_exists(target_database)
        conn = get_database_connection(target_database)
        run_ddl(conn)
        print("Connected to PostgreSQL successfully.")
        print(f"Database '{target_database}' is ready.")
        print("Ingestion schema DDL executed successfully.")
    except (pyodbc.Error, RuntimeError) as exc:
        print(f"PostgreSQL connection failed: {exc}")
    finally:
        if conn is not None:
            conn.close()
