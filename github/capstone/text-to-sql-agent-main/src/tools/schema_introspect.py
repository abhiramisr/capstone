"""Load CSV into DuckDB and produce a structured schema summary."""

from __future__ import annotations

import json
import os

import duckdb

from src.models.schemas import SchemaColumn, SchemaSummary, TableSchema

_CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "config")


def _load_pii_columns() -> set[str]:
    """Load PII column names from config, with fallback to hardcoded defaults."""
    try:
        path = os.path.join(_CONFIG_DIR, "pii_config.json")
        with open(path, "r") as f:
            config = json.load(f)
        return {c.lower() for c in config.get("pii_columns", [])}
    except (FileNotFoundError, json.JSONDecodeError):
        return {"name", "email", "phone", "address"}


def _load_allowed_tables() -> set[str]:
    """Load allowed table names from config, with fallback to hardcoded defaults."""
    try:
        path = os.path.join(_CONFIG_DIR, "allowlist_config.json")
        with open(path, "r") as f:
            config = json.load(f)
        return set(config.get("allowed_tables", []))
    except (FileNotFoundError, json.JSONDecodeError):
        return {"retail_transactions", "retail_transactions_typed"}


# Module-level variables used by sql_validate.py
_PII_COLUMNS = _load_pii_columns()
ALLOWED_TABLES = _load_allowed_tables()


def load_csv_to_duckdb(csv_path: str) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection and load the CSV as a table.

    Creates:
      - ``retail_transactions`` — raw import via ``read_csv_auto``
      - ``retail_transactions_typed`` — view with parsed date/time columns
    """
    conn = duckdb.connect(database=":memory:")

    # Load raw table
    conn.execute(
        f"""
        CREATE TABLE retail_transactions AS
        SELECT * FROM read_csv_auto('{csv_path}', header=true, ignore_errors=true)
        """
    )

    # Create typed view with aliased date/time columns.
    # DuckDB read_csv_auto already parses Date as DATE and Time as TIME,
    # so we just alias them for a consistent contract.
    conn.execute(
        """
        CREATE VIEW retail_transactions_typed AS
        SELECT
            *,
            "Date" AS date_parsed,
            "Time" AS time_parsed
        FROM retail_transactions
        """
    )

    return conn


def get_schema_summary(conn: duckdb.DuckDBPyConnection) -> SchemaSummary:
    """Query DuckDB for column metadata and return a SchemaSummary."""
    rows = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'retail_transactions_typed' "
        "ORDER BY ordinal_position"
    ).fetchall()

    columns: list[SchemaColumn] = []
    for col_name, col_type in rows:
        columns.append(
            SchemaColumn(
                name=col_name,
                type=col_type,
                pii=col_name.lower() in _PII_COLUMNS,
            )
        )

    table = TableSchema(
        name="retail_transactions_typed",
        description="Retail transactions from CSV with parsed date/time columns",
        columns=columns,
        primary_key_candidates=["Transaction_ID"],
    )

    row_count = conn.execute(
        "SELECT COUNT(*) FROM retail_transactions_typed"
    ).fetchone()[0]

    return SchemaSummary(
        tables=[table],
        recommended_table="retail_transactions_typed",
        notes=[f"Total rows: {row_count}"],
    )


def load_datasource_config() -> dict:
    """Load the datasource configuration for the query router."""
    try:
        path = os.path.join(_CONFIG_DIR, "datasource_config.json")
        with open(path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"datasources": []}
