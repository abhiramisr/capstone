"""Tests for schema introspection."""

import os
import pytest

from src.tools.schema_introspect import get_schema_summary, load_csv_to_duckdb

# Path to the test CSV
CSV_PATH = os.path.join(
    os.path.dirname(__file__), "..", "new_retail_data 1.csv"
)


@pytest.fixture
def conn():
    """Load CSV and return DuckDB connection."""
    if not os.path.exists(CSV_PATH):
        pytest.skip(f"CSV not found at {CSV_PATH}")
    return load_csv_to_duckdb(CSV_PATH)


class TestSchemaIntrospect:
    def test_tables_created(self, conn) -> None:
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        table_names = {t[0] for t in tables}
        assert "retail_transactions" in table_names

    def test_view_created(self, conn) -> None:
        # The typed view should be queryable
        result = conn.execute("SELECT COUNT(*) FROM retail_transactions_typed").fetchone()
        assert result[0] > 0

    def test_schema_summary_columns(self, conn) -> None:
        schema = get_schema_summary(conn)
        assert len(schema.tables) == 1

        table = schema.tables[0]
        col_names = {c.name for c in table.columns}

        # Check some expected columns exist
        assert "Transaction_ID" in col_names or "transaction_id" in {n.lower() for n in col_names}
        assert "Total_Amount" in col_names or "total_amount" in {n.lower() for n in col_names}
        assert "date_parsed" in col_names

    def test_pii_tagging(self, conn) -> None:
        schema = get_schema_summary(conn)
        pii = schema.pii_columns
        pii_lower = {c.lower() for c in pii}
        assert "email" in pii_lower
        assert "phone" in pii_lower
        assert "name" in pii_lower
        assert "address" in pii_lower

    def test_row_count_in_notes(self, conn) -> None:
        schema = get_schema_summary(conn)
        assert any("Total rows:" in n for n in schema.notes)
