"""Tests for the deterministic SQL validator."""

import pytest

from src.models.schemas import SchemaColumn, SchemaSummary, TableSchema
from src.tools.sql_validate import inject_limit, validate_sql


@pytest.fixture
def schema() -> SchemaSummary:
    """Minimal schema for testing."""
    return SchemaSummary(
        tables=[
            TableSchema(
                name="retail_transactions_typed",
                columns=[
                    SchemaColumn(name="Transaction_ID", type="DOUBLE"),
                    SchemaColumn(name="Customer_ID", type="DOUBLE"),
                    SchemaColumn(name="Name", type="VARCHAR", pii=True),
                    SchemaColumn(name="Email", type="VARCHAR", pii=True),
                    SchemaColumn(name="Total_Amount", type="DOUBLE"),
                    SchemaColumn(name="Product_Category", type="VARCHAR"),
                    SchemaColumn(name="date_parsed", type="DATE"),
                    SchemaColumn(name="Country", type="VARCHAR"),
                ],
            )
        ],
        recommended_table="retail_transactions_typed",
    )


# ---- PASS cases ----

class TestValidQueries:
    def test_simple_select(self, schema: SchemaSummary) -> None:
        result = validate_sql(
            "SELECT Product_Category, SUM(Total_Amount) FROM retail_transactions_typed GROUP BY Product_Category LIMIT 10",
            schema,
        )
        assert result.is_safe
        assert result.is_read_only
        assert not result.has_blockers

    def test_with_cte(self, schema: SchemaSummary) -> None:
        sql = """
        WITH top_cats AS (
            SELECT Product_Category, SUM(Total_Amount) as total
            FROM retail_transactions_typed
            GROUP BY Product_Category
        )
        SELECT * FROM top_cats ORDER BY total DESC LIMIT 5
        """
        result = validate_sql(sql, schema)
        assert result.is_safe
        assert not result.has_blockers

    def test_aggregate_no_limit(self, schema: SchemaSummary) -> None:
        """A single aggregate (e.g. COUNT(*)) without LIMIT should be okay."""
        result = validate_sql(
            "SELECT COUNT(*) FROM retail_transactions_typed",
            schema,
        )
        assert result.is_safe
        assert not result.has_blockers


# ---- REJECT cases ----

class TestDangerousQueries:
    def test_drop_table(self, schema: SchemaSummary) -> None:
        result = validate_sql("DROP TABLE retail_transactions_typed", schema)
        assert result.has_blockers

    def test_multi_statement(self, schema: SchemaSummary) -> None:
        result = validate_sql(
            "SELECT 1; DROP TABLE retail_transactions_typed",
            schema,
        )
        assert result.has_blockers

    def test_insert_into(self, schema: SchemaSummary) -> None:
        result = validate_sql(
            "INSERT INTO retail_transactions_typed (Transaction_ID) VALUES (1)",
            schema,
        )
        assert result.has_blockers

    def test_update(self, schema: SchemaSummary) -> None:
        result = validate_sql(
            "UPDATE retail_transactions_typed SET Total_Amount = 0",
            schema,
        )
        assert result.has_blockers

    def test_delete(self, schema: SchemaSummary) -> None:
        result = validate_sql(
            "DELETE FROM retail_transactions_typed WHERE 1=1",
            schema,
        )
        assert result.has_blockers

    def test_create_table(self, schema: SchemaSummary) -> None:
        result = validate_sql(
            "CREATE TABLE evil AS SELECT * FROM retail_transactions_typed",
            schema,
        )
        assert result.has_blockers


# ---- Schema checks ----

class TestSchemaChecks:
    def test_unknown_table(self, schema: SchemaSummary) -> None:
        result = validate_sql(
            "SELECT * FROM secret_table LIMIT 10",
            schema,
        )
        assert result.has_blockers

    def test_unknown_column_is_warning(self, schema: SchemaSummary) -> None:
        """Unknown columns generate warnings, not blockers (could be aliases)."""
        result = validate_sql(
            "SELECT nonexistent_col FROM retail_transactions_typed LIMIT 10",
            schema,
        )
        # Should be a warning, not a blocker
        assert not result.has_blockers
        warn_msgs = [i.message for i in result.issues if i.severity == "warn"]
        assert any("nonexistent_col" in m.lower() for m in warn_msgs)


# ---- LIMIT injection ----

class TestLimitInjection:
    def test_limit_injected_when_missing(self, schema: SchemaSummary) -> None:
        result = validate_sql(
            "SELECT * FROM retail_transactions_typed",
            schema,
        )
        assert any("LIMIT" in (i.message or "") for i in result.issues)

    def test_inject_limit_function(self) -> None:
        sql = "SELECT * FROM retail_transactions_typed"
        result = inject_limit(sql, 100)
        assert "100" in result
        assert "LIMIT" in result.upper()

    def test_inject_limit_preserves_existing(self) -> None:
        sql = "SELECT * FROM retail_transactions_typed LIMIT 10"
        result = inject_limit(sql, 100)
        # Should keep original LIMIT 10
        assert "10" in result
