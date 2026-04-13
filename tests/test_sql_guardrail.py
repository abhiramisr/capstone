"""Tests for the SQL guardrail — validates SQL before execution."""

import json
import os
import tempfile

import pytest

from src.guardrails.sql_guardrail import check_sql


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def allowlist_file(tmp_path):
    """Create a temporary allowlist config file."""
    config = {
        "datasources": [
            {
                "name": "TestDS",
                "type": "fabric_warehouse",
                "allowed_tables": ["sales", "customers", "products"],
                "allowed_operations": ["SELECT"],
            },
            {
                "name": "OpenDataBay",
                "type": "fabric_warehouse",
                "allowed_tables": ["retail_transactions"],
                "allowed_operations": ["SELECT"],
            },
        ]
    }
    path = tmp_path / "allowlist_config.json"
    path.write_text(json.dumps(config))
    return str(path)


# ---------------------------------------------------------------------------
# Valid queries
# ---------------------------------------------------------------------------

def test_valid_select(allowlist_file):
    result = check_sql("SELECT * FROM sales", "TestDS", allowlist_file)
    assert result["allowed"] is True


def test_valid_select_with_where(allowlist_file):
    result = check_sql(
        "SELECT name, amount FROM customers WHERE amount > 100",
        "TestDS", allowlist_file,
    )
    assert result["allowed"] is True


def test_valid_cte(allowlist_file):
    sql = "WITH top_sales AS (SELECT * FROM sales) SELECT * FROM top_sales"
    result = check_sql(sql, "TestDS", allowlist_file)
    assert result["allowed"] is True


def test_valid_join(allowlist_file):
    sql = "SELECT s.id, c.name FROM sales s JOIN customers c ON s.cid = c.id"
    result = check_sql(sql, "TestDS", allowlist_file)
    assert result["allowed"] is True


def test_valid_schema_qualified(allowlist_file):
    result = check_sql("SELECT * FROM dbo.sales", "TestDS", allowlist_file)
    assert result["allowed"] is True


# ---------------------------------------------------------------------------
# Statement type enforcement
# ---------------------------------------------------------------------------

def test_blocks_insert(allowlist_file):
    result = check_sql("INSERT INTO sales VALUES (1)", "TestDS", allowlist_file)
    assert result["allowed"] is False
    assert "INSERT" in result["reason"]


def test_blocks_update(allowlist_file):
    result = check_sql("UPDATE sales SET amount = 0", "TestDS", allowlist_file)
    assert result["allowed"] is False


def test_blocks_delete(allowlist_file):
    result = check_sql("DELETE FROM sales", "TestDS", allowlist_file)
    assert result["allowed"] is False


def test_blocks_drop(allowlist_file):
    result = check_sql("DROP TABLE sales", "TestDS", allowlist_file)
    assert result["allowed"] is False


def test_blocks_alter(allowlist_file):
    result = check_sql("ALTER TABLE sales ADD col INT", "TestDS", allowlist_file)
    assert result["allowed"] is False


def test_blocks_truncate(allowlist_file):
    result = check_sql("TRUNCATE TABLE sales", "TestDS", allowlist_file)
    assert result["allowed"] is False


# ---------------------------------------------------------------------------
# Destructive clauses in subqueries
# ---------------------------------------------------------------------------

def test_blocks_drop_in_subquery(allowlist_file):
    sql = "SELECT * FROM (DROP TABLE sales) x"
    result = check_sql(sql, "TestDS", allowlist_file)
    assert result["allowed"] is False
    assert "DROP" in result["reason"]


def test_blocks_delete_in_cte(allowlist_file):
    sql = "WITH x AS (DELETE FROM sales) SELECT * FROM x"
    result = check_sql(sql, "TestDS", allowlist_file)
    assert result["allowed"] is False


# ---------------------------------------------------------------------------
# Comment injection
# ---------------------------------------------------------------------------

def test_blocks_line_comment(allowlist_file):
    result = check_sql("SELECT * FROM sales -- bypass", "TestDS", allowlist_file)
    assert result["allowed"] is False
    assert "comment" in result["reason"].lower()


def test_blocks_block_comment(allowlist_file):
    result = check_sql("SELECT * FROM sales /* hidden */", "TestDS", allowlist_file)
    assert result["allowed"] is False


# ---------------------------------------------------------------------------
# UNION injection
# ---------------------------------------------------------------------------

def test_blocks_union(allowlist_file):
    result = check_sql(
        "SELECT * FROM sales UNION SELECT * FROM customers",
        "TestDS", allowlist_file,
    )
    assert result["allowed"] is False
    assert "UNION" in result["reason"]


def test_blocks_union_all(allowlist_file):
    result = check_sql(
        "SELECT * FROM sales UNION ALL SELECT * FROM customers",
        "TestDS", allowlist_file,
    )
    assert result["allowed"] is False


# ---------------------------------------------------------------------------
# Table allowlist
# ---------------------------------------------------------------------------

def test_blocks_unlisted_table(allowlist_file):
    result = check_sql("SELECT * FROM secrets", "TestDS", allowlist_file)
    assert result["allowed"] is False
    assert "allowlist" in result["reason"].lower()
    assert "secrets" in result["reason"]


def test_blocks_unknown_datasource(allowlist_file):
    result = check_sql("SELECT * FROM sales", "UnknownDS", allowlist_file)
    assert result["allowed"] is False
    assert "not found" in result["reason"].lower()


def test_blocks_missing_allowlist_file():
    result = check_sql("SELECT * FROM sales", "TestDS", "/nonexistent/path.json")
    assert result["allowed"] is False


def test_cte_aliases_excluded_from_allowlist(allowlist_file):
    """CTE aliases should not be checked against the allowlist."""
    sql = (
        "WITH monthly AS (SELECT * FROM sales) "
        "SELECT * FROM monthly"
    )
    result = check_sql(sql, "TestDS", allowlist_file)
    assert result["allowed"] is True


def test_recursive_cte(allowlist_file):
    """WITH RECURSIVE should also have its CTE name excluded."""
    sql = (
        "WITH RECURSIVE tree AS (SELECT * FROM customers) "
        "SELECT * FROM tree"
    )
    result = check_sql(sql, "TestDS", allowlist_file)
    assert result["allowed"] is True


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_empty_sql(allowlist_file):
    result = check_sql("", "TestDS", allowlist_file)
    assert result["allowed"] is False


def test_whitespace_sql(allowlist_file):
    result = check_sql("   ", "TestDS", allowlist_file)
    assert result["allowed"] is False


def test_non_string_sql(allowlist_file):
    result = check_sql(None, "TestDS", allowlist_file)  # type: ignore
    assert result["allowed"] is False


def test_case_insensitive_datasource(allowlist_file):
    result = check_sql("SELECT * FROM sales", "testds", allowlist_file)
    assert result["allowed"] is True
