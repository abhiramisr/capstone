"""Execute a validated SQL query on DuckDB and return structured results."""

from __future__ import annotations

import json
import os
import time
from typing import Any

import duckdb

from src.models.schemas import ColumnInfo, ExecutionResult


def execute_sql(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    run_id: str,
    output_dir: str = "outputs/runs",
    max_preview_rows: int = 20,
) -> ExecutionResult:
    """Run *sql* and return an ``ExecutionResult`` with preview rows and saved artifacts."""
    run_dir = os.path.join(output_dir, run_id)
    os.makedirs(run_dir, exist_ok=True)

    start = time.perf_counter_ns()
    result_rel = conn.execute(sql)
    columns_info = result_rel.description  # list of (name, type_code, …)
    rows = result_rel.fetchall()
    elapsed_ms = (time.perf_counter_ns() - start) // 1_000_000

    col_names = [c[0] for c in columns_info]
    col_types = [str(c[1]) for c in columns_info]

    # Build preview
    preview: list[dict[str, Any]] = []
    for row in rows[:max_preview_rows]:
        preview.append(dict(zip(col_names, (_safe_value(v) for v in row))))

    # Save result as parquet via DuckDB COPY
    parquet_path = os.path.join(run_dir, "result.parquet")
    try:
        conn.execute(
            f"COPY ({sql}) TO '{parquet_path}' (FORMAT 'parquet')"
        )
    except Exception:
        # Fallback: save as JSON
        parquet_path = os.path.join(run_dir, "result.json")
        with open(parquet_path, "w") as f:
            all_rows = [dict(zip(col_names, (_safe_value(v) for v in row))) for row in rows]
            json.dump(all_rows, f, indent=2, default=str)

    # Save query
    with open(os.path.join(run_dir, "query.sql"), "w") as f:
        f.write(sql)

    # Save preview
    with open(os.path.join(run_dir, "result_preview.json"), "w") as f:
        json.dump(preview, f, indent=2, default=str)

    return ExecutionResult(
        row_count=len(rows),
        columns=[ColumnInfo(name=n, type=t) for n, t in zip(col_names, col_types)],
        preview_rows=preview,
        result_path=parquet_path,
        execution_ms=elapsed_ms,
    )


def _safe_value(v: Any) -> Any:
    """Convert non-JSON-serializable values to strings."""
    if v is None:
        return None
    if isinstance(v, (int, float, str, bool)):
        return v
    return str(v)
