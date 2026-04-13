"""Anomaly detection runner — gathers dataset stats, invokes the anomaly agent,
and persists findings to the anomaly store.

Can be run on-demand via the ``/anomalies/run`` API endpoint or as a periodic
background task via ``start_anomaly_scheduler()``.
"""

from __future__ import annotations

import asyncio
import json
import os
from uuid import uuid4

from agents import Runner

from src.agents.anomaly import anomaly_agent
from src.anomaly_store import init_anomaly_db, save_finding
from src.connectors.azure_openai_connector import get_azure_run_config
from src.connectors.fabric_connector import USE_FABRIC, get_fabric_connection
from src.models.schemas import AnomalyReport
from src.tools.run_logger import RunLogger
from src.tools.schema_introspect import (
    build_fabric_schema_summary,
    get_schema_summary,
    load_csv_to_duckdb,
    load_datasource_config,
)

# How often the background job runs (seconds). Default: 6 hours.
_INTERVAL_SECONDS = int(os.environ.get("ANOMALY_INTERVAL_SECONDS", 6 * 3600))

# CSV path for DuckDB mode
_CSV_PATH = os.environ.get("ANOMALY_CSV_PATH", "new_retail_data 1.csv")


def _resolve_csv_path(csv_path: str) -> str:
    if not os.path.isabs(csv_path):
        if not os.path.exists(csv_path):
            alt = os.path.join("data", "sources", csv_path)
            if os.path.exists(alt):
                return alt
    return csv_path


def _build_aggregate_stats(conn, table_name: str, *, is_fabric: bool = False) -> str:
    """Run a handful of aggregate queries and return a text summary for the agent."""
    stats_lines = [f"## Aggregate Statistics for {table_name}\n"]

    # Row count
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        stats_lines.append(f"Total rows: {row[0]}")
    except Exception as exc:
        stats_lines.append(f"Row count error: {exc}")

    # Numeric column stats
    try:
        cols_result = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_name = '{table_name}' ORDER BY ordinal_position"
        ).fetchall()
    except Exception:
        cols_result = []

    numeric_types = {"double", "float", "decimal", "numeric", "int", "integer",
                     "bigint", "smallint", "tinyint", "real", "hugeint"}

    for col_name, col_type in cols_result:
        base_type = col_type.split("(")[0].lower().strip()
        if base_type in numeric_types:
            try:
                if is_fabric:
                    agg_sql = (
                        f"SELECT MIN([{col_name}]), MAX([{col_name}]), "
                        f"AVG(CAST([{col_name}] AS FLOAT)), COUNT(*) - COUNT([{col_name}]) "
                        f"FROM {table_name}"
                    )
                else:
                    agg_sql = (
                        f'SELECT MIN("{col_name}"), MAX("{col_name}"), '
                        f'AVG("{col_name}"), COUNT(*) - COUNT("{col_name}") '
                        f"FROM {table_name}"
                    )
                row = conn.execute(agg_sql).fetchone()
                stats_lines.append(
                    f"\n### {col_name} ({col_type})\n"
                    f"  Min: {row[0]}, Max: {row[1]}, Avg: {row[2]}, Nulls: {row[3]}"
                )
            except Exception as exc:
                stats_lines.append(f"\n### {col_name} ({col_type})\n  Stats error: {exc}")

    return "\n".join(stats_lines)


async def run_anomaly_detection() -> AnomalyReport | None:
    """Execute one anomaly detection cycle: introspect → aggregate → agent → store."""
    run_id = uuid4().hex[:12]
    logger = RunLogger(run_id)
    logger.log("anomaly", "run_started", {})
    print(f"\n[anomaly run_id={run_id}] Starting anomaly detection...")

    run_config = get_azure_run_config()
    datasource_config = load_datasource_config()

    if USE_FABRIC:
        conn = get_fabric_connection()
        schema = build_fabric_schema_summary(conn, datasource_config)
    else:
        csv_path = _resolve_csv_path(_CSV_PATH)
        conn = load_csv_to_duckdb(csv_path)
        schema = get_schema_summary(conn)

    if not schema.tables:
        print("  [WARN] No tables found — skipping anomaly detection")
        return None

    table_name = schema.recommended_table or schema.tables[0].name
    schema_text = schema.format_for_prompt()
    stats_text = _build_aggregate_stats(conn, table_name, is_fabric=USE_FABRIC)

    agent_input = (
        f"## Database Schema\n{schema_text}\n\n"
        f"{stats_text}"
    )

    logger.log("anomaly", "invoking_agent", {"table": table_name})
    print(f"  Invoking anomaly agent on table: {table_name}")

    try:
        result = await Runner.run(anomaly_agent, agent_input, run_config=run_config)
        report: AnomalyReport = result.final_output
    except Exception as exc:
        logger.log("anomaly", "agent_error", {"error": str(exc)})
        print(f"  [ERROR] Anomaly agent failed: {exc}")
        return None

    logger.log("anomaly", "completed", {"severity": report.severity})
    logger.save_json_artifact("anomaly_report.json", report)

    # Persist to store
    init_anomaly_db()
    save_finding(
        run_id=run_id,
        severity=report.severity,
        summary=report.summary,
        anomalies=[a.model_dump() for a in report.anomalies],
        trends=[t.model_dump() for t in report.trends],
        recommended_queries=report.recommended_queries,
    )

    print(f"  [anomaly run_id={run_id}] Completed — severity: {report.severity}, "
          f"{len(report.anomalies)} anomalies, {len(report.trends)} trends")
    return report


# ---------------------------------------------------------------------------
# Background scheduler
# ---------------------------------------------------------------------------

_scheduler_task: asyncio.Task | None = None


async def _anomaly_loop() -> None:
    """Periodically run anomaly detection."""
    while True:
        try:
            await run_anomaly_detection()
        except Exception as exc:
            print(f"  [anomaly scheduler] Error: {exc}")
        await asyncio.sleep(_INTERVAL_SECONDS)


def start_anomaly_scheduler() -> None:
    """Start the background anomaly detection loop (idempotent)."""
    global _scheduler_task
    if _scheduler_task is not None and not _scheduler_task.done():
        return
    loop = asyncio.get_event_loop()
    _scheduler_task = loop.create_task(_anomaly_loop())
    print(f"[anomaly] Background scheduler started (interval: {_INTERVAL_SECONDS}s)")


def stop_anomaly_scheduler() -> None:
    """Cancel the background anomaly task if running."""
    global _scheduler_task
    if _scheduler_task and not _scheduler_task.done():
        _scheduler_task.cancel()
        _scheduler_task = None
        print("[anomaly] Background scheduler stopped")
