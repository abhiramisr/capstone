"""SQLite persistence for anomaly detection findings."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).resolve().parent.parent / "chat_history.db"


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_anomaly_db() -> None:
    """Create the anomaly_findings table if it does not exist."""
    conn = _get_conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS anomaly_findings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'info',
                summary TEXT NOT NULL DEFAULT '',
                anomalies TEXT NOT NULL DEFAULT '[]',
                trends TEXT NOT NULL DEFAULT '[]',
                recommended_queries TEXT NOT NULL DEFAULT '[]',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def save_finding(
    run_id: str,
    severity: str,
    summary: str,
    anomalies: list[dict[str, Any]],
    trends: list[dict[str, Any]],
    recommended_queries: list[str],
) -> int:
    """Persist an anomaly report and return its id."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            """
            INSERT INTO anomaly_findings
                (run_id, severity, summary, anomalies, trends, recommended_queries)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                severity,
                summary,
                json.dumps(anomalies, default=str),
                json.dumps(trends, default=str),
                json.dumps(recommended_queries),
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_recent_findings(limit: int = 10) -> list[dict[str, Any]]:
    """Return the most recent anomaly findings, newest first."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            """
            SELECT id, run_id, severity, summary, anomalies, trends,
                   recommended_queries, created_at
            FROM anomaly_findings
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
        results = []
        for r in rows:
            d = dict(r)
            d["anomalies"] = json.loads(d["anomalies"])
            d["trends"] = json.loads(d["trends"])
            d["recommended_queries"] = json.loads(d["recommended_queries"])
            results.append(d)
        return results
    finally:
        conn.close()
