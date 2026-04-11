"""Microsoft Fabric Warehouse connector — replaces DuckDB for production.

Connects to Microsoft Fabric via ODBC with service principal authentication.
Enforces read-only access at the connection level.

Environment variables:
    FABRIC_CONNECTION_STRING: Full ODBC connection string (if provided, used directly)
    FABRIC_SERVER: Fabric SQL endpoint server
    FABRIC_DATABASE: Fabric database/warehouse name
    AZURE_CLIENT_ID: Service principal client ID
    AZURE_CLIENT_SECRET: Service principal client secret
    AZURE_TENANT_ID: Azure AD tenant ID
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any

from src.models.schemas import ColumnInfo, ExecutionResult


# Detect if Fabric is configured
USE_FABRIC = bool(os.getenv("FABRIC_CONNECTION_STRING") or os.getenv("FABRIC_SERVER"))


class FabricResult:
    """Wrapper to mimic DuckDB result interface from a pyodbc cursor."""

    def __init__(self, cursor: Any):
        self._cursor = cursor
        self.description = cursor.description

    def fetchall(self) -> list[tuple]:
        return self._cursor.fetchall()

    def fetchone(self) -> tuple | None:
        return self._cursor.fetchone()


class FabricConnection:
    """Connection wrapper for Microsoft Fabric Warehouse.

    Provides an interface compatible with DuckDB connections
    used elsewhere in the codebase (execute → result with .description/.fetchall).
    """

    def __init__(self, connection_string: str | None = None):
        """Initialize connection to Fabric Warehouse.

        Parameters
        ----------
        connection_string:
            ODBC connection string. If None, built from environment variables.
        """
        self._conn_string = connection_string or self._build_connection_string()
        self._conn: Any = None

    @staticmethod
    def _build_connection_string() -> str:
        """Build ODBC connection string from environment variables."""
        if conn_str := os.getenv("FABRIC_CONNECTION_STRING"):
            return conn_str

        server = os.environ["FABRIC_SERVER"]
        database = os.environ["FABRIC_DATABASE"]
        client_id = os.environ["AZURE_CLIENT_ID"]
        client_secret = os.environ["AZURE_CLIENT_SECRET"]
        tenant_id = os.environ["AZURE_TENANT_ID"]

        return (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={client_id}@{tenant_id};"
            f"PWD={client_secret};"
            f"Authentication=ActiveDirectoryServicePrincipal;"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
        )

    def _get_connection(self) -> Any:
        """Lazily establish the pyodbc connection."""
        if self._conn is None:
            try:
                import pyodbc
                self._conn = pyodbc.connect(self._conn_string, readonly=True)
            except ImportError:
                raise RuntimeError(
                    "pyodbc is required for Fabric connections. "
                    "Install with: pip install pyodbc"
                )
        return self._conn

    def execute(self, sql: str) -> FabricResult:
        """Execute a SQL query with read-only enforcement.

        Only SELECT and WITH (CTE) queries are allowed.
        """
        # Enforce read-only at the application level
        stripped = sql.strip().upper()
        if not (stripped.startswith("SELECT") or stripped.startswith("WITH")):
            raise PermissionError(
                f"Only SELECT/WITH queries allowed. Got: {stripped[:50]}..."
            )

        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        return FabricResult(cursor)

    def close(self) -> None:
        """Close the underlying connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def get_fabric_connection() -> FabricConnection | None:
    """Return a FabricConnection if Fabric env vars are configured, else None."""
    if not USE_FABRIC:
        return None
    return FabricConnection()


def execute_sql_fabric(
    conn: FabricConnection,
    sql: str,
    run_id: str,
    output_dir: str = "outputs/runs",
    max_preview_rows: int = 20,
) -> ExecutionResult:
    """Execute SQL on Fabric and return an ExecutionResult (same interface as DuckDB executor)."""
    run_dir = os.path.join(output_dir, run_id)
    os.makedirs(run_dir, exist_ok=True)

    start = time.perf_counter_ns()
    result = conn.execute(sql)
    rows = result.fetchall()
    elapsed_ms = (time.perf_counter_ns() - start) // 1_000_000

    col_names = [c[0] for c in result.description]
    col_types = [str(c[1].__name__) if hasattr(c[1], "__name__") else str(c[1]) for c in result.description]

    # Build preview
    def _safe_value(v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, (int, float, str, bool)):
            return v
        return str(v)

    preview: list[dict[str, Any]] = []
    for row in rows[:max_preview_rows]:
        preview.append(dict(zip(col_names, (_safe_value(v) for v in row))))

    # Save query
    with open(os.path.join(run_dir, "query.sql"), "w") as f:
        f.write(sql)

    # Save result as JSON (no parquet support without DuckDB)
    result_path = os.path.join(run_dir, "result.json")
    all_rows = [dict(zip(col_names, (_safe_value(v) for v in row))) for row in rows]
    with open(result_path, "w") as f:
        json.dump(all_rows, f, indent=2, default=str)

    # Save preview
    with open(os.path.join(run_dir, "result_preview.json"), "w") as f:
        json.dump(preview, f, indent=2, default=str)

    return ExecutionResult(
        row_count=len(rows),
        columns=[ColumnInfo(name=n, type=t) for n, t in zip(col_names, col_types)],
        preview_rows=preview,
        result_path=result_path,
        execution_ms=elapsed_ms,
    )
