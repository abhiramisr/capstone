"""Microsoft Fabric Warehouse connector — replaces DuckDB for production.

This runtime uses **ODBC only** (``pyodbc``). **JDBC is not used** here; a JVM
service would use JDBC separately. Warehouse access is **Azure AD service
principal** (client id + secret + tenant), either via ``FABRIC_CONNECTION_STRING``
or ``FABRIC_SERVER`` / ``FABRIC_DATABASE`` plus ``AZURE_*`` env vars.

Two auth strategies are tried in order:

1. **MSAL token-based auth** — acquires an Azure AD access token via
   ``msal.ConfidentialClientApplication`` and passes it as a pre-auth
   attribute (``SQL_COPT_SS_ACCESS_TOKEN``).  This is more reliable across
   Fabric SKUs than the ODBC driver's built-in SP flow.
2. **ODBC built-in** ``ActiveDirectoryServicePrincipal`` — fallback if
   ``msal`` is not installed.

Read-only access:
    - **Connection string**: ``ApplicationIntent=ReadOnly`` is appended when
      missing (ODBC read intent for SQL Server–compatible endpoints).
    - **API**: ``FabricConnection.execute`` only allows ``SELECT`` / ``WITH`` (CTE).

Credentials (e.g. from Frank) belong in environment or a secret store — never
committed to the repo.

Environment variables:
    FABRIC_CONNECTION_STRING: Full ODBC connection string (Azure AD / SP auth;
        read intent applied if not already present)
    FABRIC_SERVER: Fabric SQL endpoint hostname
    FABRIC_DATABASE: Logical database / warehouse name
    FABRIC_ODBC_DRIVER: Optional; defaults to ``ODBC Driver 18 for SQL Server``
    AZURE_CLIENT_ID: Service principal (application) ID
    AZURE_CLIENT_SECRET: Service principal secret
    AZURE_TENANT_ID: Azure AD tenant ID
"""

from __future__ import annotations

import json
import os
import re
import struct
import time
from typing import Any

from src.models.schemas import ColumnInfo, ExecutionResult


def _odbc_connection_string_with_read_intent(conn_str: str) -> str:
    """Ensure ``ApplicationIntent=ReadOnly`` for ODBC (connection-level read intent)."""
    s = conn_str.strip()
    if re.search(r"\bApplicationIntent\s*=", s, re.IGNORECASE):
        return s
    if s.endswith(";"):
        return s + "ApplicationIntent=ReadOnly;"
    return s + ";ApplicationIntent=ReadOnly;"


def _service_principal_env_ok() -> bool:
    return all(
        os.getenv(k)
        for k in (
            "AZURE_TENANT_ID",
            "AZURE_CLIENT_ID",
            "AZURE_CLIENT_SECRET",
            "FABRIC_SERVER",
            "FABRIC_DATABASE",
        )
    )


# Fabric mode: full ODBC string, or server + database + service principal env vars
USE_FABRIC = bool(os.getenv("FABRIC_CONNECTION_STRING")) or (
    bool(os.getenv("FABRIC_SERVER")) and _service_principal_env_ok()
)


def get_sql_dialect() -> str:
    """SQL dialect for validation/codegen (sqlglot): ``tsql`` when Fabric is active."""
    return "tsql" if USE_FABRIC else "duckdb"


# ---- MSAL token acquisition ------------------------------------------------

_SQL_COPT_SS_ACCESS_TOKEN = 1256
_FABRIC_SQL_SCOPE = "https://database.windows.net/.default"


def _acquire_token_msal() -> str | None:
    """Acquire an Azure AD access token for the SQL endpoint via MSAL.

    Returns the raw JWT string, or *None* if msal is not installed or
    token acquisition fails.
    """
    try:
        import msal  # noqa: F811
    except ImportError:
        return None

    tenant_id = os.environ.get("AZURE_TENANT_ID", "")
    client_id = os.environ.get("AZURE_CLIENT_ID", "")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET", "")
    if not (tenant_id and client_id and client_secret):
        return None

    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret,
    )
    result = app.acquire_token_for_client(scopes=[_FABRIC_SQL_SCOPE])
    if "access_token" in result:
        return result["access_token"]

    print(
        f"  [WARN] MSAL token error: {result.get('error')}: "
        f"{result.get('error_description', '')[:200]}"
    )
    return None


def _token_to_odbc_struct(token: str) -> bytes:
    """Encode a JWT into the binary struct expected by SQL_COPT_SS_ACCESS_TOKEN."""
    token_bytes = token.encode("utf-16-le")
    return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)


# ---- FabricResult / FabricConnection ---------------------------------------


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
        self._explicit_conn_string = connection_string
        self._conn: Any = None

    @staticmethod
    def _build_connection_string(*, include_auth: bool = True) -> str:
        """Build ODBC connection string from environment variables.

        When *include_auth* is False the UID/PWD/Authentication keys are
        omitted (used when passing a pre-auth token via attrs_before).
        """
        if conn_str := os.getenv("FABRIC_CONNECTION_STRING"):
            return _odbc_connection_string_with_read_intent(conn_str)

        server = os.environ["FABRIC_SERVER"]
        database = os.environ["FABRIC_DATABASE"]
        driver = os.getenv("FABRIC_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

        base = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"ApplicationIntent=ReadOnly;"
        )

        if include_auth:
            client_id = os.environ["AZURE_CLIENT_ID"]
            client_secret = os.environ["AZURE_CLIENT_SECRET"]
            tenant_id = os.environ["AZURE_TENANT_ID"]
            base += (
                f"UID={client_id}@{tenant_id};"
                f"PWD={client_secret};"
                f"Authentication=ActiveDirectoryServicePrincipal;"
            )

        return base

    def _get_connection(self) -> Any:
        """Lazily establish the pyodbc connection.

        Strategy order:
        1. If an explicit connection string was provided, use it directly.
        2. Try MSAL token-based auth (most reliable for Fabric).
        3. Fall back to ODBC driver's built-in ActiveDirectoryServicePrincipal.
        """
        if self._conn is not None:
            return self._conn

        try:
            import pyodbc
        except ImportError:
            raise RuntimeError(
                "pyodbc is required for Fabric connections. "
                "Install with: pip install pyodbc"
            )

        last_error: Exception | None = None

        # --- Strategy 1: explicit connection string -------------------------
        if self._explicit_conn_string:
            try:
                conn_ready = _odbc_connection_string_with_read_intent(self._explicit_conn_string)
                self._conn = pyodbc.connect(conn_ready)
                print("  [INFO] Connected to Fabric using explicit connection string")
                return self._conn
            except pyodbc.Error as exc:
                last_error = exc
                print(f"  [WARN] Explicit connection string failed: {exc}")

        # --- Strategy 2: MSAL token-based auth ------------------------------
        token = _acquire_token_msal()
        if token:
            try:
                conn_str = self._build_connection_string(include_auth=False)
                token_struct = _token_to_odbc_struct(token)
                self._conn = pyodbc.connect(
                    conn_str,
                    attrs_before={_SQL_COPT_SS_ACCESS_TOKEN: token_struct},
                )
                print("  [INFO] Connected to Fabric using MSAL token auth")
                return self._conn
            except pyodbc.Error as exc:
                last_error = exc
                print(f"  [WARN] MSAL token connection failed: {exc}")

        # --- Strategy 3: ODBC built-in SP auth ------------------------------
        try:
            conn_str = self._build_connection_string(include_auth=True)
            self._conn = pyodbc.connect(conn_str)
            print("  [INFO] Connected to Fabric using ODBC SP auth")
            return self._conn
        except pyodbc.Error as exc:
            last_error = exc
            print(f"  [WARN] ODBC SP auth connection failed: {exc}")

        # --- All strategies failed ------------------------------------------
        raise RuntimeError(
            f"Could not connect to Microsoft Fabric Warehouse.\n"
            f"Last error: {last_error}\n\n"
            f"Troubleshooting checklist:\n"
            f"  1. Verify the service principal ({os.getenv('AZURE_CLIENT_ID')}) "
            f"has been granted access to the Fabric workspace.\n"
            f"  2. Ensure 'Service principals can use Fabric APIs' is enabled "
            f"in the Fabric Admin Portal.\n"
            f"  3. The service principal must be added to the workspace with "
            f"at least Viewer/Contributor role.\n"
            f"  4. Check that FABRIC_SERVER and FABRIC_DATABASE are correct.\n"
            f"  5. Verify the client secret has not expired."
        )

    def execute(
        self,
        sql: str,
        params: tuple | list | None = None,
    ) -> FabricResult:
        """Execute a SQL query with read-only enforcement.

        Only SELECT and WITH (CTE) queries are allowed.
        *params* are bound as ODBC parameters (for introspection metadata only).
        """
        stripped = sql.strip().upper()
        if not (stripped.startswith("SELECT") or stripped.startswith("WITH")):
            raise PermissionError(
                f"Only SELECT/WITH queries allowed. Got: {stripped[:50]}..."
            )

        conn = self._get_connection()
        cursor = conn.cursor()
        if params is not None:
            cursor.execute(sql, params)
        else:
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
