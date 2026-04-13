"""Schema loading: DuckDB from CSV (local dev), or dynamic introspection from Fabric.

Fabric mode queries ``INFORMATION_SCHEMA.COLUMNS`` / ``TABLES`` for the configured
``sql_schema`` — any tables/columns the warehouse exposes there are supported; the
logical database name (e.g. OpenDataBay) is only a label in ``datasource_config.json``.

Runtime metadata is merged with ``schema_config.json`` (column descriptions, PII,
relationships). Allowed tables for validation come from ``datasource_config.json``
(unless that file lists no tables — then allowlist_config / defaults apply).
"""

from __future__ import annotations

import json
import os
from typing import Any

import duckdb

from src.models.schemas import (
    SchemaColumn,
    SchemaRelationship,
    SchemaSummary,
    TableSchema,
)

_CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "config")


def _load_pii_columns() -> set[str]:
    try:
        path = os.path.join(_CONFIG_DIR, "pii_config.json")
        with open(path, "r") as f:
            config = json.load(f)
        # Support both new schema (pii_keywords) and legacy (pii_columns)
        keywords = config.get("pii_keywords", config.get("pii_columns", []))
        return {c.lower() for c in keywords}
    except (FileNotFoundError, json.JSONDecodeError):
        return {"name", "email", "phone", "address"}


def _allowed_tables_fallback() -> set[str]:
    try:
        path = os.path.join(_CONFIG_DIR, "allowlist_config.json")
        with open(path, "r") as f:
            config = json.load(f)
        # Support new datasource-aware schema
        if "datasources" in config:
            tables: set[str] = set()
            for ds in config["datasources"]:
                tables.update(ds.get("allowed_tables", []))
            if tables:
                return tables
        # Legacy flat schema
        got = set(config.get("allowed_tables", []))
        if got:
            return got
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return {"retail_transactions", "retail_transactions_typed"}


def _table_names_from_datasource_config(cfg: dict) -> set[str]:
    names: set[str] = set()
    for ds in cfg.get("datasources", []):
        for tbl in ds.get("tables", []):
            if n := tbl.get("name"):
                names.add(str(n))
    return names


def get_allowlisted_tables(*, use_fabric: bool = False) -> set[str]:
    """Table names allowed for SQL validation for the active backend."""
    try:
        cfg = load_datasource_config()
    except Exception:
        cfg = {}
    types = ("fabric",) if use_fabric else ("duckdb", "csv", "local")
    names: set[str] = set()
    for ds in cfg.get("datasources", []):
        if ds.get("type") not in types:
            continue
        for tbl in ds.get("tables", []):
            if n := tbl.get("name"):
                names.add(str(n))
    if names:
        return names
    return _allowed_tables_fallback()


# Populated at import for backward compatibility; prefer get_allowlisted_tables(use_fabric=...)
ALLOWED_TABLES: set[str] = get_allowlisted_tables(use_fabric=False)


def refresh_allowlist_cache() -> None:
    """Refresh module-level ALLOWED_TABLES (local/DuckDB datasource) after config edits."""
    global ALLOWED_TABLES
    ALLOWED_TABLES = get_allowlisted_tables(use_fabric=False)


_PII_COLUMNS = _load_pii_columns()


def load_csv_to_duckdb(csv_path: str) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection and load the CSV as a table.

    Creates:
      - ``retail_transactions`` — raw import via ``read_csv_auto``
      - ``retail_transactions_typed`` — view with parsed date/time columns
    """
    conn = duckdb.connect(database=":memory:")

    conn.execute(
        f"""
        CREATE TABLE retail_transactions AS
        SELECT * FROM read_csv_auto('{csv_path}', header=true, ignore_errors=true)
        """
    )

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


def load_schema_config() -> dict:
    try:
        path = os.path.join(_CONFIG_DIR, "schema_config.json")
        with open(path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def load_datasource_config() -> dict:
    try:
        path = os.path.join(_CONFIG_DIR, "datasource_config.json")
        with open(path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"datasources": []}


def get_active_datasource(datasource_config: dict, *, use_fabric: bool) -> dict | None:
    """Pick the datasource entry used for routing metadata and Fabric introspection."""
    sources = datasource_config.get("datasources") or []
    default_name = datasource_config.get("default_datasource")

    if use_fabric:
        fabric_ds = [d for d in sources if d.get("type") == "fabric"]
        if default_name:
            for d in fabric_ds:
                if d.get("name") == default_name:
                    return d
        return fabric_ds[0] if fabric_ds else None

    for typ in ("duckdb", "csv", "local"):
        candidates = [d for d in sources if d.get("type") == typ]
        if default_name:
            for d in candidates:
                if d.get("name") == default_name:
                    return d
        if candidates:
            return candidates[0]
    return sources[0] if sources else None


def _sql_type_from_information_schema(
    data_type: str | None,
    char_max: Any,
    num_prec: Any,
    num_scale: Any,
) -> str:
    dt = (data_type or "UNKNOWN").upper()
    if dt in ("NVARCHAR", "NCHAR", "VARCHAR", "CHAR") and char_max is not None and char_max != -1:
        return f"{dt}({int(char_max)})"
    if dt in ("DECIMAL", "NUMERIC") and num_prec is not None:
        sc = int(num_scale) if num_scale is not None else 0
        return f"{dt}({int(num_prec)},{sc})"
    return dt


def _relationships_from_schema_entry(entry: dict) -> list[SchemaRelationship]:
    out: list[SchemaRelationship] = []
    for r in entry.get("relationships", []):
        out.append(
            SchemaRelationship(
                name=r.get("name", ""),
                to_table=r["to_table"],
                from_columns=list(r.get("from_columns", [])),
                to_columns=list(r.get("to_columns", [])),
                cardinality=r.get("cardinality", r.get("type", "many_to_one")),
            )
        )
    return out


def merge_schema_config_overlay(summary: SchemaSummary, schema_cfg: dict) -> SchemaSummary:
    """Enrich introspected tables with descriptions, PII flags, PK hints, relationships."""
    by_table = {e["table"]: e for e in schema_cfg.get("schemas", [])}
    new_tables: list[TableSchema] = []

    for tbl in summary.tables:
        entry = by_table.get(tbl.name)
        if not entry:
            new_tables.append(tbl)
            continue

        desc = entry.get("description") or tbl.description
        pk = entry.get("primary_key_candidates") or tbl.primary_key_candidates
        rels = _relationships_from_schema_entry(entry) or tbl.relationships
        col_meta = {str(c["name"]).lower(): c for c in entry.get("columns", [])}
        new_cols: list[SchemaColumn] = []
        for c in tbl.columns:
            o = col_meta.get(c.name.lower())
            if o:
                new_cols.append(
                    SchemaColumn(
                        name=c.name,
                        type=c.type,
                        pii=o.get("pii", c.pii),
                        description=o.get("description", "") or c.description,
                    )
                )
            else:
                new_cols.append(c)
        new_tables.append(
            TableSchema(
                name=tbl.name,
                description=desc,
                columns=new_cols,
                primary_key_candidates=list(pk) if pk else [],
                relationships=rels,
            )
        )

    return SchemaSummary(
        tables=new_tables,
        recommended_table=summary.recommended_table,
        notes=list(summary.notes),
    )


def schema_summary_from_config_filtered(
    schema_cfg: dict,
    table_names: set[str],
    *,
    recommended_table: str = "",
) -> SchemaSummary:
    """Load only schema entries whose table name is in *table_names* (Fabric offline guardrail)."""
    tables: list[TableSchema] = []
    for entry in schema_cfg.get("schemas", []):
        if entry["table"] not in table_names:
            continue
        columns = [
            SchemaColumn(
                name=c["name"],
                type=c.get("type", "VARCHAR"),
                pii=c.get("pii", False),
                description=c.get("description", ""),
            )
            for c in entry.get("columns", [])
        ]
        tables.append(
            TableSchema(
                name=entry["table"],
                description=entry.get("description", ""),
                columns=columns,
                primary_key_candidates=list(entry.get("primary_key_candidates", [])),
                relationships=_relationships_from_schema_entry(entry),
            )
        )
    rec = recommended_table or (tables[0].name if tables else "")
    return SchemaSummary(
        tables=tables,
        recommended_table=rec,
        notes=["Schema subset loaded from schema_config.json (no live introspection)"],
    )


def schema_summary_from_config_only(schema_cfg: dict) -> SchemaSummary:
    """Build SchemaSummary from ``schema_config.json`` when the warehouse is unreachable."""
    tables: list[TableSchema] = []
    for entry in schema_cfg.get("schemas", []):
        columns = [
            SchemaColumn(
                name=c["name"],
                type=c.get("type", "VARCHAR"),
                pii=c.get("pii", False),
                description=c.get("description", ""),
            )
            for c in entry.get("columns", [])
        ]
        tables.append(
            TableSchema(
                name=entry["table"],
                description=entry.get("description", ""),
                columns=columns,
                primary_key_candidates=list(entry.get("primary_key_candidates", [])),
                relationships=_relationships_from_schema_entry(entry),
            )
        )
    return SchemaSummary(
        tables=tables,
        recommended_table=tables[0].name if tables else "",
        notes=["Schema loaded from schema_config.json (offline / config-only)"],
    )


def _discover_tables_from_fabric(conn: Any, sql_schema: str) -> list[dict]:
    """Auto-discover all tables/views in the given schema from INFORMATION_SCHEMA.

    Returns a list of dicts like ``[{"name": "MyTable", "description": "BASE TABLE"}, ...]``.
    """
    discovery_sql = """
        SELECT TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ?
        ORDER BY TABLE_NAME
    """
    result = conn.execute(discovery_sql, params=[sql_schema])
    rows = result.fetchall()
    return [
        {"name": str(row[0]), "description": f"Fabric {str(row[1]).lower()}"}
        for row in rows
    ]


def _is_placeholder_table_list(table_metas: list[dict]) -> bool:
    """Detect if the configured table list only contains placeholders."""
    if not table_metas:
        return True
    placeholder_names = {"replacewithfabrictable", "placeholder", ""}
    return all(
        str(tm.get("name", "")).lower().strip() in placeholder_names
        for tm in table_metas
    )


def introspect_fabric_schema(conn: Any, datasource_entry: dict) -> SchemaSummary:
    """Query INFORMATION_SCHEMA for each table listed under the Fabric datasource.

    If only placeholder tables are configured, auto-discovers all tables in the
    schema from ``INFORMATION_SCHEMA.TABLES`` first.
    """
    sql_schema = str(datasource_entry.get("sql_schema") or datasource_entry.get("schema") or "dbo")
    table_metas = datasource_entry.get("tables") or []

    # Auto-discover tables if the config only has placeholders
    if _is_placeholder_table_list(table_metas):
        print(f"  [INFO] Auto-discovering tables in schema [{sql_schema}]...")
        try:
            discovered = _discover_tables_from_fabric(conn, sql_schema)
            if discovered:
                print(f"  [INFO] Discovered {len(discovered)} tables: "
                      f"{[t['name'] for t in discovered]}")
                table_metas = discovered
            else:
                return SchemaSummary(
                    notes=[f"No tables found in schema [{sql_schema}]. "
                           f"Check the sql_schema setting in datasource_config.json."],
                )
        except Exception as exc:
            return SchemaSummary(
                notes=[f"Table discovery failed for schema [{sql_schema}]: {exc}"],
            )

    if not table_metas:
        return SchemaSummary(notes=["No tables configured for Fabric datasource"])

    tables: list[TableSchema] = []
    notes: list[str] = []

    meta_sql = """
        SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION,
               CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """

    for tm in table_metas:
        tname = str(tm["name"])
        tdesc = str(tm.get("description", ""))
        result = conn.execute(
            meta_sql,
            params=[sql_schema, tname],
        )
        rows = result.fetchall()
        if not rows:
            notes.append(f"No columns found for [{sql_schema}].[{tname}] (check name/schema)")
            continue

        columns: list[SchemaColumn] = []
        for row in rows:
            col_name = row[0]
            data_type = row[1]
            char_max = row[3]
            num_prec = row[4]
            num_scale = row[5]
            sql_t = _sql_type_from_information_schema(data_type, char_max, num_prec, num_scale)
            columns.append(
                SchemaColumn(
                    name=str(col_name),
                    type=sql_t,
                    pii=str(col_name).lower() in _PII_COLUMNS,
                )
            )

        tables.append(
            TableSchema(
                name=tname,
                description=tdesc,
                columns=columns,
            )
        )

    if notes:
        notes.insert(0, "Fabric INFORMATION_SCHEMA introspection")

    return SchemaSummary(
        tables=tables,
        recommended_table="",
        notes=notes or [f"Fabric schema introspection complete ({len(tables)} tables)"],
    )


def build_fabric_schema_summary(conn: Any, datasource_config: dict) -> SchemaSummary:
    """Introspect Fabric + merge ``schema_config.json``; fall back to config-only if needed."""
    schema_cfg = load_schema_config()
    ds = get_active_datasource(datasource_config, use_fabric=True)

    if ds is None:
        return SchemaSummary(
            tables=[],
            recommended_table="",
            notes=[
                "No datasource with type=fabric in datasource_config.json — add one to enable routing metadata"
            ],
        )

    summary = introspect_fabric_schema(conn, ds)
    summary.recommended_table = str(
        ds.get("default_table") or (summary.tables[0].name if summary.tables else "")
    )

    # If introspection returned placeholder results, try table names from config
    fabric_tables = {str(t["name"]) for t in ds.get("tables", []) if t.get("name")}
    if not summary.tables and fabric_tables and schema_cfg.get("schemas"):
        filtered = schema_summary_from_config_filtered(
            schema_cfg,
            fabric_tables,
            recommended_table=str(ds.get("default_table", "")),
        )
        if filtered.tables:
            summary = filtered

    if summary.tables and schema_cfg.get("schemas"):
        summary = merge_schema_config_overlay(summary, schema_cfg)

    return summary


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

    summary = SchemaSummary(
        tables=[table],
        recommended_table="retail_transactions_typed",
        notes=[f"Total rows: {row_count}"],
    )

    schema_cfg = load_schema_config()
    if schema_cfg.get("schemas"):
        summary = merge_schema_config_overlay(summary, schema_cfg)
    return summary
