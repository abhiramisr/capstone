"""Pydantic models for all agent and tool I/O contracts."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Schema introspection models
# ---------------------------------------------------------------------------

class SchemaColumn(BaseModel):
    name: str
    type: str
    pii: bool = False
    description: str = ""


class SchemaRelationship(BaseModel):
    """Logical FK-style edge loaded from schema_config / introspection metadata."""

    name: str = ""
    to_table: str
    from_columns: list[str] = Field(default_factory=list)
    to_columns: list[str] = Field(default_factory=list)
    cardinality: str = "many_to_one"


class TableSchema(BaseModel):
    name: str
    description: str = ""
    columns: list[SchemaColumn] = Field(default_factory=list)
    primary_key_candidates: list[str] = Field(default_factory=list)
    relationships: list[SchemaRelationship] = Field(default_factory=list)


class SchemaSummary(BaseModel):
    tables: list[TableSchema] = Field(default_factory=list)
    recommended_table: str = "retail_transactions_typed"
    notes: list[str] = Field(default_factory=list)

    @property
    def pii_columns(self) -> list[str]:
        cols: list[str] = []
        for t in self.tables:
            for c in t.columns:
                if c.pii:
                    cols.append(c.name)
        return cols

    @property
    def column_names(self) -> set[str]:
        names: set[str] = set()
        for t in self.tables:
            for c in t.columns:
                names.add(c.name.lower())
        return names

    def format_for_prompt(self) -> str:
        """Return a concise text representation for LLM prompts."""
        lines = [f"Recommended table: {self.recommended_table}\n"]
        for t in self.tables:
            lines.append(f"Table: {t.name}")
            lines.append(f"  Description: {t.description}")
            if t.relationships:
                lines.append("  Relationships:")
                for r in t.relationships:
                    fc = ", ".join(r.from_columns)
                    tc = ", ".join(r.to_columns)
                    lines.append(
                        f"    - → {r.to_table} ({fc}) → ({tc})"
                        + (f" [{r.cardinality}]" if r.cardinality else "")
                    )
            lines.append("  Columns:")
            for c in t.columns:
                pii_tag = " [PII]" if c.pii else ""
                desc = f" — {c.description}" if c.description else ""
                lines.append(f"    - {c.name} ({c.type}){pii_tag}{desc}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Agent I/O models
# ---------------------------------------------------------------------------

class TimeRange(BaseModel):
    start: str | None = None
    end: str | None = None


class FilterSpec(BaseModel):
    field: str
    op: str = "="
    value: str


class BusinessContext(BaseModel):
    business_goal: str
    primary_metric: str
    dimensions: list[str] = Field(default_factory=list)
    time_range: TimeRange = Field(default_factory=TimeRange)
    filters: list[FilterSpec] = Field(default_factory=list)
    grain: str = "transaction"
    pii_required: bool = False
    assumptions: list[str] = Field(default_factory=list)


class SelectExpression(BaseModel):
    expression: str
    alias: str | None = None


class OrderBySpec(BaseModel):
    expression: str
    direction: str = "asc"


class TechnicalSpec(BaseModel):
    task: str
    select_expressions: list[SelectExpression] = Field(default_factory=list)
    group_by: list[str] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)
    order_by: list[OrderBySpec] = Field(default_factory=list)
    limit: int = 100
    notes: list[str] = Field(default_factory=list)
    avoid_pii: bool = True


class SQLCandidate(BaseModel):
    sql: str
    dialect: str = "duckdb"  # duckdb | tsql (Fabric / SQL Server)
    expected_columns: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class ValidationIssue(BaseModel):
    severity: str = "blocker"  # blocker | warn
    category: str = ""  # readonly | injection | syntax | schema | pii
    message: str = ""


class ValidationResult(BaseModel):
    is_safe: bool = True
    is_read_only: bool = True
    has_prompt_injection: bool = False
    syntax_ok: bool = True
    schema_ok: bool = True
    issues: list[ValidationIssue] = Field(default_factory=list)
    recommended_fix: str | None = None

    @property
    def has_blockers(self) -> bool:
        return any(i.severity == "blocker" for i in self.issues)


class ColumnInfo(BaseModel):
    name: str
    type: str


class ExecutionResult(BaseModel):
    row_count: int = 0
    columns: list[ColumnInfo] = Field(default_factory=list)
    preview_rows: list[dict[str, Any]] = Field(default_factory=list)
    result_path: str = ""
    execution_ms: int = 0


class AnalysisReport(BaseModel):
    executive_summary: list[str] = Field(default_factory=list)
    key_findings: list[str] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)
    suggested_next_questions: list[str] = Field(default_factory=list)


class ClarificationResult(BaseModel):
    """Output of the Clarification Agent."""
    is_clear: bool = True
    clarifying_question: str = ""
    original_question: str = ""
    interpreted_intent: str = ""
    confidence: float = 1.0


class RoutingResult(BaseModel):
    """Output of the Query Router Agent."""
    relevant_tables: list[str] = Field(default_factory=list)
    datasource: str = "default"
    reasoning: str = ""
    schema_subset: str = ""


class AnomalyItem(BaseModel):
    """A single detected anomaly."""
    metric: str
    expected_range: str = ""
    actual_value: str = ""
    deviation_pct: float = 0.0
    description: str = ""


class TrendItem(BaseModel):
    """A single detected trend."""
    metric: str
    direction: str = ""
    period: str = ""
    description: str = ""


class AnomalyReport(BaseModel):
    """Output of the Anomaly/Insight Agent."""
    anomalies: list[AnomalyItem] = Field(default_factory=list)
    trends: list[TrendItem] = Field(default_factory=list)
    summary: str = ""
    severity: str = "info"  # info | warning | critical
    recommended_queries: list[str] = Field(default_factory=list)


class FinalResponse(BaseModel):
    question: str
    business_context_summary: str = ""
    sql: str = ""
    execution_summary: str = ""
    analysis: str = ""
    answer: str = ""
    needs_clarification: bool = False
    preview_rows: list[dict[str, Any]] = Field(default_factory=list)
    columns: list[ColumnInfo] = Field(default_factory=list)
    # Populated by orchestrator for API / UI (DuckDB vs Fabric parity)
    data_backend: str = ""  # "duckdb" | "fabric"
    citation_tables: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

class LogEvent(BaseModel):
    ts: str = ""
    run_id: str = ""
    stage: str = ""
    event: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)

    def __init__(self, **data: Any):
        super().__init__(**data)
        if not self.ts:
            self.ts = datetime.utcnow().isoformat()
