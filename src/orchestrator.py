"""Pipeline orchestrator — drives the multi-agent text-to-SQL workflow.

Enforces the full data flow:
    User Input → Input Guardrail → Clarification Agent → Query Router Agent →
    NLQ Agent → SQL Guardrail → Execute SQL → RAG Agent → Output Guardrail → User
"""

from __future__ import annotations

import json
from uuid import uuid4

from agents import Runner

from src.agents.clarification import clarification_agent
from src.agents.query_router import query_router_agent
from src.agents.nlq_agent import nlq_agent
from src.agents.rag_agent import rag_agent
from src.connectors.azure_openai_connector import get_azure_run_config
from src.connectors.fabric_connector import (
    USE_FABRIC,
    execute_sql_fabric,
    get_fabric_connection,
    get_sql_dialect,
)
from src.guardrails.input_guardrail import check_input_safety, is_hard_block
from src.guardrails.output_guardrail import check_output_safety, has_critical_issues
from src.guardrails.sql_guardrail import check_sql
from src.models.schemas import (
    ClarificationResult,
    FinalResponse,
    RoutingResult,
    SchemaSummary,
    SQLCandidate,
)
from src.tools.redact import redact_preview
from src.tools.run_logger import RunLogger
from src.tools.schema_introspect import (
    build_fabric_schema_summary,
    get_schema_summary,
    load_csv_to_duckdb,
    load_datasource_config,
)
from src.tools.sql_execute import execute_sql
from src.tools.sql_validate import inject_limit, validate_sql

MAX_SQL_ATTEMPTS = 3


def _warn_datasource_env_mismatch(datasource_config: dict, use_fabric: bool) -> None:
    """Log when ``default_datasource`` in JSON disagrees with actual backend (USE_FABRIC)."""
    name = datasource_config.get("default_datasource")
    if not name:
        return
    by_name = {d.get("name"): d for d in datasource_config.get("datasources", []) if d.get("name")}
    target = by_name.get(name)
    if not target:
        return
    dtype = str(target.get("type", "")).lower()
    if use_fabric and dtype in ("duckdb", "csv", "local"):
        print(
            f"  [WARN] Fabric env is active (USE_FABRIC) but default_datasource "
            f"'{name}' is a local type ({dtype}). Pipeline uses the warehouse; "
            f"set default_datasource to your Fabric entry in datasource_config.json "
            f"to avoid confusion."
        )
    if not use_fabric and dtype == "fabric":
        print(
            f"  [WARN] Fabric env is not set (DuckDB/CSV path) but default_datasource "
            f"'{name}' is fabric. Set FABRIC_* / AZURE_* (or FABRIC_CONNECTION_STRING) "
            f"to use the live warehouse."
        )


def _citation_tables_for_api(
    schema: SchemaSummary, routing: RoutingResult | None
) -> list[str]:
    known = {t.name for t in schema.tables}
    if routing and routing.relevant_tables:
        picked = [t for t in routing.relevant_tables if t in known]
        if picked:
            return picked
    if schema.recommended_table and schema.recommended_table in known:
        return [schema.recommended_table]
    if schema.tables:
        return [schema.tables[0].name]
    return []


def _attach_backend_meta(
    resp: FinalResponse,
    schema: SchemaSummary,
    routing: RoutingResult | None,
    use_fabric: bool,
) -> None:
    resp.data_backend = "fabric" if use_fabric else "duckdb"
    resp.citation_tables = _citation_tables_for_api(schema, routing)


async def run_pipeline(
    question: str,
    csv_path: str,
    conversation_history: list | None = None,
) -> FinalResponse:
    """Execute the full text-to-SQL agent pipeline and return a ``FinalResponse``."""

    run_id = uuid4().hex[:12]
    logger = RunLogger(run_id)
    logger.log("setup", "run_started", {"question": question, "csv_path": csv_path})
    history_text = _format_conversation_history(conversation_history)
    print(f"\n[run_id={run_id}] Starting pipeline...")

    # Get Azure run config (None if not configured — uses default OpenAI)
    run_config = get_azure_run_config()
    if run_config:
        print("  [INFO] Using Azure AI Foundry endpoint")

    # ------------------------------------------------------------------
    # Stage 0 — Setup: get connection + introspect schema
    # ------------------------------------------------------------------
    print("[Stage 0] Loading data and introspecting schema...")
    dialect = get_sql_dialect()
    datasource_config = load_datasource_config()
    _warn_datasource_env_mismatch(datasource_config, USE_FABRIC)

    if USE_FABRIC:
        print("  [INFO] Using Microsoft Fabric Warehouse (ODBC, service principal, read-only)")
        conn = get_fabric_connection()
        schema = build_fabric_schema_summary(conn, datasource_config)
    else:
        conn = load_csv_to_duckdb(csv_path)
        schema = get_schema_summary(conn)

    logger.save_json_artifact("schema.json", schema)
    logger.log("schema", "introspected", {"tables": len(schema.tables), "notes": schema.notes})
    schema_text = _schema_prompt_preamble(dialect) + schema.format_for_prompt()

    # ------------------------------------------------------------------
    # Stage 1 — Input Guardrail (enhanced deterministic)
    # ------------------------------------------------------------------
    print("[Stage 1] Checking input safety...")
    safety_issues = check_input_safety(question)
    if safety_issues:
        logger.log("safety", "flagged", {"issues": safety_issues})
        if is_hard_block(safety_issues):
            logger.log("safety", "hard_blocked", {"issues": safety_issues})
            print(f"  [BLOCKED] Hard block triggered: {safety_issues}")
            fr = FinalResponse(
                question=question,
                answer="Your question was blocked by our safety system. "
                       "Please rephrase without instructions or commands.",
            )
            _attach_backend_meta(fr, schema, None, USE_FABRIC)
            return fr
        print(f"  [WARN] Safety flags (soft): {safety_issues}")
    else:
        logger.log("safety", "passed", {})

    # ------------------------------------------------------------------
    # Stage 2 — Clarification Agent (is the question clear?)
    # ------------------------------------------------------------------
    print("[Stage 2] Clarification Agent checking question clarity...")
    clarification_input = (
        f"{history_text}"
        f"## User Question\n{question}\n\n"
        f"## Database Schema\n{schema_text}"
    )
    clar_result = await Runner.run(
        clarification_agent, clarification_input, run_config=run_config
    )
    clar: ClarificationResult = clar_result.final_output
    logger.log("clarification", "completed", clar)

    if not clar.is_clear:
        logger.log("clarification", "needs_clarification", {
            "clarifying_question": clar.clarifying_question
        })
        print(f"  [CLARIFY] {clar.clarifying_question}")
        fr = FinalResponse(
            question=question,
            needs_clarification=True,
            answer=clar.clarifying_question,
        )
        _attach_backend_meta(fr, schema, None, USE_FABRIC)
        return fr
    print(f"  Question is clear (confidence: {clar.confidence:.2f})")

    # ------------------------------------------------------------------
    # Stage 3 — Query Router Agent (which tables?)
    # ------------------------------------------------------------------
    print("[Stage 3] Query Router Agent selecting tables...")
    router_input = (
        f"## User Question\n{question}\n\n"
        f"## Datasource Configuration\n"
        f"```json\n{json.dumps(datasource_config, indent=2)}\n```\n\n"
        f"## Available Schema\n{schema_text}"
    )
    router_result = await Runner.run(
        query_router_agent, router_input, run_config=run_config
    )
    routing: RoutingResult = router_result.final_output
    logger.log("query_router", "completed", routing)
    print(f"  Routed to tables: {routing.relevant_tables}")

    # Filter schema to only include routed tables
    filtered_schema = _filter_schema(schema, routing.relevant_tables)
    filtered_schema_text = _schema_prompt_preamble(dialect) + filtered_schema.format_for_prompt()

    # ------------------------------------------------------------------
    # Stage 4 — NLQ Agent: question → SQL (with retry loop)
    # ------------------------------------------------------------------
    validated_sql: str | None = None
    errors_feedback: str = ""

    for attempt in range(MAX_SQL_ATTEMPTS):
        print(f"[Stage 4] NLQ Agent generating SQL (attempt {attempt + 1}/{MAX_SQL_ATTEMPTS})...")

        nlq_input = (
            f"{history_text}"
            f"## User Question\n{question}\n\n"
            f"## Database Schema\n{filtered_schema_text}"
        )
        if errors_feedback:
            nlq_input += (
                f"\n\n## Previous Validation Errors — Fix These\n{errors_feedback}"
            )

        nlq_result = await Runner.run(nlq_agent, nlq_input, run_config=run_config)
        sql_candidate: SQLCandidate = nlq_result.final_output
        logger.log("nlq_agent", f"attempt_{attempt}", sql_candidate)
        print(f"  SQL: {sql_candidate.sql[:120]}...")

        # ---- Stage 5a: SQL Guardrail — regex/allowlist check ----
        print(f"[Stage 5a] SQL Guardrail allowlist check (attempt {attempt + 1})...")
        datasource_name = routing.datasource if routing else "default"
        sql_guard = check_sql(sql_candidate.sql, datasource=datasource_name)
        logger.log("sql_guardrail_check", f"attempt_{attempt}", sql_guard)

        if not sql_guard["allowed"]:
            errors_feedback = f"- {sql_guard['reason']}"
            print(f"  [BLOCKED] {sql_guard['reason']}")
            continue

        # ---- Stage 5b: SQL Guardrail — AST validation (hard gate) ----
        print(f"[Stage 5b] SQL AST validation (attempt {attempt + 1})...")
        det_val = validate_sql(
            sql_candidate.sql,
            schema,
            dialect=dialect,
            use_fabric=USE_FABRIC,
        )
        logger.log("sql_guardrail", f"attempt_{attempt}", det_val)

        if det_val.has_blockers:
            blocker_msgs = [i.message for i in det_val.issues if i.severity == "blocker"]
            errors_feedback = "\n".join(f"- {m}" for m in blocker_msgs)
            print(f"  [BLOCKED] {blocker_msgs}")
            continue

        # Inject LIMIT if needed
        final_sql = sql_candidate.sql
        if det_val.recommended_fix and "auto-injected" in det_val.recommended_fix.lower():
            final_sql = inject_limit(final_sql, dialect=dialect)

        validated_sql = final_sql
        logger.log("sql_guardrail", "passed", {"attempt": attempt})
        logger.save_artifact("query.sql", validated_sql)
        break

    if validated_sql is None:
        logger.log("pipeline", "failed", {"reason": "SQL validation failed after max attempts"})
        print("[FAILED] Could not produce a valid SQL query.")
        fr = FinalResponse(
            question=question,
            answer="I was unable to generate a valid SQL query for your question after multiple attempts. "
                   "Please try rephrasing your question.",
        )
        _attach_backend_meta(fr, schema, routing, USE_FABRIC)
        return fr

    # ------------------------------------------------------------------
    # Stage 6 — Execute SQL (Fabric Warehouse or DuckDB)
    # ------------------------------------------------------------------
    print("[Stage 6] Executing SQL...")
    try:
        if USE_FABRIC:
            exec_result = execute_sql_fabric(conn, validated_sql, run_id)
        else:
            exec_result = execute_sql(conn, validated_sql, run_id)
    except Exception as exc:
        logger.log("execution", "error", {"error": str(exc)})
        print(f"  [ERROR] Execution failed: {exc}")
        fr = FinalResponse(
            question=question,
            sql=validated_sql,
            answer=f"The SQL query was valid but execution failed: {exc}",
        )
        _attach_backend_meta(fr, schema, routing, USE_FABRIC)
        return fr

    logger.log("execution", "completed", {
        "row_count": exec_result.row_count,
        "execution_ms": exec_result.execution_ms,
    })
    print(f"  Returned {exec_result.row_count} rows in {exec_result.execution_ms}ms")

    # Redact PII from preview
    preview_rows = redact_preview(exec_result.preview_rows, schema.pii_columns)

    # ------------------------------------------------------------------
    # Stage 7 — RAG Agent: results → grounded answer
    # ------------------------------------------------------------------
    print("[Stage 7] RAG Agent generating answer...")
    rag_input = (
        f"{history_text}"
        f"## User Question\n{question}\n\n"
        f"## SQL Query\n```sql\n{validated_sql}\n```\n\n"
        f"## Execution Summary\n{exec_result.row_count} rows returned in {exec_result.execution_ms}ms\n\n"
        f"## Results Preview (first {len(preview_rows)} rows)\n"
        f"```json\n{json.dumps(preview_rows, indent=2, default=str)}\n```"
    )
    rag_result = await Runner.run(rag_agent, rag_input, run_config=run_config)
    final: FinalResponse = rag_result.final_output

    # Deterministic SQL + API metadata (DuckDB / Fabric parity)
    final.sql = validated_sql
    _attach_backend_meta(final, schema, routing, USE_FABRIC)

    # Attach structured data from execution so the API can surface
    # grounded tables/charts to the frontend.
    final.preview_rows = preview_rows
    final.columns = exec_result.columns

    # ------------------------------------------------------------------
    # Stage 8 — Output Guardrail (citation + grounding check)
    # ------------------------------------------------------------------
    print("[Stage 8] Output Guardrail checking response quality...")
    output_issues = check_output_safety(
        answer=final.answer,
        sql=final.sql,
        analysis=final.analysis,
        preview_rows=preview_rows,
    )
    if output_issues:
        logger.log("output_guardrail", "flagged", {"issues": output_issues})
        for issue in output_issues:
            print(f"  [{issue.split(':')[0]}] {issue}")
        if has_critical_issues(output_issues):
            logger.log("output_guardrail", "blocked", {"issues": output_issues})
            fr = FinalResponse(
                question=question,
                sql=validated_sql,
                answer="The generated response did not meet quality standards. "
                       "Please try rephrasing your question.",
            )
            _attach_backend_meta(fr, schema, routing, USE_FABRIC)
            return fr
    else:
        logger.log("output_guardrail", "passed", {})

    logger.log("rag_agent", "completed", final)
    logger.save_artifact("final.md", _format_final_md(final))
    logger.save_json_artifact("final.json", final)

    print(f"\n[run_id={run_id}] Pipeline completed successfully.")
    print(f"  Artifacts saved to: outputs/runs/{run_id}/")
    return final


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_conversation_history(conversation_history: list | None) -> str:
    """Format recent conversation turns into a prompt section."""
    if not conversation_history:
        return ""
    lines = ["## Conversation History (most recent exchanges)\n"]
    for i, turn in enumerate(conversation_history, 1):
        lines.append(f"### Exchange {i}")
        lines.append(f"User: {turn.question}")
        lines.append(f"Answer: {turn.answer}")
        if turn.sql:
            lines.append(f"SQL used: {turn.sql}")
        lines.append("")
    lines.append(
        "Use this conversation history to resolve references like "
        '"what about...", "for that category", "the same but...", etc. '
        "If the current question refers to entities or metrics from prior exchanges, "
        "incorporate that context.\n\n"
    )
    return "\n".join(lines)


def _schema_prompt_preamble(dialect: str) -> str:
    if dialect == "tsql":
        return (
            "## SQL dialect\n"
            "Target warehouse: **Microsoft Fabric / T-SQL**. Emit dialect-compatible SQL only: "
            "use `TOP (n)` or `OFFSET ... ROWS FETCH NEXT n ROWS ONLY` instead of `LIMIT`. "
            "Use unqualified table names exactly as listed in the schema (or `[schema].[table]` if shown).\n"
            "Date filters: if `Date` is a string column, use `TRY_CONVERT(date, Date)` or "
            "`CAST(Date AS date)` — do **not** assume `date_parsed` / `time_parsed` exist on Fabric.\n\n"
        )
    return (
        "## SQL dialect\n"
        "Target engine: **DuckDB**. Use `LIMIT` and DuckDB date/string helpers as needed.\n\n"
    )


def _filter_schema(schema: SchemaSummary, tables: list[str]) -> SchemaSummary:
    """Return a SchemaSummary containing only the specified tables.

    Falls back to the full schema when no tables match or the list is empty.
    """
    if not tables:
        return schema

    table_set = {t.lower() for t in tables}
    filtered = [t for t in schema.tables if t.name.lower() in table_set]

    if not filtered:
        return schema

    rec = schema.recommended_table
    if rec and rec.lower() not in table_set:
        rec = filtered[0].name

    return SchemaSummary(
        tables=filtered,
        recommended_table=rec,
        notes=list(schema.notes),
    )


def _format_final_md(resp: FinalResponse) -> str:
    """Format the final response as a readable Markdown document."""
    parts = [
        f"# Query Report\n",
        f"## Question\n{resp.question}\n",
        f"## Business Context\n{resp.business_context_summary}\n",
        f"## SQL\n```sql\n{resp.sql}\n```\n",
        f"## Execution\n{resp.execution_summary}\n",
        f"## Analysis\n{resp.analysis}\n",
        f"## Answer\n{resp.answer}\n",
    ]
    return "\n".join(parts)
