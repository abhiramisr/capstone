## Implementation Plan — Multi-Agent Text-to-SQL System (CSV → SQL → Analysis)

### 0) What you’re building

A **local** (CLI + optional API) system that:

- Accepts a user’s **natural language question** about one or more data sources (starting with `new_retail_data 1.csv`).
- Understands the **business context** and re-expresses the request as a **technical SQL task spec**.
- Generates an **SQL query** (DuckDB dialect), validates it with strict **read-only / anti-injection guardrails**, executes it, then writes a human-friendly **analysis/report**.
- Produces **auditable logs** for every step (inputs, outputs, decisions, SQL, row counts, errors).
- Ends with a **synthesis** response that merges everything into what the user sees.

This plan combines two proven design ideas:

- **“Hub-and-spoke orchestrator” / agent-as-tool** pattern from OpenAI’s Agents SDK cookbook example (central orchestrator invokes specialist agents/tools, can run them in parallel, and supports tracing). Source: `multi_agent_portfolio_collaboration.ipynb` ([link](https://raw.githubusercontent.com/openai/openai-cookbook/main/examples/agents_sdk/multi-agent-portfolio-collaboration/multi_agent_portfolio_collaboration.ipynb)).
- **Multi-stage “council” review** pattern from `llm-council` (generate candidates, cross-review/rank, then a chairman synthesizes). Source: `karpathy/llm-council` ([link](https://github.com/karpathy/llm-council/tree/master)).

---

### 1) Non-negotiable requirements (acceptance criteria)

- **Read-only guarantee**: executed SQL must be a *single statement* and **SELECT-only** (or `WITH ... SELECT`). No `INSERT/UPDATE/DELETE/MERGE/DROP/ALTER/CREATE`, no `COPY`, no `ATTACH/DETACH`, no `LOAD`, no `PRAGMA`, no multi-statement, no UDF creation, no file/network access via SQL.
- **Schema correctness**: columns/tables referenced by SQL must exist in the loaded dataset.
- **Prompt-injection resistance**: data values (e.g., CSV cells) must never be treated as instructions; tool usage must follow a strict allowlist.
- **Determinism & auditability**: every stage logs structured records; each run has a `run_id`.
- **Safe output**: analysis must avoid exposing unnecessary PII (emails/phones/addresses) unless user explicitly asks and policy allows.

---

### 2) Data source: `new_retail_data 1.csv`

Observed header (first row):

- `Transaction_ID, Customer_ID, Name, Email, Phone, Address, City, State, Zipcode, Country, Age, Gender, Income, Customer_Segment, Date, Year, Month, Time, Total_Purchases, Amount, Total_Amount, Product_Category, Product_Brand, Product_Type, Feedback, Shipping_Method, Payment_Method, Order_Status, Ratings, products`

Treat this as one main fact table: **`retail_transactions`**.

Key notes:

- `Date` is a string like `9/18/2023` (parse to a DATE).
- `Time` is a string like `22:03:55` (parse to a TIME).
- Many numeric fields appear as floats in the CSV; normalize in DuckDB types.
- Contains PII fields (`Name`, `Email`, `Phone`, `Address`) → apply output minimization by default.

---

### 3) Architecture overview

#### 3.1 Runtime components

- **Orchestrator** (single controller): owns the run state machine; calls other agents/tools; retries on validation failures; produces final response.
- **Specialist agents** (LLM-driven): each has a single responsibility and produces *structured JSON outputs*.
- **Deterministic tools** (Python functions): schema inspection, SQL parsing/validation, execution, logging, redaction.
- **Storage**:
  - DuckDB database file under `data/warehouse.duckdb` (or in-memory for ephemeral runs).
  - Run artifacts under `outputs/runs/{run_id}/...` (JSONL logs + query.sql + result parquet/csv + report.md).

#### 3.2 Recommended stack

- **Language**: Python 3.11+
- **SQL engine**: DuckDB
- **LLM orchestration**: OpenAI Agents SDK (agent-as-tool, parallel tool calls, tracing)
- **SQL parsing/AST validation**: `sqlglot` (DuckDB dialect)
- **API (optional)**: FastAPI

---

### 4) Agents and responsibilities

All agent outputs must be **machine-validated JSON** (use `pydantic` models) so the orchestrator can reliably route and retry.

#### 4.1 `BusinessContextAgent` (LLM)

**Input**: user question + (optional) schema summary  
**Output JSON**:

```json
{
  "business_goal": "string",
  "primary_metric": "string",
  "dimensions": ["string"],
  "time_range": {"start": "YYYY-MM-DD|null", "end": "YYYY-MM-DD|null"},
  "filters": [{"field": "string", "op": "=", "value": "string"}],
  "grain": "transaction|customer|day|month|category|... (string)",
  "pii_required": false,
  "assumptions": ["string"],
  "questions_to_clarify": ["string"]
}
```

Notes:

- `questions_to_clarify` should be non-empty only when truly required. Orchestrator should proceed with reasonable assumptions but record them.

#### 4.2 `TechnicalSpecAgent` (LLM) — “rewrite into technical SQL task”

This is your “agent 1” requirement.

**Input**: user question + business context + schema summary  
**Output JSON**:

```json
{
  "task": "string (precise)",
  "select": [{"expression": "string", "alias": "string|null"}],
  "group_by": ["string"],
  "filters": [{"expression": "string"}],
  "order_by": [{"expression": "string", "direction": "asc|desc"}],
  "limit": 100,
  "notes": ["string"],
  "avoid_pii": true
}
```

Rules:

- Must choose an explicit `limit` by default (e.g., 100–500) unless the user requests full output.
- Prefer aggregated outputs over raw rows when possible.

#### 4.3 `SQLWriterAgent` (LLM) — “write SQL but don’t run”

This is your “agent 2”.

**Input**: technical spec + schema contract (table + columns + types)  
**Output JSON**:

```json
{
  "sql": "WITH ... SELECT ...",
  "dialect": "duckdb",
  "expected_shape": {"rows": "small|medium|large", "columns": ["string"]},
  "notes": ["string"]
}
```

Hard rules for the SQL writer prompt:

- Output a **single statement only**.
- No comments that contain executable SQL.
- Must reference only allowed tables/views.

#### 4.4 `SQLGuardrailEvaluatorAgent` (LLM) — evaluate & guardrail

This is your “agent 3”.

**Input**: user question + technical spec + proposed SQL + schema summary + engine constraints  
**Output JSON**:

```json
{
  "is_safe": true,
  "is_read_only": true,
  "has_prompt_injection": false,
  "syntax_likely_ok": true,
  "schema_likely_ok": true,
  "issues": [
    {"severity": "blocker|warn", "category": "readonly|injection|syntax|schema|pii", "message": "string"}
  ],
  "recommended_fix": "string|null"
}
```

Important: this evaluator is **advisory**. The *actual* enforcement must be deterministic (next section).

#### 4.5 `SQLExecutorAgent` (tool-backed, can be an LLM agent but should be deterministic)

Executes SQL only after deterministic guardrails pass. Returns:

```json
{
  "row_count": 123,
  "columns": [{"name": "string", "type": "string"}],
  "preview_rows": [ {"col": "val"} ],
  "result_path": "outputs/runs/{run_id}/result.parquet",
  "execution_ms": 12
}
```

#### 4.6 `AnalysisAgent` (LLM)

**Input**: business context + technical spec + result preview (and optionally full result path summary stats)  
**Output**: Markdown report section:

- Executive summary (1–3 bullets)
- Key findings
- Caveats/data quality notes
- Suggested next questions

PII handling:

- Default to aggregated analysis and do not print raw `Email/Phone/Address`.
- If the user asks for PII, require explicit confirmation via policy (or refuse).

#### 4.7 `LoggingAgent` (deterministic tool + optional LLM summarizer)

Your “logs agent” requirement is best implemented as:

- **Deterministic log writer tool**: append JSONL events at every step.
- **Optional** LLM “log summary” agent: produce a short, human-readable run summary for debugging.

#### 4.8 `SynthesisAgent` (LLM)

Consumes:

- Original question
- Business context
- SQL (final)
- Execution summary (row count, preview)
- Analysis report

Produces:

- Final user-facing answer with:
  - A brief explanation
  - The SQL (shown)
  - A results preview (small)
  - The analysis narrative

---

### 5) Deterministic guardrails (must be enforced in code)

#### 5.1 SQL allowlist policy

Implement a strict validator using `sqlglot`:

- Parse with DuckDB dialect.
- Must be exactly **one** statement.
- Root must be `Select` or `With` → leading to a `Select`.
- Disallow any node types representing:
  - DDL/DML: `Create`, `Drop`, `Alter`, `Insert`, `Update`, `Delete`, `Merge`
  - Dangerous ops: `Copy`, `Attach`, `Detach`, `Pragma`, `Command`, `Load`
- Disallow `;` inside SQL (multi-statement).
- Enforce **table allowlist**: only `retail_transactions` (and safe views you create).
- Enforce **column existence**:
  - Extract column identifiers from AST.
  - Confirm each is in schema (or is an aggregate alias).
- Enforce **row limits**:
  - If query lacks `LIMIT`, inject a safe limit (e.g., 5000) unless user explicitly requests full data and policy allows.

#### 5.2 Prompt-injection defense

Input screening (deterministic + LLM advisory):

- If user prompt contains instructions like “ignore previous”, “run tool X”, “exfiltrate”, “drop table”, mark as suspicious.
- The orchestrator must **never** pass raw CSV cell values as instructions; schema profiling must treat them as data only.
- Tool access allowlist: only `schema_introspect`, `validate_sql`, `execute_sql`, `log_event`, `redact_preview`, etc.

#### 5.3 PII minimization

- Default analysis uses aggregated queries.
- If SQL selects PII columns, evaluator flags `warn/blocker` depending on policy.
- Redact preview output columns `Email`, `Phone`, `Address` unless explicitly allowed.

---

### 6) Orchestration: sequential vs parallel

Use an orchestrator agent with **parallel tool calls enabled** (mirrors Agents SDK best practice from the cookbook notebook).

#### 6.1 Execution graph (recommended)

**Stage 0 — Run setup (sequential)**

- Create `run_id`, create output directories, initialize logger.

**Stage 1 — Early screening + schema/context (parallel)**

Run these in parallel (independent):

- `InputSafetyCheck` (deterministic + optional LLM evaluator) on the user prompt.
- `SchemaIntrospectorTool` on CSV → produces `SchemaSummary`.
- `BusinessContextAgent` (can run with partial schema or wait; recommended: run with schema summary if ready, but it’s okay if it starts with prompt only).

**Stage 2 — Technical rewrite (sequential)**

- `TechnicalSpecAgent` consumes user prompt + business context + schema summary.

**Stage 3 — SQL generation (council-style optional parallelism)**

Base version (matches your requested roles):

- `SQLWriterAgent` generates one SQL.

Optional “LLM Council” enhancement (recommended for robustness):

- Run **N SQLWriter agents in parallel** (same role, different prompting styles or models).
- Run `SQLGuardrailEvaluatorAgent` to rank/cross-review.
- Orchestrator selects best candidate (or asks one more “chairman SQL” pass).

**Stage 4 — Validation (sequential, deterministic first)**

- Run deterministic `validate_sql(sql, schema)`:
  - If **blocker**: loop back to Stage 3 with the exact error list and constraints (max 2–3 retries).
- In parallel with deterministic validation (non-blocking), run `SQLGuardrailEvaluatorAgent` for advisory issues and better error messaging.

**Stage 5 — Execute (sequential)**

- Execute only after deterministic validation passes.
- Save result to `result.parquet` and produce a small preview.

**Stage 6 — Analysis + synthesis (parallel)**

Run in parallel:

- `AnalysisAgent` writes report.
- `LogSummaryAgent` (optional) produces a concise run summary.

Then sequential:

- `SynthesisAgent` merges into final response.

Rationale:

- Schema introspection and business-context interpretation are independent → parallel.
- SQL execution must strictly follow validation → sequential.
- Analysis can run as soon as data arrives; log summarization can run in parallel.

---

### 7) Concrete file/folder structure to implement

Create this project layout:

```text
text-to-sql-agent/
  IMPLEMENTATION_PLAN.md
  README.md
  pyproject.toml
  .env.example
  src/
    app.py                      # CLI entry
    api.py                      # FastAPI (optional)
    orchestrator.py             # state machine + retries + parallel calls
    agents/
      business_context.py
      technical_spec.py
      sql_writer.py
      sql_evaluator.py
      analysis.py
      synthesis.py
    tools/
      schema_introspect.py
      sql_validate.py
      sql_execute.py
      logging.py
      redact.py
    models/
      schemas.py                # Pydantic models for all JSON I/O
    prompts/
      business_context.md
      technical_spec.md
      sql_writer.md
      sql_evaluator.md
      analysis.md
      synthesis.md
  data/
    sources/
      new_retail_data 1.csv
    warehouse.duckdb
  outputs/
    runs/
      {run_id}/
        events.jsonl
        schema.json
        business_context.json
        technical_spec.json
        query.sql
        validation.json
        result.parquet
        result_preview.json
        analysis.md
        final.md
```

---

### 8) DuckDB ingestion & schema contract

#### 8.1 One-time (or per-run) ingestion

- Load CSV into DuckDB table `retail_transactions`.
- Create a view `retail_transactions_typed` that parses date/time:
  - `date_parsed = strptime(Date, '%m/%d/%Y')::DATE`
  - `time_parsed = strptime(Time, '%H:%M:%S')::TIME`

#### 8.2 Schema summary format

`SchemaIntrospectorTool` outputs:

```json
{
  "tables": [
    {
      "name": "retail_transactions_typed",
      "description": "Retail transactions from CSV",
      "columns": [
        {"name": "transaction_id", "type": "DOUBLE", "pii": false},
        {"name": "email", "type": "VARCHAR", "pii": true}
      ],
      "primary_key_candidates": ["transaction_id"]
    }
  ],
  "recommended_table": "retail_transactions_typed",
  "notes": ["string"]
}
```

Implementation detail: keep original column names but also expose **normalized aliases** (lowercase) so LLMs are less error-prone.

---

### 9) Orchestrator logic (pseudo-code)

```python
def run(user_question: str, data_sources: list[str]) -> FinalResponse:
    run_id = new_run_id()
    log(event="run_started", user_question=user_question, data_sources=data_sources)

    # Stage 1 (parallel)
    safety, schema, business = parallel(
        input_safety_check(user_question),
        schema_introspect(data_sources),
        business_context_agent(user_question)  # or wait for schema if desired
    )
    log_many(...)

    tech_spec = technical_spec_agent(user_question, business, schema)
    log(event="tech_spec_created", tech_spec=tech_spec)

    for attempt in range(MAX_SQL_ATTEMPTS):
        sql_candidate = sql_writer_agent(tech_spec, schema)
        log(event="sql_drafted", sql=sql_candidate.sql)

        det_validation = validate_sql_deterministic(sql_candidate.sql, schema)
        llm_eval = sql_guardrail_evaluator_agent(user_question, tech_spec, sql_candidate.sql, schema)
        log(event="sql_validated", det_validation=det_validation, llm_eval=llm_eval)

        if det_validation.blockers:
            tech_spec = patch_tech_spec_with_errors(tech_spec, det_validation)
            continue

        exec_result = execute_sql(sql_candidate.sql, run_id)
        log(event="sql_executed", exec_result=exec_result)
        break
    else:
        return friendly_failure_response(...)

    analysis = analysis_agent(user_question, business, tech_spec, exec_result)
    final = synthesis_agent(user_question, business, tech_spec, sql_candidate.sql, exec_result, analysis)
    log(event="final_generated")
    write_artifacts(...)
    return final
```

---

### 10) Prompting: essential instructions per agent

Store prompts in `src/prompts/*.md` and load them at runtime so they’re easy to tune.

#### 10.1 Cross-cutting prompt rules

- Always output **valid JSON** matching the Pydantic model.
- Never claim to have executed SQL (only executor can).
- Prefer aggregate results; default `LIMIT`.
- Do not include PII in analysis unless explicitly requested and allowed.

#### 10.2 SQL writer prompt highlights

- “You must output ONE DuckDB SQL statement.”
- “Only use table `retail_transactions_typed`.”
- “Use explicit column names from the provided schema.”
- “Add `LIMIT` unless an aggregate query already yields small output.”

---

### 11) Logging & tracing

#### 11.1 JSONL event log

Write `outputs/runs/{run_id}/events.jsonl`, one JSON per line:

```json
{"ts":"...","run_id":"...","stage":"schema","event":"schema_introspected","payload":{...}}
```

Include:

- inputs (user question, sanitized)
- agent outputs
- SQL strings (exact)
- validation decisions (blockers/warnings)
- execution metadata (row counts, timings)
- final response

#### 11.2 Tracing (optional but recommended)

If using Agents SDK tracing, record a trace per run and store the trace id in logs (mirrors the cookbook example’s trace-centric observability).

---

### 12) Testing plan (must ship with MVP)

Add tests that run locally without network:

- **Guardrail tests**:
  - Reject `DROP TABLE ...`
  - Reject `SELECT 1; DROP ...` (multi-statement)
  - Reject `COPY (SELECT ...) TO 'file.csv'`
  - Reject `PRAGMA ...`
- **Schema tests**:
  - Reject unknown column references
  - Accept known columns and aggregates
- **Execution tests**:
  - Known query returns non-empty result
- **PII redaction tests**:
  - Preview redacts `Email/Phone/Address` by default

---

### 13) Build steps an implementation agent should follow (in order)

1. Scaffold Python project with `pyproject.toml` and dependencies:
   - `duckdb`, `pandas` (optional), `sqlglot`, `pydantic`, `fastapi` (optional), `uvicorn` (optional), `openai` + Agents SDK packages (per current docs), `python-dotenv`, `pytest`.
2. Implement deterministic tools first:
   - `schema_introspect.py` → create DuckDB table/view + schema JSON
   - `sql_validate.py` → `sqlglot` AST allowlist + schema checks + limit injection
   - `sql_execute.py` → run query safely, write parquet + preview
   - `logging.py` → JSONL append + artifact writer
   - `redact.py` → preview redaction rules
3. Implement Pydantic models in `models/schemas.py`.
4. Implement prompts and agents with strict JSON output.
5. Implement orchestrator loop with retries and parallel steps.
6. Add CLI `src/app.py`:
   - `python -m src.app --question "..." --source "data/sources/new_retail_data 1.csv"`
7. (Optional) Add FastAPI `src/api.py` endpoint `/query` returning final response + run_id.
8. Add tests and run them.

---

### 14) MVP default SQL conventions

- Default table: `retail_transactions_typed`
- Date handling:
  - Use `date_parsed` for filtering by time range.
  - Use `date_trunc('month', date_parsed)` for month aggregation.
- Amount handling:
  - Use `Total_Amount` for transaction-level totals.
- Common user questions:
  - “Top categories by revenue” → `SUM(total_amount)` grouped by `product_category`.
  - “Monthly trend” → grouped by `date_trunc('month', date_parsed)`.

---

### 15) Optional “Council mode” (recommended enhancement)

Borrowing from `llm-council`:

- Run 2–4 SQL writers in parallel:
  - “strict aggregator” prompt
  - “time-series specialist” prompt
  - “segmentation specialist” prompt
- Run evaluator to rank candidates on:
  - schema correctness
  - safety/read-only
  - closeness to technical spec
  - simplicity/performance
- Chairman step (can be the orchestrator or a dedicated agent) chooses the final SQL to validate/execute.

This improves reliability when the user question is ambiguous.

---

### 16) Deliverables checklist

- [ ] Working CLI that answers at least 5 representative questions over the CSV
- [ ] Deterministic SQL guardrails + tests
- [ ] Structured logs and saved artifacts per run
- [ ] Clear README with setup/run instructions
- [ ] Optional API server

