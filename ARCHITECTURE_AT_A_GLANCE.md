# Text-to-SQL Agent — Architecture at a Glance

**For business stakeholders**

---

## What It Does

Users ask questions in plain English (e.g. *"What are the top 5 product categories by total revenue?"*). The system turns that into a safe, read-only database query, runs it against **DuckDB** (local CSV) or a **Microsoft Fabric** warehouse, and returns a clear answer with key numbers and a short analysis — **without exposing sensitive data** and **without allowing any changes to the data**.

A React web UI and FastAPI backend provide a chat-style experience with conversation history, auto-generated charts, and optional anomaly detection.

---

## High-Level Flow

```
User Question
     │
     ▼
┌──────────────────────┐
│ Input Guardrail      │  ← Deterministic safety patterns (injection, abuse)
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Clarification Agent  │  ← Is the question clear enough to answer?
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Query Router Agent   │  ← Which datasource + tables are relevant?
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ NLQ Agent            │  ← Generate one SQL query (DuckDB or T-SQL)
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ SQL Guardrails       │  ← Regex allowlist check + AST validation (sqlglot)
│ (deterministic)      │  ← Retry loop (up to 3 attempts) on failure
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ SQL Executor         │  ← DuckDB (in-memory) or Fabric (ODBC, read-only)
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ RAG Agent            │  ← Grounded answer from preview rows + SQL
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Output Guardrail     │  ← Answer quality, PII leakage, grounding checks
└──────────────────────┘
           ▼
      Final Answer
  (answer, SQL, chart, preview)
```

---

## Pipeline Stages (Business View)

| Stage | Name | Purpose |
|-------|------|---------|
| **0** | Setup | Start the run, create a unique run ID, connect to the data backend, introspect the database schema. |
| **1** | Input guardrail | Check the question for injection attacks, abuse, or unsafe patterns. Hard blocks halt the pipeline; soft flags are logged. |
| **2** | Clarification | An AI agent evaluates whether the question is clear enough to answer. If not, it asks the user a clarifying question instead of guessing. |
| **3** | Query routing | An AI agent selects the relevant datasource and tables from the configuration, so the SQL generator works against the right data. |
| **4** | SQL generation | The NLQ agent translates the question + schema into a single read-only SQL query (DuckDB or T-SQL dialect). |
| **5** | SQL validation | Two layers: (a) a regex/allowlist check blocks disallowed tables, DML, and dangerous patterns; (b) AST validation via **sqlglot** enforces read-only, correct syntax, and schema compliance. If validation fails, the pipeline retries SQL generation (up to 3 attempts) with the error details. A row cap (`LIMIT` / `TOP`) is auto-injected when missing. |
| **6** | Execution | The validated query runs against the data backend. Results are saved as Parquet/JSON artifacts and a preview is built. |
| **7** | RAG answer | An AI agent produces a grounded, business-friendly answer from the question, the SQL, and the result preview. |
| **8** | Output guardrail | Deterministic checks on the final answer: SQL leakage, PII patterns, and grounding heuristics. Critical issues block the response. |

---

## Dual Data Backend

The same pipeline supports two backends; the choice is driven by environment variables.

| Backend | When it activates | SQL dialect | Primary table |
|---------|-------------------|-------------|---------------|
| **DuckDB** (in-memory) | Fabric env vars **not** set | DuckDB | `retail_transactions_typed` (CSV loaded at startup) |
| **Microsoft Fabric** (ODBC) | `FABRIC_CONNECTION_STRING` or `FABRIC_SERVER` + Azure SP vars set | T-SQL | Configured in `datasource_config.json` (e.g. `dbo.tbltransactions`) |

The orchestrator, guardrails, and agents are shared across both backends. SQL dialect differences (e.g. `LIMIT` vs `TOP`, date handling) are resolved by schema preamble instructions to the LLM and dialect-aware validation.

---

## Web Application

| Component | Technology | Port | Purpose |
|-----------|------------|------|---------|
| **API server** | FastAPI (`bridge_server.py`) | 8000 | Exposes `/ask`, `/history`, `/anomalies` endpoints |
| **Frontend** | React (separate repo) | 3000 | Chat UI with conversation history sidebar, SQL panel, data table, charts |

The API server works with or without an `OPENAI_API_KEY` — without one it returns raw data previews instead of LLM-driven analysis.

### API Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/ask` | Submit a question; returns answer, SQL, chart data, preview rows |
| `GET` | `/history` | Recent chat questions for the sidebar |
| `DELETE` | `/history/{id}` | Remove a single history item |
| `GET` | `/anomalies` | Recent anomaly findings |
| `POST` | `/anomalies/run` | Trigger on-demand anomaly detection |

---

## Anomaly Detection

An optional background system periodically scans the dataset for statistical anomalies and trends:

1. Gathers aggregate statistics (min, max, avg, nulls) across numeric columns.
2. An AI agent analyzes the stats and flags anomalies, trends, and severity.
3. Findings are persisted to SQLite and surfaced through the `/anomalies` API.

Runs every 6 hours when `ANOMALY_ENABLED=true`, or on demand via the API.

---

## Key Benefits for the Business

- **Safety first** — Only read-only queries are allowed. No inserts, updates, deletes, or schema changes. Validation is rule-based and deterministic, not only AI-based.

- **Controlled data access** — Queries are restricted to allowed tables and columns. PII columns are tagged and redacted in previews shown to the user.

- **Self-correction** — If the generated SQL fails validation, the system retries with the error feedback (up to 3 attempts), improving success without manual intervention.

- **Conversation context** — Follow-up questions like *"what about for 2024?"* are resolved using conversation history, so users can drill down naturally.

- **Auditability** — Every run is logged (stages, events, artifacts) under `outputs/runs/{run_id}/`. You can trace what question was asked, what SQL was run, and what was returned.

- **Clear output** — The final answer is written for business users: executive summary, main numbers, caveats, and suggested follow-up questions.

---

## Project Structure

```
src/
  app.py                 # CLI entry point
  orchestrator.py        # Pipeline orchestration (all 9 stages)
  agents/                # AI agents (clarification, router, NLQ, RAG, anomaly)
  connectors/            # Azure OpenAI / Foundry + Fabric ODBC connectors
  guardrails/            # Input, output, and SQL guardrails
  tools/                 # Schema introspection, SQL validation, execution, PII redaction
  models/schemas.py      # Pydantic I/O contracts for all agents and tools
  prompts/               # Agent instruction templates (Markdown)
  config/                # datasource_config, schema_config, allowlist, PII config
  chat_store.py          # SQLite chat history persistence
  anomaly_store.py       # SQLite anomaly findings persistence
  anomaly_runner.py      # Anomaly detection runner + background scheduler
bridge_server.py         # FastAPI server (pairs with React frontend)
config/                  # Root-level allowlist + PII config (used by SQL guardrail)
tests/                   # pytest suite (13 test files, mocked agent calls)
docs/                    # Architecture flowchart source (Mermaid)
```

---

## End-to-End in One Sentence

**Question in (natural language) → safety check → clarification → table routing → SQL generation → validated (regex + AST) → executed (DuckDB or Fabric) → grounded answer → quality check → concise answer out (with SQL, chart, and redacted preview).**
