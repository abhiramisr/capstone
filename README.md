# Text-to-SQL Agent

A multi-agent system that answers natural language questions about your data by generating, validating, and executing SQL — then producing a human-friendly analysis.

Built with the [OpenAI Agents SDK](https://openai.github.io/openai-agents-python/), [sqlglot](https://github.com/tobymao/sqlglot), and **either**:

| Backend | When it runs | Primary table(s) |
|--------|----------------|------------------|
| **DuckDB** + CSV | Fabric env vars **not** set | `retail_transactions` / `retail_transactions_typed` (in-memory) |
| **Microsoft Fabric** (ODBC) | `FABRIC_CONNECTION_STRING` **or** `FABRIC_SERVER` + `FABRIC_DATABASE` + Azure SP vars | Configured in `datasource_config.json` (e.g. `dbo.tbltransactions`) |

Both paths use the **same orchestrator** (guardrails → router → NLQ → validation → execute → RAG). SQL dialect is **DuckDB** locally and **T-SQL** against Fabric.

## How It Works

```
User Question
     │
     ▼
┌──────────────────────┐
│ Input Guardrail      │  ← Safety patterns
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Clarification Agent  │  ← Is the question clear?
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Query Router Agent   │  ← Tables from datasource_config (exact names)
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ NLQ Agent            │  ← One SQL candidate (DuckDB or T-SQL)
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ SQL Validator        │  ← sqlglot: read-only, allowlist, row cap
│ (deterministic)      │
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ SQL Executor         │  ← DuckDB or Fabric (pyodbc)
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ RAG Agent            │  ← Grounded answer from preview rows
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ Output Guardrail     │
└──────────────────────┘
```

Stages are logged to `outputs/runs/{run_id}/events.jsonl`.

## DuckDB vs Fabric (Option A — parity)

- **Same questions** map to the **same column names** on both sides for the retail-style dataset.
- **Differences:** The DuckDB view adds **`date_parsed`** / **`time_parsed`**. Fabric tables (e.g. `tbltransactions`) typically have **`Date`** as a string and **`Time`** as `DATETIME2` — the NLQ prompt and schema preamble tell the model to use **`TRY_CONVERT` / `CAST`** for date filters on Fabric, not `date_parsed`.
- **Switching backends:** Unset Fabric-related env vars to use DuckDB + CSV; set them to use the warehouse. `default_datasource` in `datasource_config.json` should match your intent; the orchestrator logs a **warning** if it disagrees with the active backend.

API responses (`bridge_server`) include **`citations`** (routed table names) and **`dataBackend`** (`duckdb` | `fabric`) when the full pipeline runs.

## Prerequisites

- Python 3.11+
- An OpenAI API key (for the full agent pipeline)
- **Fabric only:** [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server) and a **service principal** with access to the workspace (see `src/connectors/fabric_connector.py`)

## Setup

1. **Clone and install:**

```bash
cd text-to-sql-agent
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
# Optional — Microsoft Fabric / pyodbc
pip install -e ".[fabric]"
```

2. **Set your API key:**

```bash
cp .env.example .env
# Edit .env — at minimum OPENAI_API_KEY for agents
```

3. **Local data (DuckDB path):**

Default CSV is `new_retail_data 1.csv` in the project root (or under `data/sources/`). Used only when Fabric is **not** configured.

4. **Fabric (live warehouse):**

Set either `FABRIC_CONNECTION_STRING` **or** `FABRIC_SERVER` + `FABRIC_DATABASE` + `AZURE_TENANT_ID` + `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET`. List your warehouse tables under the `type: "fabric"` datasource in `datasource_config.json` and set `default_table`.

## Configuration

| File | Purpose |
|------|---------|
| `src/config/datasource_config.json` | Multiple datasources (`fabric` vs `duckdb`), allowed tables, `default_datasource`, optional `connection_env` key hints for env vars |
| `src/config/schema_config.json` | Column descriptions, PII, relationships — merged at runtime with introspected types |
| `src/config/allowlist_config.json` | Fallback allowlist if datasource config lists no tables |

## Usage

```bash
source .venv/bin/activate

# Local DuckDB + CSV — unset FABRIC_* / FABRIC_CONNECTION_STRING / AZURE_* used only for Fabric
python -m src.app --question "What are the top 5 product categories by total revenue?"

python -m src.app --question "Show monthly sales trends" --source "path/to/data.csv"
```

With Fabric env vars set, the **same command** runs against the warehouse; SQL is validated as **T-SQL** and row caps use **TOP** / **FETCH**.

### Example Questions

```bash
python -m src.app -q "What are the top 5 product categories by total revenue?"
python -m src.app -q "What is the average order value by country?"
python -m src.app -q "Show me monthly revenue trends for 2023"
```

## Running the Web UI

The project includes a FastAPI server (`bridge_server.py`) that pairs with a React frontend for an interactive experience.

### 1. Start the API server

```bash
source .venv/bin/activate
python bridge_server.py
```

The server starts at `http://127.0.0.1:8000`. It works with or without an `OPENAI_API_KEY` — without one it returns raw data previews instead of LLM analysis.

### 2. Set up the frontend

In a **separate terminal**:

```bash
git clone https://github.com/rachltan/text-to-sql-agent-frontend.git
cd text-to-sql-agent-frontend
npm install
npm start
```

The React dev server starts at `http://localhost:3000` and calls the backend API automatically.

### 3. Verify

Open `http://localhost:3000`, type a question, and confirm the answer, chart, and SQL panel all populate. Recent questions should appear in the left sidebar.

## Output

Each run writes under `outputs/runs/{run_id}/`:

| File | Description |
|------|-------------|
| `events.jsonl` | Append-only log of pipeline stages |
| `schema.json` | Schema shown to agents (introspected + merged from `schema_config.json`) |
| `query.sql` | Validated SQL (after any row-cap injection) |
| `result.parquet` | Full result set (**DuckDB** path, when `COPY` succeeds) |
| `result.json` | Full result set (**Fabric** path, or DuckDB fallback) |
| `result_preview.json` | First rows of the result |
| `final.json` / `final.md` | Structured + Markdown final response |

## Safety Guarantees

- **Read-only SQL:** AST validation blocks DML/DDL; Fabric connector allows only `SELECT` / `WITH` and uses ODBC **read intent** where applicable
- **Single statement:** Semicolons that separate statements are rejected
- **Table allowlist:** Tables must appear under the **active** backend in `datasource_config.json`
- **PII:** Sensitive columns are redacted in previews per schema / PII config
- **Row cap:** `LIMIT` (DuckDB) or **TOP** / **FETCH** (T-SQL)

## Running Tests

```bash
pytest tests/ -v
```

Use `PYTHONPATH=.` from the repo root if you run pytest without an editable install.

## Project Structure

```
src/
  app.py                 # CLI entry point
  orchestrator.py        # Pipeline (sets data_backend + citation_tables on FinalResponse)
  agents/                # Clarification, query router, NLQ, RAG, …
  connectors/            # Azure OpenAI, Fabric (ODBC)
  tools/                 # Schema, validate, execute, log, redact
  config/                # datasource_config.json, schema_config.json, …
  models/schemas.py      # Pydantic I/O contracts
  prompts/               # Agent prompt templates (Markdown)
tests/
data/sources/            # CSV files (DuckDB)
outputs/runs/            # Per-run artifacts
bridge_server.py         # FastAPI (optional); honors DuckDB vs Fabric for /ask
```

## Architecture

The orchestrator follows the [OpenAI Agents SDK](https://openai.github.io/openai-agents-python/) pattern: sequential `Runner.run` calls with structured outputs. The **deterministic SQL validator** (sqlglot) is the hard gate before execution; LLM steps do not replace that check.
