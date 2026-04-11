# Text-to-SQL Agent

A multi-agent system that answers natural language questions about your data by generating, validating, and executing SQL queries — then producing a human-friendly analysis.

Built with [OpenAI Agents SDK](https://openai.github.io/openai-agents-python/), [DuckDB](https://duckdb.org/), and [sqlglot](https://github.com/tobymao/sqlglot).

## How It Works

```
User Question
     │
     ▼
┌─────────────────────┐
│ BusinessContextAgent │  ← Understands the business intent
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ TechnicalSpecAgent   │  ← Rewrites question as a precise SQL task
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ SQLWriterAgent       │  ← Generates DuckDB SQL (does NOT execute)
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ Deterministic        │  ← sqlglot AST: read-only, single statement,
│ SQL Validator        │    table/column allowlist, LIMIT injection
├─────────────────────┤
│ SQLGuardrailAgent    │  ← LLM advisory: injection, alignment, PII
└──────────┬──────────┘
           ▼  (retry up to 3x on validation failure)
┌─────────────────────┐
│ SQL Executor         │  ← Runs query on DuckDB, saves results
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ AnalysisAgent        │  ← Writes analytical report from results
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ SynthesisAgent       │  ← Merges everything into final answer
└─────────────────────┘
```

Every step is logged to `outputs/runs/{run_id}/events.jsonl` for full auditability.

## Prerequisites

- Python 3.11+
- An OpenAI API key

## Setup

1. **Clone and install:**

```bash
cd text-to-sql-agent
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

2. **Set your API key:**

```bash
cp .env.example .env
# Edit .env and add your OPENAI_API_KEY
```

3. **Place your data:**

The default data source is `new_retail_data 1.csv` in the project root. It's automatically symlinked into `data/sources/`.

## Usage

```bash
# Activate virtualenv
source .venv/bin/activate

# Ask a question
python -m src.app --question "What are the top 5 product categories by total revenue?"

# Specify a different CSV
python -m src.app --question "Show monthly sales trends" --source "path/to/data.csv"
```

### Example Questions

```bash
python -m src.app -q "What are the top 5 product categories by total revenue?"
python -m src.app -q "What is the average order value by country?"
python -m src.app -q "Show me monthly revenue trends for 2023"
python -m src.app -q "Which shipping method has the highest average rating?"
python -m src.app -q "What is the revenue split between customer segments?"
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

Each run creates artifacts in `outputs/runs/{run_id}/`:

| File | Description |
|------|-------------|
| `events.jsonl` | Full event log (every agent input/output) |
| `schema.json` | Introspected database schema |
| `business_context.json` | Extracted business context |
| `technical_spec.json` | SQL task specification |
| `query.sql` | The executed SQL query |
| `result.parquet` | Full query results |
| `result_preview.json` | First 20 rows (PII redacted) |
| `analysis.json` | Structured analytical report |
| `final.json` | Complete final response |
| `final.md` | Formatted Markdown report |

## Safety Guarantees

- **Read-only SQL:** Deterministic AST validation (sqlglot) blocks any DML/DDL before execution
- **Single statement only:** Multi-statement SQL is rejected
- **Table/column allowlist:** Only the loaded data table is queryable
- **PII minimization:** Email, Phone, Address, Name are redacted from previews by default
- **Prompt injection screening:** Basic pattern matching + LLM advisory review
- **Automatic LIMIT:** Queries without LIMIT get a safe default injected

## Running Tests

```bash
pytest tests/ -v
```

## Project Structure

```
src/
  app.py              # CLI entry point
  orchestrator.py     # Pipeline state machine
  agents/             # 6 LLM agent definitions
  tools/              # Deterministic tools (schema, validate, execute, log, redact)
  models/schemas.py   # Pydantic I/O contracts
  prompts/            # Agent prompt templates (Markdown)
tests/                # Unit tests for tools
data/sources/         # CSV data files
outputs/runs/         # Per-run artifacts
```

## Architecture

The system follows the **hub-and-spoke / agent-as-tool** pattern from the [OpenAI Agents SDK cookbook](https://github.com/openai/openai-cookbook). The orchestrator drives each agent sequentially (with parallel steps where dependencies allow), using structured Pydantic output types to ensure reliable data flow between stages.

Key design decision: the **deterministic SQL validator** (sqlglot AST inspection) is the hard security gate. The LLM evaluator agent provides advisory feedback but is never the sole guardrail.
