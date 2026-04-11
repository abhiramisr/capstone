from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv
import os
from typing import Any, List, Dict

from src.orchestrator import run_pipeline
from src.models.schemas import FinalResponse, ColumnInfo
from src.tools.schema_introspect import load_csv_to_duckdb
from src.tools.sql_execute import execute_sql
from src.chat_store import init_db, save_chat, get_recent_chats, delete_chat


load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3001",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class QueryRequest(BaseModel):
    query: str
    source: str | None = None


def _resolve_csv_path(source: str | None) -> str:
    """Mirror the CLI behavior for locating the CSV file."""
    csv_path = source or "new_retail_data 1.csv"
    if not os.path.isabs(csv_path):
        if not os.path.exists(csv_path):
            alt = os.path.join("data", "sources", csv_path)
            if os.path.exists(alt):
                csv_path = alt
    return csv_path


def _build_chart_data(preview_rows: List[Dict[str, Any]], columns: List[ColumnInfo]) -> List[Dict[str, Any]]:
    """
    Build a simple chart configuration grounded in the preview rows.

    Heuristic:
      - Use the first column as the categorical "name"
      - Use the first numeric-looking column as "value"
    """
    if not preview_rows or not columns:
        return []

    dim_col = columns[0].name

    metric_col = None
    for col in columns[1:]:
        t = col.type.lower()
        if any(tok in t for tok in ("int", "double", "decimal", "numeric", "float", "real", "hugeint")):
            metric_col = col.name
            break

    if metric_col is None and len(columns) > 1:
        metric_col = columns[1].name

    chart_data: List[Dict[str, Any]] = []
    for row in preview_rows:
        name = row.get(dim_col)
        value = row.get(metric_col) if metric_col else None
        if name is not None and value is not None:
            chart_data.append({"name": str(name), "value": value})
    return chart_data


@app.on_event("startup")
def on_startup() -> None:
    init_db()


@app.post("/ask")
async def handle_query(request: QueryRequest):
    csv_path = _resolve_csv_path(request.source)

    # When an API key is not configured, fall back to a direct,
    # deterministic query over the CSV so the UI still receives
    # grounded data and SQL.
    if not os.environ.get("OPENAI_API_KEY"):
        conn = load_csv_to_duckdb(csv_path)
        sql = "SELECT * FROM retail_transactions_typed LIMIT 20"
        exec_result = execute_sql(conn, sql, run_id="no_api_key")

        preview_rows = exec_result.preview_rows
        columns = exec_result.columns
        chart_data = _build_chart_data(preview_rows, columns)

        answer = (
            "Environment is running without an OPENAI_API_KEY. "
            "Here is a preview slice of the underlying data."
        )

        save_chat(request.query, answer, sql)

        return {
            "answer": answer,
            "sql": sql,
            "citations": ["retail_transactions_typed"],
            "chartData": chart_data,
            "previewRows": preview_rows,
            "columns": [c.model_dump() for c in columns],
        }

    # Delegate to the existing multi‑agent text‑to‑SQL pipeline.
    final: FinalResponse = await run_pipeline(request.query, csv_path)

    # Persist this exchange for history
    save_chat(final.question, final.answer, final.sql or "")

    # Build grounded chart data + preview table
    chart_data = _build_chart_data(final.preview_rows, final.columns)

    # For now, we only expose a single logical data source (the retail table).
    citations = ["retail_transactions_typed"]

    return {
        "answer": final.answer,
        "sql": final.sql,
        "citations": citations,
        "chartData": chart_data,
        "previewRows": final.preview_rows,
        "columns": [c.model_dump() for c in final.columns],
    }


@app.get("/history")
def history(limit: int = 20):
    """
    Return the most recent chat questions for easy re-use in the sidebar.
    """
    rows = get_recent_chats(limit=limit)
    # Only expose fields the UI needs
    history_items = [
        {
            "id": r["id"],
            "question": r["question"],
            "created_at": r["created_at"],
        }
        for r in rows
    ]
    return {"history": history_items}


@app.delete("/history/{chat_id}")
def delete_history_item(chat_id: int):
    """Delete a single history item by id."""
    delete_chat(chat_id)
    return {"status": "ok", "id": chat_id}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)