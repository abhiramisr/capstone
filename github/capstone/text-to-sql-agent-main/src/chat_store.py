import sqlite3
from pathlib import Path
from typing import List, Dict, Any

DB_PATH = Path(__file__).resolve().parent.parent / "chat_history.db"


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create the chat history table if it does not exist."""
    conn = _get_conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                sql TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def save_chat(question: str, answer: str, sql: str) -> None:
    """Persist a single chat exchange."""
    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT INTO chat_history (question, answer, sql)
            VALUES (?, ?, ?)
            """,
            (question, answer, sql),
        )
        conn.commit()
    finally:
        conn.close()


def get_recent_chats(limit: int = 20) -> List[Dict[str, Any]]:
    """Return the most recent chats, newest first."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            """
            SELECT id, question, answer, sql, created_at
            FROM chat_history
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def delete_chat(chat_id: int) -> None:
    """Delete a single chat from history by id."""
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM chat_history WHERE id = ?", (chat_id,))
        conn.commit()
    finally:
        conn.close()

