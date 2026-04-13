"""JSONL event logger for pipeline runs."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any


class RunLogger:
    """Append-only JSONL logger that writes to ``outputs/runs/{run_id}/events.jsonl``."""

    def __init__(self, run_id: str, output_dir: str = "outputs/runs"):
        self.run_id = run_id
        self.run_dir = os.path.join(output_dir, run_id)
        os.makedirs(self.run_dir, exist_ok=True)
        self._log_path = os.path.join(self.run_dir, "events.jsonl")

    # ----- public API -----

    def log(self, stage: str, event: str, payload: Any = None) -> None:
        """Append a single event line."""
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id,
            "stage": stage,
            "event": event,
            "payload": _serializable(payload),
        }
        with open(self._log_path, "a") as f:
            f.write(json.dumps(record, default=str) + "\n")

    def save_artifact(self, name: str, content: str) -> str:
        """Write an arbitrary text artifact and return its path."""
        path = os.path.join(self.run_dir, name)
        with open(path, "w") as f:
            f.write(content)
        return path

    def save_json_artifact(self, name: str, data: Any) -> str:
        """Write a JSON artifact and return its path."""
        path = os.path.join(self.run_dir, name)
        with open(path, "w") as f:
            json.dump(_serializable(data), f, indent=2, default=str)
        return path


def _serializable(obj: Any) -> Any:
    """Best-effort conversion to JSON-safe types."""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if isinstance(obj, dict):
        return {k: _serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serializable(v) for v in obj]
    return str(obj)
