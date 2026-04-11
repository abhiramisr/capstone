"""Prompt loader utility."""

from __future__ import annotations

import os
from functools import lru_cache

_PROMPTS_DIR = os.path.dirname(__file__)


@lru_cache(maxsize=32)
def load_prompt(name: str) -> str:
    """Load a prompt markdown file by name (without extension)."""
    path = os.path.join(_PROMPTS_DIR, f"{name}.md")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()
