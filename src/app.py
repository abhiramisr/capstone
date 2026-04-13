"""CLI entry point for the text-to-SQL agent pipeline.

IMPORTANT: load_dotenv() is called at module level (before any src.* imports)
so that environment variables are available when agent modules are imported
and call get_model_name() during Agent() construction.
"""

from __future__ import annotations

# ── Load .env FIRST — before any src.* imports that call get_model_name() ──
from dotenv import load_dotenv
load_dotenv()
# ───────────────────────────────────────────────────────────────────────────

import argparse
import asyncio
import os
import sys

from src.connectors.azure_openai_connector import setup_foundry_tracing
setup_foundry_tracing()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ask a natural language question about your retail data.",
    )
    parser.add_argument(
        "--question", "-q",
        required=True,
        help="Natural language question (e.g. 'What are the top 5 categories by revenue?')",
    )
    parser.add_argument(
        "--source", "-s",
        default="new_retail_data 1.csv",
        help="Path to the CSV data source (default: 'new_retail_data 1.csv')",
    )
    args = parser.parse_args()

    # Validate that at least one LLM backend is configured.
    has_foundry = bool(os.environ.get("AZURE_AI_INFERENCE_ENDPOINT"))
    has_azure   = bool(os.environ.get("AZURE_OPENAI_ENDPOINT"))
    has_openai  = bool(os.environ.get("OPENAI_API_KEY"))
    if not (has_foundry or has_azure or has_openai):
        print("ERROR: No LLM backend configured. Set one of:")
        print("  AZURE_AI_INFERENCE_ENDPOINT  (Azure AI Foundry — preferred)")
        print("  AZURE_OPENAI_ENDPOINT        (Azure OpenAI Service)")
        print("  OPENAI_API_KEY               (direct OpenAI fallback)")
        sys.exit(1)

    # Resolve CSV path
    csv_path = args.source
    if not os.path.isabs(csv_path):
        if not os.path.exists(csv_path):
            alt = os.path.join("data", "sources", csv_path)
            if os.path.exists(alt):
                csv_path = alt

    if not os.path.exists(csv_path):
        print(f"ERROR: CSV file not found: {csv_path}")
        sys.exit(1)

    from src.orchestrator import run_pipeline

    result = asyncio.run(run_pipeline(args.question, csv_path))

    print("\n" + "=" * 70)
    print("FINAL ANSWER")
    print("=" * 70)
    print(f"\n{result.answer}\n")

    if result.sql:
        print("-" * 70)
        print("SQL USED:")
        print("-" * 70)
        print(result.sql)
        print()

    if result.execution_summary:
        print(f"[{result.execution_summary}]")


if __name__ == "__main__":
    main()