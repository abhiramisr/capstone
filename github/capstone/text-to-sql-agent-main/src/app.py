"""CLI entry point for the text-to-SQL agent pipeline."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from dotenv import load_dotenv


def main() -> None:
    load_dotenv()

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

    # Validate API key (after argparse so --help still works)
    if not os.environ.get("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY not set. Create a .env file or export it.")
        sys.exit(1)

    # Resolve CSV path
    csv_path = args.source
    if not os.path.isabs(csv_path):
        # Try relative to cwd first, then data/sources/
        if not os.path.exists(csv_path):
            alt = os.path.join("data", "sources", csv_path)
            if os.path.exists(alt):
                csv_path = alt

    if not os.path.exists(csv_path):
        print(f"ERROR: CSV file not found: {csv_path}")
        sys.exit(1)

    # Import here to avoid triggering agent construction before env is loaded
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
