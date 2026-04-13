"""Deterministic SQL validation using sqlglot AST inspection.

This is the *hard gate* — the LLM evaluator is advisory only.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from src.models.schemas import SchemaSummary, ValidationIssue, ValidationResult
from src.tools.schema_introspect import get_allowlisted_tables

# AST node types that represent dangerous operations
_DANGEROUS_NODES = (
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Command,
    exp.Copy,
    exp.Pragma,
    exp.Set,
)

DEFAULT_LIMIT = 5000


def validate_sql(
    sql: str,
    schema: SchemaSummary,
    *,
    dialect: str = "duckdb",
    use_fabric: bool = False,
) -> ValidationResult:
    """Validate *sql* against safety rules and the provided schema.

    Returns a ``ValidationResult`` — check ``has_blockers`` before executing.
    """
    issues: list[ValidationIssue] = []

    # ------------------------------------------------------------------
    # 1. Reject multi-statement (semicolons that separate statements)
    # ------------------------------------------------------------------
    stripped = sql.strip().rstrip(";")
    if ";" in stripped:
        issues.append(
            ValidationIssue(
                severity="blocker",
                category="readonly",
                message="Multiple statements detected (semicolons inside SQL). Only a single SELECT is allowed.",
            )
        )
        return _build_result(issues)

    # ------------------------------------------------------------------
    # 2. Parse with sqlglot
    # ------------------------------------------------------------------
    try:
        parsed = sqlglot.parse(stripped, dialect=dialect)
    except sqlglot.errors.ParseError as exc:
        issues.append(
            ValidationIssue(
                severity="blocker",
                category="syntax",
                message=f"SQL parse error: {exc}",
            )
        )
        return _build_result(issues)

    if len(parsed) != 1 or parsed[0] is None:
        issues.append(
            ValidationIssue(
                severity="blocker",
                category="syntax",
                message="Expected exactly one SQL statement.",
            )
        )
        return _build_result(issues)

    tree = parsed[0]

    # ------------------------------------------------------------------
    # 3. Root must be SELECT or WITH → SELECT
    # ------------------------------------------------------------------
    root_type = type(tree)
    if root_type not in (exp.Select, exp.Union, exp.Intersect, exp.Except):
        # WITH wraps its body as a Select
        if not isinstance(tree, exp.Select):
            issues.append(
                ValidationIssue(
                    severity="blocker",
                    category="readonly",
                    message=f"Root statement is {root_type.__name__}, not SELECT.",
                )
            )

    # ------------------------------------------------------------------
    # 4. Walk AST for dangerous nodes
    # ------------------------------------------------------------------
    for node in tree.walk():
        if isinstance(node, _DANGEROUS_NODES):
            issues.append(
                ValidationIssue(
                    severity="blocker",
                    category="readonly",
                    message=f"Disallowed operation: {type(node).__name__}",
                )
            )

    # ------------------------------------------------------------------
    # 5. Table allowlist (exclude CTE aliases)
    # ------------------------------------------------------------------
    # Collect CTE names so they aren't flagged as disallowed tables
    cte_names: set[str] = set()
    for cte in tree.find_all(exp.CTE):
        if cte.alias:
            cte_names.add(cte.alias.lower())

    allowed = get_allowlisted_tables(use_fabric=use_fabric)
    allowed_lower = {t.lower() for t in allowed} | cte_names

    for tbl in tree.find_all(exp.Table):
        tbl_name = tbl.name.lower() if tbl.name else ""
        if tbl_name and tbl_name not in allowed_lower:
            issues.append(
                ValidationIssue(
                    severity="blocker",
                    category="schema",
                    message=f"Table '{tbl.name}' is not in the allowed table list: {sorted(allowed)}",
                )
            )

    # ------------------------------------------------------------------
    # 6. Column existence check (best-effort)
    # ------------------------------------------------------------------
    known_cols = schema.column_names  # lowercase set
    for col_node in tree.find_all(exp.Column):
        col_name = col_node.name.lower() if col_node.name else ""
        # Skip star, skip columns that have an explicit table qualifier
        # that might be a subquery alias
        if col_name and col_name != "*" and col_name not in known_cols:
            # Only warn — aliases / computed columns may cause false positives
            issues.append(
                ValidationIssue(
                    severity="warn",
                    category="schema",
                    message=f"Column '{col_node.name}' not found in schema. It may be an alias.",
                )
            )

    # ------------------------------------------------------------------
    # 7. Inject LIMIT if missing
    # ------------------------------------------------------------------
    has_limit = tree.find(exp.Limit) is not None
    # Also check if it's an aggregate-only query (no GROUP BY but has agg functions)
    has_group = tree.find(exp.Group) is not None
    has_agg = any(isinstance(n, exp.AggFunc) for n in tree.walk())
    is_small_aggregate = has_agg and not has_group  # e.g. SELECT COUNT(*) …

    limit_injected = False
    if not has_limit and not is_small_aggregate:
        limit_hint = (
            f"a row cap ({DEFAULT_LIMIT}) will be injected (T-SQL: TOP / FETCH)"
            if dialect == "tsql"
            else f"a LIMIT {DEFAULT_LIMIT} will be injected"
        )
        issues.append(
            ValidationIssue(
                severity="warn",
                category="schema",
                message=f"No row limit found. {limit_hint}.",
            )
        )
        limit_injected = True

    return _build_result(issues, limit_injected=limit_injected, dialect=dialect)


def inject_limit(sql: str, limit: int = DEFAULT_LIMIT, *, dialect: str = "duckdb") -> str:
    """Append a LIMIT clause (DuckDB) or TOP / FETCH (T-SQL) if not already present."""
    try:
        parsed = sqlglot.parse(sql.strip().rstrip(";"), dialect=dialect)
        if parsed and parsed[0] is not None:
            tree = parsed[0]
            if tree.find(exp.Limit) is None:
                tree = tree.limit(limit)
            return tree.sql(dialect=dialect)
    except Exception:
        pass
    stripped = sql.strip().rstrip(";")
    if dialect == "tsql":
        return f"{stripped}\nOFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
    return f"{stripped}\nLIMIT {limit}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_result(
    issues: list[ValidationIssue],
    limit_injected: bool = False,
    dialect: str = "duckdb",
) -> ValidationResult:
    has_blocker = any(i.severity == "blocker" for i in issues)
    fix_msg = None
    if limit_injected:
        fix_msg = (
            "Row cap was auto-injected (T-SQL)"
            if dialect == "tsql"
            else "LIMIT was auto-injected"
        )
    return ValidationResult(
        is_safe=not has_blocker,
        is_read_only=not any(i.category == "readonly" and i.severity == "blocker" for i in issues),
        has_prompt_injection=False,  # deterministic check is basic; LLM does deeper check
        syntax_ok=not any(i.category == "syntax" and i.severity == "blocker" for i in issues),
        schema_ok=not any(i.category == "schema" and i.severity == "blocker" for i in issues),
        issues=issues,
        recommended_fix=fix_msg,
    )
