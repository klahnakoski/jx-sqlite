# jx_sqlite — JX queries compiled to SQLite

The top of the stack: takes a JX query against a Facts (snowflake) and produces SQL + shaped
results. Defines the **JxSql** language (`expressions/__init__.py: JxSql.register_ops(vars())`).

Ongoing split (Kyle): pure SQL is migrating down into mo_sqlite; the **projection and
null-safe (decisive) transformations belong here**. New code should respect that boundary.

## Query pipeline

1. `Facts.query(...)` (`query.py`) — normalize to `QueryOp`, pick strategy:
   - plain select → `setop.py`
   - `groupby` → `group.py`
   - `edges` (aggregation with domains) → `edges.py` (multi-table joins, snowflake traversal)
   - `window` → `window.py`

   **edges.py is slated for decomposition (Kyle).** It must be broken up into component
   operators, but the higher-level operators are not yet known. The hard part is the
   intersection of clean JX edges and regular SQL: what intermediate operators live between
   the two languages — each language simple in its own right, complicated in translation.
   When fixing edge/deep-query bugs, prefer changes that surface candidate intermediate
   operators over changes that entangle edges.py further; naming a good intermediate op is
   worth more than the bug fix.
2. Expression compilation: each op `.partial_eval(SQLang)` then `.to_sql(schema)` →
   `SqlScript` (see `vendor/mo_sqlite/CLAUDE.md` for its invariants). With TYPE_CHECK on, the
   `@check` decorator (`expressions/_utils.py`) verifies every `to_sql` returns a SqlScript.
3. `format.py` shapes rows into the three output formats every test exercises:
   `list`, `table`, `cube`.

## How decisive ops become SQL (`expressions/_utils.py`)

- Decisive multi-op: each term COALESCEd with the op's identity value (add→0, mul→1), and
  `miss = AND(term.missing() for terms)` — null only when *all* terms are null.
- Non-decisive: `miss = OR(...)`.
- `strict_*_op.py` classes are the raw SQL-semantics versions (`miss=FALSE`); decisive ops
  `partial_eval` into strict ops wrapped with explicit null handling.
- Inequalities coerce with `ToNumberOp` and COALESCE the comparison to 0 (false when either
  side null).

## Traps

- `variable.py`: one JX name may map to several typed columns (union types) → COALESCE; a
  Variable's `to_sql` depends on the schema's origin (`nested_path[0]`), so the same query
  compiles differently per perspective.
- `eq_op.py`: array RHS → IN; decisive eq means `eq(null, null)` is TRUE — generated SQL must
  include explicit `IS NULL` branches.
- `missing_op.py`: for text columns, empty string counts as missing.
- Text between ops (left/right/find/between) are built from strict substring ops; several are
  currently broken (see `docs/TEST_TRIAGE.md` clusters 3 and 9 — one known defect:
  "partial_eval(SQLang) before to_sql(schema)" ordering).

## Current campaign

`docs/TEST_TRIAGE.md` tracks the ~122 sqlite-skipped tests by root-cause cluster, with a
suggested order of attack. Work sessions should pick one cluster, fix, un-skip, update triage.
