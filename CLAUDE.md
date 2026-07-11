# jx-sqlite

JSON Query Expressions (JX) compiled to SQLite SQL.

## Purpose

Store JSON documents in a dynamically-managed SQLite schema and query them via a null-safe query language. The schema expands automatically as new documents are encountered — no manual migrations.

## Key Concepts

### Decisive (Null-Safe) Operators

JX uses "decisive" null semantics: `null` means "out of class" (the slot shouldn't exist), not "unknown value". A decisive op *skips* null operands (non-null wins); it is null only when all operands are null.

- `sum(42, null) => 42` — decisive: null skipped, non-null wins
- `eq(null, null) => true` — nulls match each other
- Contrast with SQL's conservative `NULL` (any operation with NULL returns NULL) and strict languages (NULL raises an error)

**Not every operator is decisive by default.** Scalar/fixed-arity operators (`add`, `mul`, `least`, `most`) are *conservative* (any null ⇒ null, like SQL `a+b`); aggregates (`sum`, `product`, `min`, `max`) are *decisive*. The `nulls` clause overrides per call. See `docs/null_semantics.md` (repo-local policy) and `C:\Users\kyle\code\ActiveData\docs\jx_decisive_operators.md` for the full spec.

### Snowflake / Hierarchy Model

A single hierarchy of JSON nesting maps to a "snowflake" of relational tables:

- **Fact table** (`.`): root document properties
- **Nested tables** (e.g., `a.b`): JSON arrays become child tables with `_parent` FK and `_order` column
- **Perspectives**: any table can be the query origin; field references are relative to that origin

One snowflake covers one hierarchy. A relational database with multiple hierarchies uses multiple snowflakes.

Inner objects `{"a": {"b": 0}}` are treated as a shortcut for `{"a": [{"b": 0}]}` — so schemas can evolve from one-to-one to one-to-many without breaking existing queries.

### Expression Pipeline

JX expressions (e.g., `{"gt": {"value": 0}}`) are represented as operator objects (`AddOp`, `EqOp`, `MissingOp`, …). Each implements:

- `.to_sql(schema) -> SqlScript` — produces SQL with type info and null mask
- `.partial_eval(lang)` — simplifies/folds constants before SQL generation

`SqlScript` is the lingua franca: it carries the SQL expression, the `jx_type`, and a `miss` (null indicator) alongside.

## Key Files

| File | Role |
|------|------|
| `jx_sqlite/expressions/_utils.py` | `JxSql` language definition; null-safety patterns |
| `jx_sqlite/expressions/variable.py` | JSON field → SQL column mapping; COALESCE for union types |
| `jx_sqlite/expressions/eq_op.py` | Null-aware equality (arrays → IN, nulls → explicit IS NULL checks) |
| `jx_sqlite/expressions/missing_op.py` | Null detection (`IS NULL`; text also checks `== ''`) |
| `jx_sqlite/query.py` | Query API: filtering, deletion, result formatting |
| `jx_sqlite/edges.py` | Multi-table joins, nested aggregations, snowflake traversal |
| `jx_sqlite/insert.py` | Schema evolution: new columns, scalar→array promotion |
| `vendor/mo_sqlite/models/snowflake.py` | Snowflake model; manages fact table + nested table hierarchy |
| `vendor/mo_sqlite/models/schema.py` | Per-table perspective; maps columns relative to `nested_path` |

## Running Tests

```
# Windows
set PYTHONPATH=.;vendor
python -m unittest discover -v -s tests

# Linux
export PYTHONPATH=.:vendor
python -m unittest discover -v -s tests
```

Or install the locked dependency set first:

```
python.exe -m pip install --no-deps -r tests\requirements.lock
```

Do not change tests (including the harness in `tests/__init__.py`) to make them pass — they
are more likely correct than the code they test. A failing test indicts the code; catching or
working around the failure in the harness hides the defect.

## Status

Jan 2024: 118 of 334 tests ignored due to library breakage. Core simple-case functionality works.

The repair campaign is tracked in `docs/TEST_TRIAGE.md` — all sqlite-skipped tests grouped by
root cause, with a suggested order of attack. Pick a cluster, fix, un-skip, update the doc.

**The central problem** (Kyle): discovering the set of intermediate operators that sit in the
intersection between clean JX edges and plain SQL — each language simple alone, complicated in
translation. The codebase contains redundant partial drafts of that layer, from separate
attacks on the problem (SqlStep/SqlTree in `mo_sqlite/sql_script.py`; the `sql_*_op.py` files
in `jx_sqlite/expressions/`; inline logic in `jx_sqlite/edges.py`). Redundancy you find there
is expected, not accidental design. If the right operators are found, the rest of the code
should get easier; work in that area should try to name them.
`docs/INTERSECTION_SURVEY.md` maps all the existing attacks side by side and induces a
candidate operator set from them.

## Deeper documentation (per-directory CLAUDE.md)

Each load-bearing package has its own CLAUDE.md with contracts, invariants, and traps —
loaded automatically when working on files there:

- `vendor/jx_base/CLAUDE.md` — operator double-dispatch machinery (read before debugging any op)
- `vendor/mo_sqlite/CLAUDE.md` — SQLang vs JxSql; SqlScript invariants; snowflake models
- `jx_sqlite/CLAUDE.md` — query pipeline; how decisive ops become SQL
- `vendor/mo_json/CLAUDE.md` — typed encoding (`~n~`, `~s~`, `~a~`, …) and JxType
- `vendor/mo_dots/CLAUDE.md` — Null semantics (`== None` is deliberate); field-path algebra
- `vendor/jx_python/CLAUDE.md` — JX over Python objects; post-processing shortcuts
- `tests/CLAUDE.md` — test harness shape; three output formats

### Language stack

| Language | Package | Role |
|----------|---------|------|
| `JX` | `jx_base` | abstract ops, decisive null semantics |
| `Python` | `jx_python` | interpret over Python objects (as buggy as the rest) |
| `SQLang` | `mo_sqlite` | literal SQLite SQL ops, strict null semantics |
| `JxSql` | `jx_sqlite` | compile decisive JX to SQLang |
