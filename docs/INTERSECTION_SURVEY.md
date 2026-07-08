# The Intersection Layer — a survey of existing attacks

**The problem** (Kyle): JX queries over a snowflake are simple; SQL is simple; the translation
between them is where all the complexity lives. edges.py (and friends) must be decomposed into
intermediate operators, but the right operator set is unknown. This document lays every
existing partial attempt side by side — including dead drafts — so the operator set can be
*induced* from the union of them instead of invented from scratch.

Surveyed 2026-07-07 from: `jx_sqlite/setop.py`, `jx_sqlite/edges.py`, `jx_sqlite/group.py`,
`jx_sqlite/window.py`, `jx_sqlite/expressions/sql_*_op.py`, `jx_sqlite/expressions/nested_op.py`,
`vendor/mo_sqlite/sql_script.py` (SqlStep/SqlTree), `vendor/mo_sqlite/models/container.py`,
`vendor/jx_base/expressions/sql_left_joins_op.py` and the abstract `sql_*_op.py` family.

---

## 1. The recurring sub-problems (a common vocabulary)

Every attack solves some subset of the same eight sub-problems:

| # | Sub-problem | Description |
|---|------------|-------------|
| P1 | **Origin choice** | Which table is the query's perspective; renaming names relative to it |
| P2 | **Join-chain** | LEFT JOIN chain along a nested_path (`child.__parent__ = parent.__id__`) to reach a column's table from the origin |
| P3 | **Select alignment** | One flat select list shared by several subqueries; NULL-pad the columns each branch doesn't produce |
| P4 | **Branch union** | UNION ALL of one subquery per table of the snowflake |
| P5 | **Order recovery** | Reassemble document order from `__id__`/`__order__` columns; ORDER BY over the union |
| P6 | **Domain materialization & completion** | Build the domain as a table (set literals / DISTINCT from facts / DIGITS arithmetic); guarantee a result row per domain part even with no data |
| P7 | **Result shaping (pull plan)** | Map flat result columns back into nested JSON (ColumnMapping.pull, DocumentDetails, format.py) |
| P8 | **Aggregate application** | Apply SQL aggregates, incl. tricks: COUNT of record via `__exists__`, AND/OR via SUM/NOT, union via JSON_GROUP_ARRAY, stats bundle |

---

## 2. The attacks

### Attack A — `_set_op` / `_make_sql_for_one_nest_in_set_op` (`jx_sqlite/setop.py`)

*Style:* monolithic functions on `Facts`. *Status:* **live** — this is what passes today's tests.

- **Input → output:** QueryOp → (index_to_column, SqlScript, DocumentDetails tree); then
  `_accumulate_nested` walks flat rows back into nested docs.
- **Solves:** P2 (inline), P3 (NULLs for "deep stuff at this level"), P4 (recursive UNION ALL,
  one branch per nested level), P5 (ORDER BY uid columns per path), P7 (the most complete
  pull plan anywhere: `DocumentDetails` tree mirrors the snowflake; `_accumulate_nested` is a
  streaming groupwise reassembly).
- **Unique knowledge:**
  - `__order__ = 0` distinguishes the inner-object row from array rows: child tables join with
    `ORDER = 0` to fetch "the first row" (inner-object semantics), while parent levels filter
    `ORDER > 0` to avoid double-counting. This encodes the "inner object = one-element array"
    rule at the SQL level, and no other attack has it.
  - Sibling nested paths are *ignored* per branch (each branch handles one root-to-leaf line).

### Attack B — `SqlStep` / `SqlTree` (`vendor/mo_sqlite/sql_script.py`)

*Style:* clean classes. *Status:* **dead draft — zero call sites.** A redo of A's SQL-assembly
half, without A's result-shaping half.

- **Input → output:** tree of SqlStep(parent, subquery, selects, uids, order) → one SQL:
  UNION ALL of branches, ORDER BY over all order/uid columns.
- **Solves:** P2 (`branch_sql` walks parent chain, `SqlJoinOne` on uid equality), P3 (the
  cleanest solution anywhere: systematic column names `o{id}_{i}` / `i{id}_{i}` / `c{id}_{i}`
  with mechanical NULL padding via `position()` start/end offsets), P4, P5.
- **Unique knowledge:** separating `node_sql` (minimum columns to pass through a level) from
  `leaf_sql` (full columns at the level being selected); the alias scheme makes select
  alignment a computation instead of bookkeeping.
- **Missing:** P1, P6, P7, P8 — and any caller.

### Attack C — the algebraic ops (`jx_sqlite/expressions/sql_*_op.py` + jx_base abstractions + `mo_sqlite/models/container.py`)

*Style:* expression algebra — tables and queries as composable operators. *Status:* **in-flight
and partially stale**; entry point exists (`Container.query` builds `SqlSelectAllFromOp` for a
Variable query) but most paths raise NotImplementedError, and two `to_sql` methods
(`SqlLeftJoinsOp`, `SelectOp` in jx_sqlite) pass `data_type=` to a SqlScript that takes
`jx_type=` — they would TypeError if reached. There are also **two** `SqlSelectAllFromOp`
drafts: mo_sqlite's (just emits `SELECT * FROM t`) and jx_sqlite's (has the `.query(expr)`
resolution logic).

- **The algebra so far:** `SqlSelectAllFromOp(table)` represents a whole table as an
  expression; `.query(expr)` resolves a Variable against the schema → `SelectOp`, or a
  many-relation → builds `Source`/`Join` topology, wraps child in `SqlGroupByOp`, and returns
  `SqlOriginsOp(SqlLeftJoinsOp(parent), child)`. `AggregateOp` distributes over it.
- **Solves (in intent):** P1 — **`SqlOriginsOp` is the only attack that names origin choice as
  an operator** (root + origin, query applies at the origin); P2 as *data*
  (`Source`/`Join` dataclasses in jx_base — topology separated from SQL emission).
- **Unique knowledge:** `SqlGroupByOp`'s docstring is the clearest statement of the whole
  intersection problem in the codebase: *group-by without aggregation is a hierarchical
  result, and the SQL for it is the parent/child UNION ALL* — the same SQL shape A and B
  produce for nested tables. Container.query's comments sketch the model: "SELECT is a
  lambda; FROM <snowflake> is really a tree (union) of joined tables, each with schema."

### Attack D — `_edges_op` + `aggregates` (`jx_sqlite/edges.py`)

*Style:* monolithic. *Status:* live for shallow cases; the file slated for decomposition.

- **Input → output:** QueryOp with edges → (SQL, index_to_column).
- **Solves:** P2 (inline, third copy), P6 (**entirely — nothing else touches it**),
  P8 (all the special cases), P5 (orderby with `IS NULL` first so the null part sorts last),
  P7 (ColumnMapping + `domain.getKeyByIndex` pulls).
- **Unique knowledge (P6 in detail):**
  - Three domain materialization strategies: *set* → UNION ALL of literal rows; *default* →
    `SELECT COUNT(1), value FROM facts GROUP BY value ORDER BY count DESC LIMIT n`
    (the domain is *discovered* from the data); *time/duration/range* → arithmetic over
    DIGITS_TABLE cross-joins (a generated integer sequence).
  - Domain completion is a **two-phase full-outer-join emulation**: phase 1 aggregates facts
    joined to domains; phase 2 ("ALL COORDINATES MISSED BY primary DATA") cross-joins the
    completed domains (`domain UNION ALL SELECT NULL` when allowNulls) and LEFT JOINs phase 1
    back onto it. This exists because SQLite historically lacks FULL OUTER JOIN.
  - Multi-column edges (tuple/select) need per-part domain aliases and null-tolerant
    equality in the ON clause (`a = b OR (a IS NULL AND b IS NULL)`) — decisive eq leaking
    into join conditions.

### Attack E — `_groupby_op` (`jx_sqlite/group.py`)

*Style:* monolithic; a simpler sibling of D. *Status:* live.

- **Solves:** P2 (inline, *fourth* copy of the same LEFT-JOIN-chain construction), P8
  (plain subset), P7. Deliberately no P6: groupby emits no row for empty groups — that is
  the semantic difference between `groupby` and `edges`.
- Redundancy evidence: the `nest_to_alias`/`tables`/`from_sql` preamble in D and E is
  near-identical, line for line.

### Attack F — `_window_op` (`jx_sqlite/window.py`)

Small, orthogonal: JX window → SQL window function (`OVER (PARTITION BY ... ROWS BETWEEN)`).
Mostly string assembly; consumed by both D and E as one more select column.

### Bridge — `NestedOp` (`jx_sqlite/expressions/nested_op.py`)

A query on a nested table *as an expression*: builds a QueryOp and calls `Facts.to_sql`
(attack A) from inside expression compilation. It is the seam where the expression algebra
(C) reaches back into the monoliths — proof that "query on a nested table" wants to be an
operator, and currently isn't.

---

## 3. Redundancy map

| Sub-problem | A setop | B SqlTree | C algebra | D edges | E group |
|---|---|---|---|---|---|
| P1 origin | implicit | — | **SqlOriginsOp** | implicit | implicit |
| P2 join-chain | inline #1 | branch_sql | Source/Join (data) | inline #2 | inline #3 |
| P3 select alignment | ad-hoc NULLs | **o/i/c scheme** | — | — | — |
| P4 branch union | recursive | leaves loop | (docstring) | — | — |
| P5 order recovery | uid sorts | order cols | — | IS NULL trick | sort clause |
| P6 domains | — | — | — | **only here** | (refused) |
| P7 pull plan | **DocumentDetails** | — | — | ColumnMapping | ColumnMapping |
| P8 aggregates | — | — | AggregateOp stub | **all cases** | subset |

P2 is solved four times; P3 twice; P6 and the best P3/P7 solutions each exist exactly once,
in different attacks that don't know about each other.

---

## 4. Induction: what the intermediate operators appear to be

**The unifying observation.** Every attack is a special case of one job: *deliver a
one-to-many (hierarchical) result through SQL's flat, rectangular interface.* The flat
encoding is invariant across all of them — aligned select lists, UNION ALL branches, order
columns, then a pull plan to fold rows back into hierarchy. What differs is only **where the
hierarchy comes from**:

1. **nested child tables** (A, B) — hierarchy stored in the snowflake;
2. **group-by** (C's SqlGroupByOp, E) — hierarchy created by partitioning;
3. **domain cross-product** (D) — hierarchy imposed by declared edges, completed even where
   data is absent.

SqlGroupByOp's docstring already says this for case 2; A and B's SQL is the same shape for
case 1; D's two-phase query is the same shape for case 3 plus completion.

**Candidate operator set** (names indicative; every one already exists in embryo):

- `SqlOrigins(root, origin)` — P1. Exists (C). The perspective operator.
- `SqlJoinChain(Source, Join…)` — P2. Exists as data (C); write the emitter once, delete
  four inline copies.
- `SqlDomain(edge) -> table` — P6a. Extract D's three materialization strategies as an
  operator producing a plain table expression.
- `SqlComplete(facts_agg, domains…)` — P6b. The full-outer-join emulation (phase-1/phase-2
  pattern) as its own operator, independent of what is being completed.
- `SqlHierarchy(parent, children…)` — P3+P4+P5 fused: the one-to-many result encoder.
  B (SqlStep/SqlTree) is 80% of this and is currently dead code; it needs the `ORDER = 0`
  inner-object rule from A, which B never learned.
- `PullPlan` — P7. Not SQL at all: the declarative inverse of `SqlHierarchy` (flat columns →
  nested docs). A's DocumentDetails/_accumulate_nested is the seed. This wants to be its own
  module regardless of what happens above it.
- `SqlAggregate(op, expr)` — P8. D's `aggregates()` special cases as small per-op rules
  (several already exist as jx_base AggregateOp machinery).

Then the three query strategies stop being monoliths and become compositions:

    setop   =  Origins → JoinChain → Hierarchy → PullPlan
    groupby =  Origins → JoinChain → GroupBy → Aggregate → PullPlan
    edges   =  Origins → JoinChain → Domain* → GroupBy → Aggregate → Complete → PullPlan

**A note of caution.** This induction is from the code's own drafts, not from first
principles; the operator boundaries above may still be wrong (in particular, whether
`SqlHierarchy` and `Complete` are one operator or two — completion produces a hierarchy too).
The test of a good decomposition: attack A, D, and E should each shrink to a page of
composition, and the `ORDER = 0` rule should appear in exactly one place.

---

## 5. Suggested next steps

1. **Decide B's fate.** SqlStep/SqlTree is the best P3/P4/P5 solution and is dead code.
   Either promote it to be `SqlHierarchy` (teaching it A's `ORDER = 0` rule) or delete it so
   it stops being a decoy.
2. **Unify the two SqlSelectAllFromOp drafts** (mo_sqlite emitter vs jx_sqlite resolver) —
   per the mo_sqlite/jx_sqlite split, emission belongs in mo_sqlite, schema resolution in
   jx_sqlite.
3. **Extract P2 once.** Four inline copies of the join-chain preamble → one emitter over
   Source/Join. Mechanical, low-risk, and shrinks edges.py before the harder decomposition.
4. Fix or quarantine C's stale `to_sql` signatures (`data_type=` → `jx_type=`) so the algebra
   path fails loudly at the right place, not with a TypeError.
5. Only then attack triage cluster 1 — with fixes phrased as compositions of the named
   operators wherever possible.
