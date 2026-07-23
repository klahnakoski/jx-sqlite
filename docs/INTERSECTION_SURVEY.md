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

See §6 for the chosen direction that gives these steps a target shape.

---

## 6. The shared-branch builder (chosen direction, 2026-07-21)

**Decision (Kyle):** pursue a *builder* that assembles one hierarchical query by having each
operator **contribute a branch** to shared structures, rather than each operator returning a
finished, detached `SELECT`. This is `SqlHierarchy` (P3+P4+P5) and `PullPlan` (P7) fused into
one operator, kept in lockstep by a shared column index.

**Driving case.** The subquery-in-select
`select ["o", {"name":"a._a", "value":{"from":"a._a", "select":["v","s"]}}]`
(pinned skipped test `test_deep_where_on_fact_table_subquery`). Its value is a **`NestedOp`**
(`{nested_path, select, where, sort, limit}` — already the JX representation; jx_sqlite's
`NestedOp.to_sql` builds a QueryOp and calls `engine.to_sql` but **discards the pull plan**,
returning only SQL — fine for `exists`/scalar contexts, wrong for a select value).

**What "shared" means.** setop already produces ONE query + ONE pull plan; four things are
shared across every nested level:
- aligned column list (`sql_selects`, position = index; NULL-pad columns a branch lacks) — P3
- one `UNION ALL` of per-level SELECTs — P4
- one sort domain (`__id__`/`__order__` chain) so the union streams for a **single-pass**
  reassembly — P5
- one `DocumentDetails` tree that `_accumulate_nested` walks — P7

The load-bearing invariant: **the column index is the shared key** — `_make_column_name(n)`
names the SQL alias and `get_column(n)` is the pull for the same `n`. SQL side and pull side
reference one integer. A branch contribution adds columns/branch/node **under consistent
indices**; a detached subquery makes a second result set and loses both the single-pass
streaming and correlation-for-free (correlation = the branch's `JOIN _parent = parent.__id__`,
not a per-row re-query).

**The interface.** Reify what `setop.to_sql` already does inline as a builder threaded through
compilation:
```
add_branch(nested_path, parent_node, where, sort, limit) -> DocumentDetails node
add_column(node, sql, push_name, pull) -> index      # appends to the aligned list
```
Then the **select clause drives** it (today the *snowflake* drives it: setop walks
`query_paths` and `place()`s a node per table). One rule per term type:
- leaf at origin → `add_column(root, …)`
- path leaf into a child (dissolve, multivalue lift) → `add_column(child, …)` marked lift-to-parent
- `NestedOp` (group) → `add_branch(child)` then recurse over its select terms (nesting composes)
whole-child `select a._a` becomes **sugar** for a `NestedOp` child selecting all leaves.

**The two halves already exist.** SQL side = `SqlStep`/`SqlTree` (Attack B, dead): the cleanest
P3/P4/P5, `o/i/c` naming + `position()` offsets make alignment a computation. Pull side =
`DocumentDetails`/`_accumulate_nested` (Attack A, live). The builder is the operator that owns
**both on one index**; they must be one operator because the shared index is exactly what
splitting them would force you to duplicate and keep in sync. SqlTree is dead partly *because*
it solved only the SQL half — pair it with `DocumentDetails` and teach it A's `ORDER = 0`
inner-object rule.

**Why better than a detached subquery (not just tidier):**
1. Convergence — whole-child select, explicit NestedOp, and setop's snowflake walk all become
   one `add_branch` call.
2. Unlocks **branch-local `where`/`sort`/`limit`** — correlated top-N per parent
   (`{from:a._a, where:…, sort:"v", limit:3}` → `ROW_NUMBER() OVER (PARTITION BY _parent …)`).
   A flat leaf projection has nowhere to hang a per-parent filter/order/limit; a branch does.
3. edges/groupby are the same shape — `setop = branches from tables`,
   `groupby = branches from partitions`, `edges = branches from domains` (§4 induction). The
   builder is the common substrate.

**Pre-mortem (the hard parts):**
- **Nested ordering** — a branch with its own sort must nest its order *under* the parent id
  (sort within the parent group). Sort-key concatenation per depth is the subtle bit.
- **Branch-local limit needs window functions** — the branch stops being a plain
  `SELECT … UNION` and becomes a windowed subquery; localized, but the first place the
  "every branch is identical" assumption bends.
- **Perspective per branch** — compiling a NestedOp's select switches origin to the child;
  `Names`/`ResolvedName` already does perspective, but the builder must carry the right `Names`
  per node. **This is the same refactor as NAMES.md #1 ("resolve the select once at the
  origin") viewed from the other end** — `select_op`'s per-branch re-compile KLUDGE is the
  projection rule written in the wrong layer.

**Cheapest de-risking experiment.** Make the select-normalizer rewrite whole-child
`select a._a` into a `NestedOp`, and confirm it routes through the *same* branch +
`DocumentDetails` path that already makes `select ["o","a._a"]` work. If those two literally
converge on one branch, the shared-contribution model is validated before touching SqlTree or
edges.py.

---

## 7. Implemented so far, and the 3c plan (2026-07-21)

**Done (commits `af2d62d`..`5f1201a`, tree GREEN 373/0/78):**
- `jx_sqlite/builder.py`: `BranchBuilder` (with `DocumentDetails` + `place`). `setop.to_sql`
  drives it via `add_branch`/`add_column`. The column index is the shared key (SQL alias
  `_make_column_name(n)` ↔ pull `get_column(n)`).
- The branch set is **query-driven**: `branches` = the paths referenced by select ∪ where ∪
  sort, plus ancestors (for the join chain). Both the sort keys and `_make_sql_for_one_nest_in_set_op`
  read this one `branches` list, not `snowflake.query_paths`. Finding: "select-driven" is really
  **query-driven** — where/sort tables must be joined though not selected.

**3c model — CONFIRMED by Kyle.** *Every deep select is a branch; a table can host several
branches.* A branch bottoming out at ONE leaf returns a simple list of non-missing values; a
branch with several leaves returns sub-objects. The two pinned tests differ ONLY in branch
count, not in the flattened expression:
- deep-leaf path `a._a.s` ≡ one-leaf subquery. Kyle's exact rewrite:
  `["a._a.s"]` → `[{"name":"a._a.s","from":"a._a","select":"s"}]` — split at the array boundary
  (`from` = nested table, `select` = property inside, `name` = full path).
- flat `a._a.v, a._a.s` → TWO subquery terms → TWO branches at the SAME table `a._a` → two
  independent lists (`test_deep_where_on_fact_table_multivalue`).
- explicit `{from:a._a, select:[v,s]}` → ONE branch → sub-objects
  (`test_deep_where_on_fact_table_subquery`).

`partial_eval` flattening is CORRECT (it flattens each path to its deepest property); the branch
COUNT carries the grouping. This kills the `len(deep_leaves)==1` collapse hack in `setop.py`.

**Detection (found by probe):** a subquery term is `is_op(term.value, SelectOp)` (jx_base
`SelectOp`); its origin path is `first(term.value.frum.vars())` (e.g. `"a._a"`; `frum` is a
`FromOp`). Handling must read the **pre-`partial_eval`** terms — `partial_eval` flattens the
subquery back to a path. A single-leaf subquery on its table-branch reuses the existing
whole-child machinery (`_make_sql`/`_accumulate_nested` unchanged): branch-relative inner
columns assemble as sub-objects; a lone leaf collapses to a bare list via `push_list_name="."`.

**Incremental plan (each step its own GREEN checkpoint):**
1. **Subquery test — DONE** (`test_deep_where_on_fact_table_subquery` GREEN, 373/0/77).
   `setop.to_sql` partitions `query.select.terms` (pre-`partial_eval`) into plain vs subquery
   (`is_op(term.value, SelectOp)`); `select_vars`/`plain_select` are built from plain terms only
   (else `partial_eval` flattens the subquery to deep leaves = the multivalue double-nest bug);
   each subquery's origin table (`first(term.value.frum.vars())` → its leaves' `nested_path[0]`)
   is added to `active_paths` so it becomes a branch. In the branch loop, at the subquery's
   origin branch each inner term's VALUE is compiled directly against `sub_schema`
   (`value.to_sql(sub_schema)`) and `add_column`'d with branch-relative names ("v","s") — same
   column loop as the whole-child path, so no `deep_leaves` collapse fires and they assemble as
   sub-objects. NOTE: compiled the inner *values* (not the inner `SelectOp` whole) because the
   subquery's `frum` is a `FromOp`, which has no `nested_path` and trips select_op's getattr
   KLUDGE; per-value compile is also closer to what `add_column` wants. `_make_sql`/
   `_accumulate_nested` untouched.
2. **Path→subquery rewrite — DONE** (`test_deep_where_on_fact_table` GREEN via the subquery
   path, 373/0/77). `_deep_split(var)` splits a deep-leaf path at the array boundary
   (`from` = the leaf's table, relative to origin; `select` = leaf relative to that table) when
   all of `var`'s leaves live in ONE table below the origin. The partition rewrites a deep-leaf
   plain term into a one-leaf subquery entry `(name, [(select_path, Variable(select_path))])`.
   The subquery handler gained the **one-leaf collapse** (generalising A's `len(deep_leaves)==1`):
   a branch of exactly one column collapses to a bare multivalue (`leaf.push_list_name="."`,
   `node.push_list_name = outer term name`); several columns assemble as sub-objects. This
   dropped the deep leaf's redundant fact-branch NULL-pad column, so
   `test_deep_where_on_fact_table`'s whitebox `expecting_resultset` was updated 8→7 cols (Kyle
   approved; list/table/cube unchanged). GUARD: a table with SEVERAL deep-leaf plain terms stays
   on the old re-rooted plain path (`len(cands)==1 and table not in subqueries`) — step 3's job.
3. **Multiple branches per table** — decouple `add_branch` from `query_paths`; key branches on
   the select term so `a._a` can host two. Then the multivalue test's two deep leaves become
   two independent branches. This is where `_make_sql` and the `DocumentDetails` tree stop
   being 1:1 with tables.

   **3-pre: drop the inline first-row optimization (DECIDED by Kyle, do it globally).**
   The blocker for branch-per-term: today the first child row (`__order__ = 0`) is carried
   *on the parent row* (the child table is `LEFT JOIN … AND __order__ = 0` on the parent's arm,
   and the child's own arm filters `__order__ > 0`). This inline join is only an **optimization**
   — it lets SQL pre-merge one child row per parent so Python skips a row — but it is what makes
   two branches over the *same* child rows impossible: `node_v` and `node_s` would both have to
   read (and destructively advance past) the one shared inline row, and it is also the sole way
   `_accumulate_nested` currently *detects* a child (`row[child.id_coord]` read off the parent
   row). **Kyle's call: remove the optimization globally.** Each branch becomes a clean full arm
   (all its rows, `__order__ >= 0`); the parent arm carries none of the child's columns.
   `_accumulate_nested` changes from "read the child uid off the parent row" to "the child's
   rows follow the parent's row" (children on their own lines) — detect/consume each child's
   contiguous run by its own uid, in whatever order the uid sort produces (match the next row to
   whichever child's uid is non-null, not a fixed children order). Ordering already cooperates:
   sort by the real (joined) sort value then each branch's uid (NULLs first) puts the parent row
   before its children and keeps each child's run contiguous. Consequence: every deep
   `expecting_resultset` snapshot changes (first child no longer inlined onto the parent row —
   splits into separate parent/child rows); semantics (`list`/`table`/`cube`) unchanged. The
   `docs/JSON in Database.md` "Sorted and Compressed" description and any inline-first-row notes
   need updating to match.

   **BLOCKER found while attempting 3-pre (code reverted; tree kept GREEN at step 2).** Dropping
   the child join breaks **WHERE clauses that reference a nested table**. `where {exists: a._a.v}`
   at origin=fact was evaluated *on the fact arm* — which worked only because the inline
   `LEFT JOIN a._a AND __order__=0` put `a._a.v` in scope there (and dropped a fact whose first
   element lacked `v`). With no child join the fact arm errors: `no such column: …a._a.$A.v.$S`.
   So the inline join was doing **two** jobs: (a) carry the first child on the parent line (the
   optimization Kyle named) AND (b) make nested-table WHERE filters evaluable on the parent arm
   (existence filtering — drop the fact doc when no child matches). Removing it needs a decision
   on (b): how does a nested-table filter at a parent origin evaluate without the inline join,
   given the deliberate "no EXISTS subqueries, filter per child row" policy
   ([[multivalue-eval-model]])? Candidates: keep a **filter-only** join (joined, not selected) on
   the parent arm; move the filter to the child arm and drop childless parent docs at assembly;
   or relax the no-EXISTS rule for parent-origin nested filters.

   **Kyle's answer (WHERE):** filter on the child arm + drop childless parents at assembly. This
   was implemented and works (a `required` flag on the where-referenced branch node below the
   tree root; `_accumulate` drops a parent that has no row in a required child branch — covers
   both fact-origin `where exists a._a.v` and nested-origin `where b=x`, where `o=4`/childless
   facts are dropped). Per-arm WHERE: an arm applies the where iff every table it references is
   joined there (`all(startswith_field(primary, wt) for wt in where_tables)`), else `WHERE 1`.

   **SECOND BLOCKER — SORT (the inline join's other hidden job).** With the WHERE handled, the
   remaining failures were `test_sort.test_single_nested` / `test_nested`: a **nested origin**
   sorted by a nested column (`from b, sort a` = `b.a`). Two problems:
   1. the sort key `b.a` is a SELECT column valid only where `b` is joined — same off-arm
      NULL-pad fix as the WHERE (done, kills the `no such column` error); but then
   2. a nested-origin **flat** result wants a *global* value-sort (`-4,1,4,4` across all parents)
      while the no-inline streaming needs each parent's child rows *contiguous*. The value-sort
      is primary, so it scatters a parent's children across other parents, and the bare parent
      (fact) rows — which no longer carry a first child — sort last and reassemble as spurious
      empty `{}` docs. Output came out `[{},{},{a:4},{},{}]`.

   So the inline join was doing a THIRD job beyond (a)/(b): it made the parent row *carry a
   child's sort value* (parent = first child), so global value-sort and parent-contiguity
   coexisted. Without inline they conflict.

   **Kyle's answer (SORT) — the confirmed model.** "Every table/select has an order; with no
   sort it is the natural `__order__`. Streaming order and result order are the SAME — Python
   cannot handle anything else. We do not care whether the parent is contiguous if our origin is
   not the parent." So:
   - **Reassembly roots at the ORIGIN, not the fact.** For a nested origin `from b`, `b` is the
     reassembler's top level; the fact (and any ancestor of the origin) is a **join only** — it
     supplies columns and filters but is NOT a `DocumentDetails` reassembly level, so its rows
     need not be contiguous. This drops the current fact-root + post-proc `listwrap … or [{}]`
     flatten (`_set_op` lines ~143-152) and makes `primary_doc_details` the origin's node.
   - **One hierarchical `ORDER BY` = stream order = result order.** Order by the origin's key
     (its `sort`, else natural `__order__`), then each descendant level's key nested under its
     parent. For a nested origin sorted by a nested column this is a *global* sort over the
     origin's rows (fact grouping is irrelevant), giving `test_nested` / `test_single_nested`
     their global order. For a fact origin it is (fact sort, then child `__order__`), keeping a
     fact's children contiguous under it. Origin-and-below must be contiguous; above-origin need
     not be. Oracles: `test_sort.test_nested` (cross-fact interleaving — only a global,
     origin-rooted order can produce it) and `test_single_nested`.

   Remaining to implement 3-pre with this model: no-inline arms (each level a full arm); per-arm
   WHERE + `required`/drop-childless (proven above); reassembler rooted at origin with ancestors
   as joins; hierarchical order keys from the origin down. Reverted to step 2 GREEN; this is the
   next build.

   **3-pre LANDED (2026-07-23, commits `7e15e6f` + `ac8772c`; full suite GREEN 373/0/77).** The
   inline first-row join is gone globally. Final shape:
   - **Arms.** Each snowflake level = one UNION-ALL arm. The **origin arm LEFT JOINs** down from
     the fact (a childless parent yields one row = the implicit empty element — Kyle's option 2:
     `missing child == [{}]`, realised by the LEFT JOIN, not a code branch). **Child arms
     INNER JOIN** (emit only real rows; no `order>0` filter — that filter's hidden job of
     suppressing LEFT-JOIN-NULL rows on child arms is now done by INNER). Select condition
     flipped: a column is real on an arm iff it is at that level **or an ancestor** joined there
     (`startswith(arm, col.table)`); descendants/siblings NULL-pad.
   - **Reassembler = streaming hierarchical grouper** (`_accumulate_nested`, rewritten). Rooted at
     the origin (`parent_id_coord=None`). Build each element from its own row (child uids NULL),
     then **dispatch the following same-uid rows to whichever child branch's uid is present** —
     one pass, because ORDER BY keeps each child's rows contiguous. This is what makes **sibling
     arrays** (`{a:[…], b:[…]}` both selected) assemble without one arm eating another's rows.
     TRAP: rows carry **mo_dots `Null`, not `None`** — every uid-presence check uses `is_missing`,
     not `is None` (an `is None` check silently dispatched the wrong sibling and ate its rows).
   - **WHERE is per-arm.** An arm applies the compiled where iff every referenced table is joined
     there (`all(startswith(arm, wt))`), else `WHERE 1`. A where table **below** the origin marks
     its whole branch chain (origin-exclusive down to the where table) `required`; at assembly a
     parent with **no surviving row** in a required child is dropped. Existence is keyed on a
     *dispatched row*, not doc content (`exists a.b` selects no child columns, so the child's
     doc is always empty — only the row's presence signals the match).
   - **Order** left as (explicit sort keys, then every branch uid shallow→deep); the hierarchical
     grouping it provides was already sufficient — no separate piece-4 change was needed.
   - Test change: `test_deep_where_on_fact_table` lost its `expecting_resultset` (it encoded the
     deleted inline row shape; Kyle: raw-row oracles here cause more harm than good).
   - **Known gaps / next:** multi-level required-drop propagation is coded (chain marking) but only
     single-level `where` is exercised by the suite. Nested-origin arms still LEFT JOIN the fact
     ancestor even when unused (harmless). A default `LIMIT` is applied to the UNION *before*
     assembly and can truncate mid-document — pre-existing, not addressed here.
