# Test Triage — the repair campaign

Tests skipped for sqlite (`@skipIf(global_settings.use == "sqlite", ...)` / `@skip(...)`),
grouped by suspected root cause so each work session can pick one cluster, fix it, and check
items off. Update this file as tests are un-skipped or reasons are refined.

Legend: `[ ]` skipped, `[x]` passing (decorator removed), `[-]` won't fix.

> Baseline (dev, 2026-07-29): 414 ran / 0 err / 61 skip. The checklists below are the source of
> truth for what remains; work one cluster per session.
>
> Reconciled 2026-07-29 by a **stale-skip sweep**: strip every sqlite-relevant skip in a
> throwaway worktree, run the suite, diff per-test status against a clean run. 12 skipped tests
> were already passing, and 3 more entries were marked `[ ]` here while running green. Repeat the
> sweep after any cluster lands — a fix in one cluster keeps un-blocking tests filed under
> another. The 62 that still fail are the real remaining work.

## 1. Deep / nested queries (~55 tests — the dominant cluster)

Queries that reach across tables of the snowflake: deep selects, deep wheres, aggs on
parent+child, relative (`..`) names. Almost all of `test_deep_ops.py`. Likely a family of
root causes in multi-table join assembly (`jx_sqlite/edges.py`, `mo_sqlite/sql_script.py`
SqlStep/SqlTree) rather than one bug; expect fixing the first few to reveal the pattern.

### test_deep_ops.py
- [x] test_select_gt_on_sub
- [ ] test_select_in_w_multivalue — multivalue GetOp.to_sql arity (partial_eval/to_sql ordering); order-dependent flake
- [ ] test_select_when_on_multivalue — same multivalue GetOp arity flake
- [x] test_deep_select_column — fixed by nested-origin extraction (empty-parent row kept)
- [ ] test_deep_select_column_w_groupby — groupby header mints `_a..v` (dot doubling in group.py naming)
- [x] test_bad_deep_select_column_w_groupby
- [x] test_abs_shallow_select — fixed: _deep_header lands up-reach (ancestor) columns under their own push name
- [x] test_select_whole_document — fixed (insert row-reuse + plain-`*` depth filter + deep header)
- [x] test_select_whole_nested_document
- [x] test_deep_names_w_star — passing; the skip was already gone when the 2026-07-29 sweep ran
- [x] test_deep_names_select_value
- [x] test_deep_names
- [x] test_deep_agg_on_expression
- [x] test_deep_agg_on_expression_w_shallow_where
- [x] test_agg_w_complicated_where
- [x] test_deep_where_on_fact_table — fixed: explicit deep-leaf select keeps its term-rooted push name (ResolvedName push_child='.'); _accumulate_nested merges one value per child row as a multivalue on the origin doc
- [x] test_deep_where_on_fact_table_multivalue — fixed (step 3, two arms): each deep leaf is its own UNION-ALL arm on the table (place() makes same-table nodes siblings; each deep-leaf candidate is its own subquery-entry branch node), so a._a.v -> list and a._a.s -> False collapse independently. Also fixed the reassembler dropping a collapsed falsy scalar (doc or is_origin -> not is_missing(doc)), and made setop's LIMIT count documents not union rows (SqlOrderByOp is now a first-class command; row-limit dropped, docs sliced at assembly)
- [x] test_deep_where_on_fact_table_subquery — passing (never re-skipped after the two-arms work); a subquery `{from: a._a, select: [v, s]}` as one select element does keep v,s together per element. Kyle: "just-another-element in the select clause"
- [x] test_id_select — passing; GUID `_id` from a nested origin and the empty-parent row both work now
- [x] test_aggs_on_parent
- [x] test_aggs_on_parent_and_child
- [x] test_aggs_on_parent_and_child2
- [x] test_aggs_on_parent_and_child3
- [x] test_deep_edge_using_list
- [x] test_deep_agg_w_deeper_select_relative_name_neop — select name `v.u` was literal_field-escaped
      (`v..u`) by normalize_one for table/cube; edges.py aggregates now unescapes push_column_name
      (matching group.py/setop.py) and format.py unescapes cube data keys at emission
- [x] test_setop_w_deep_select_value_neop
- [x] test_deep_agg_w_deeper_select_relative_name — same fix
- [x] test_shallow_and_ne_deep
- [x] test_setop_w_deep_select_value
- [x] test_select_average_on_none
- [x] test_missing
- [x] test_missing_on_not_exists
- [x] test_exists
- [x] test_deep_or
- [x] test_sibling_nested_column
- [x] test_deep_star
- [ ] test_deep_star_w_parent — needs `..*` (parent-star) relative names
- [x] test_deep_select_dot
- [ ] test_from_shallow_select_deep_column
- [x] test_setop_w_shallow_eq_string
- [ ] test_deep_edge_w_shallow_expression
- [ ] test_deep_edge_w_shallow_var
- [ ] test_nested_property_edge_w_shallow_expression
- [ ] test_nested_document_selection — select of literal nested-doc list: 'Expecting an expression, not [{...'
- [x] test_nested_filter_with_groupby
- [ ] test_deep_origin_agg_on_child

### test_nested.py
- [ ] TestNestedQueries (whole class) — "broken"

### test_sort.py (nested subset)
- [x] test_nested_array
- [x] test_nested
- [x] test_single_nested

### reclustered from cluster 9 (2026-07-10)
- [x] test_expressions_w_set_ops.py::test_select_average
- [x] test_expressions_w_set_ops.py::test_select_average_on_none
- [x] test_filters.py::test_regexp_expression

### test_set_ops.py (deep subset)
- [x] test_single_deep_select
- [x] test_select_w_deep_star
- [x] test_select_w_nested_values — deep (fact→_a→k) whole-doc reconstruction; table/cube fixed by
      the branch-valued select work below (the '.'+'_a' pair was the child branch re-interpreting
      the term name).  History: fixed the crash first —
      SelectOp.to_sql built a below-origin leaf's push name via `relative_field(concat_field(name,
      rel_name), branch_prefix)`, but `rel_name` from `schema.leaves` is already branch-relative, so a
      real prefix (`_a.k`) manufactured an up-ref (`...b`) that JxType rejects ("not allowed"). Now
      `concat_field(name, rel_name)` — a no-op when branch_prefix=="." (the only case any passing test
      hit, so regression-free) and correct for deep branches. The test's own table/cube expectation
      was also wrong (it wanted a single '.' column) and has been corrected to `["_a"]`: no select means the implied
      name '.', which declares the *top-level properties* as columns — same rule that makes
      test_single_no_select `["a"]` and test_select_whole_document `["o","_a","c"]`. A literal '.'
      column only comes from a select whose value is '.'. So index_to_columns' push_column_name
      assignment should drop the '.' column, not merge into it. Doc: docs/jx_expressions_leaves.md
      §Table format / No select clause; the star-vs-no-select difference is pinned by
      test_no_select_w_inner_object + test_star_select_w_inner_object (both pass). That's the
      column-mapping/push-name layer (setop/ColumnMapping / Names), a separate deeper step.
- [ ] test_prefix_in_deep_where_clause — "fix me"
- [ ] test_exists_in_where_clause — "fix me"
- [ ] test_select_into_children — "Too complicated"

### branch-valued select terms (2026-07-29) — test_select_array_as_value, test_select_id_and_source, test_select_w_nested_values

A select term whose value NAMES A BRANCH (`select "_a"`, `{"name":"_source","value":"."}`) is a
subquery per table the name reaches, and **Names already says where each binding lands**:
`push_name` is the path from the queried name to the array holding the binding, `push_child` its
path inside that array's element (docs/NAMES.md). So the branch's assembled value lands at
`concat(term name, push_name)` and its columns are named `push_child`.

That split replaced two heuristics in `setop.py`:

- `_deep_split` (one table or nothing) → `_branch_split` (group the bindings by table). A name
  spanning the origin *and* a child (`.` over a doc with a nested array) now hives off the child
  part and keeps its plain term for the rest — that term used to be re-compiled at the child
  branch under the wrong name (`_source.b` landing at `a`, giving `a:[{_source:{b}}]}`).
- "a branch of ONE column collapses to a bare multivalue" → `push_child == "."` collapses. Same
  answer for `a._a.v` (the queried name *is* the value), different — and now right — for a
  one-leaf array of objects: `select "a"` over `[{"b":1}]` keeps `[{b:1}]` instead of `[1]`.

A node's push name became an **absolute** (fact-rooted) path, defaulting to the table's own path;
assembly lands a child at `relative_field(child, parent)`. That is what plain document assembly
always computed from table paths — now a select term can override it, including with `"."` (the
branch *is* the doc: `select "_a"` in list format).

### slots — one table's rows answering several terms (2026-07-29)

The above left one limit: a term renaming a branch **across an intermediate array** (`select "_a"`
where `_a` holds another array) could not apply the rename, because only the deepest node was
renamed and the intermediate node belonged to the snowflake. Its two arms then disagreed about the
coordinate system — `{"v":9,"_a":{"k":[…]}}` for data `{"_a":[{"v":9,"k":[{"b":1},{"b":2}]}]}`.

Fixed by naming the thing that was missing: a **slot** is one term's answer from one table —
`DocumentDetails.slot_path` (slot → absolute path where its doc lands) and `slot_columns`
(slot → its columns), keyed by the term's name, `None` being the document itself. So:

- `_accumulate_nested` builds one doc per **(element, slot)** instead of one per element, and lands
  each at `relative_field(child.slot_path[slot], parent.slot_path[slot])`. A slot the parent does
  not answer (its own part was shallow, like `_source`'s `v`) lands in the parent's document.
- A branch term claims a slot at **every level** from below the origin down to its binding table,
  so the rename reaches the levels beneath it. `select "_a"` now gives `[{"v":9,"k":[…]}]`, and
  `["o",{"name":"x","value":"_a"}]` gives `{"o":1,"x":{"k":[…]}}`.
- One node per table again: same-table siblings collapse, so `place()` needs no branch identity and
  the "two arms" of step 3 become two slots on the same rows — `a._a.v` → list and `a._a.s` →
  False still collapse independently, in **2 UNION-ALL arms instead of 4**. Slots subsume the
  two-arms mechanism; the fork is in the pull plan, not the SQL.

Not a node per term (a wrapper per instance) — two side tables keyed by slot.

**Still open, unrelated to slots**: `format.py` derives a table/cube header from the term's *var*,
not its name, so `{"select":{"name":"x","value":"_a"},"format":"table"}` emits header `_a` and a
Null cell. Broken identically before this work; no test covers it.

`Variable.to_sql`'s `logger.warning("not expected")` branch is no longer reached from setop (it
built a SELECT whose FROM was the table name reversed character-by-character); it is still live
for other callers.

(This left one limit — a rename across an intermediate array — fixed by slots, below.)

## 2. Selecting objects / stars / leaves (setop formatting) — CLEARED (10 fixed, 1 reclustered)

Shallow queries whose select clause is an object, `*`, leaves, or an array value
(`jx_sqlite/format.py` result-shaping + jx_base select normalization; see vendor/jx_base/BUGS.md #5).

### test_set_ops.py
- [x] test_select_w_star
- [x] test_select_expression — header ordering (format_deep)
- [x] test_select_object
- [x] test_select_leaves — `a.*` wildcard expansion
- [x] test_select_leaves2 — `a*` flatten-to-dotted-keys (LeavesOp prefix)
- [x] test_select_value_object
- [x] test_select2_object
- [x] test_select3_object
- [x] test_select_array_as_value — was filed as a hidden-column leak; the real defect was
      branch-valued select terms (see cluster 1 §branch-valued select terms)
- [-] test_union_columns — MISFILED: UnionOp not registered in JxSql (no `.to_sql`) →
      missing operator, see cluster 6 test_union; re-skipped
- [x] test_select_id_and_source — same: `select "."` beside another term is a branch-valued
      term, not a hidden-column leak

## 3. `between` op broken — CLEARED except test_between

BetweenOp (string-slicing between, and edge domains using between). Only the edge-domain half
was ever broken on sqlite; the 2026-07-29 sweep found the rest passing behind bare
`@skip("between is broken")` decorators, now narrowed to python/interpret.
- [x] test_edge_1.py::test_edge_using_between
- [x] test_edge_2.py::test_edge_using_missing_between1
- [x] test_edge_2.py::test_edge_using_missing_between2
- [x] test_expressions_w_set_ops.py::test_between_missing
- [ ] test_expressions_w_set_ops.py::test_between — "parser stack overflow"
- [x] test_edge_1.py (edge_1 bare `@skip("between is broken")`) — narrowed to python/interpret, which are still unverified

## 4. Sort coordinated with edges / groupby (~7 tests)

"coordinate sort clause with matching edges" — the ORDER BY must be expressed in terms of
the edge/groupby output columns. Related to the uncommitted `jx_base/expressions/sort_op.py`
change.
- [x] test_sort.py::test_edge_and_sort
- [ ] test_sort.py::test_2edge_and_sort
- [x] test_sort.py::test_groupby_and_sort
- [x] test_sort.py::test_groupby_expression_and_sort
- [x] test_sort.py::test_groupby2a_and_sort
- [ ] test_sort.py::test_groupby2b_and_sort
- [ ] test_sort.py::test_groupby2c_and_sort
- [x] test_edge_time.py::test_count_over_time_w_sort

## 5. Statistical aggregates (percentile/median) (~5 tests)

- [ ] test_agg_ops.py::test_median — _percentile (PercentilesOp) not implemented
- [ ] test_agg_ops.py::test_percentile — _percentile (PercentileOp) not implemented
- [ ] test_agg_ops.py::test_both_percentile — _percentile not implemented
- [x] test_agg_ops.py::test_stats — StatsOp computes stats in plain SQLite SQL
      (population variance via SUM/COUNT, std via SQRT); median dropped from the stats object
- [ ] test_agg_ops.py::test_median_on_value — _percentile not implemented (aggregate median on `.`)
- [ ] test_edge_1.py::test_percentile — _percentile not implemented

## 6. Other aggregate ops (~4 tests)
- [x] test_agg_ops.py::test_select_agg_mult_w_when
- [x] test_agg_ops.py::test_max_on_tuple — passing
- [x] test_agg_ops.py::test_max_on_tuple2 — passing
- [x] test_agg_ops.py::test_union — UnionOp needed the `frum=` aggregate ctor (like
      SumOp/MinOp) and `_union_aggregate` used the dead multi-detail `to_sql` API.
      NOTE: only the sqlite path is covered; jx_python union test/interp request lives in
      vendor/jx_python/BUGS.md #3. Nested/multi-value union: see cluster 7 test_union_*.

## 7. Edge domains (~7 tests)

- [x] test_edge_1.py::test_union_values
- [x] test_edge_1.py::test_union_nested_objects
- [x] test_edge_1.py::test_multiple_union
- [ ] test_edge_1.py::test_multiple_union2 — same
- [ ] test_edge_1.py::test_empty_default_domain_w_groupby — "broken"
- [x] test_edge_1.py::test_edge_using_tuple
- [ ] test_edge_1.py::test_shallow_with_deep_edge — "not sure what first() of nested column
      would be; requires schema merging of a.b.~n~ and a.~a~.b.~n~" (design question)

## 8. Groupby advanced (~7 tests)
- [ ] test_groupby_1.py::test_groupby_left_id — "broken"
- [ ] test_groupby_1.py::test_count_values — "requires subqueries"
- [ ] test_groupby_1.py::test_groupby_multivalue_nested — "for coverage"
- [ ] test_groupby_1.py::test_groupby_object — "broken"
- [x] test_groupby_1.py::test_groupby_star
- [x] test_groupby_1.py::test_groupby_object_star
- [ ] test_groupby_1.py::test_groupby_multivalue_naive — "requires groupby sets"

## 9. Scalar expressions (~9 tests) — CLEARED (6 fixed, 3 reclustered)
- [x] test_filters.py::test_where_expression
- [x] test_filters.py::test_add_expression
- [-] test_filters.py::test_regexp_expression — MISFILED: `select *` from nested table
      (returns parent doc w hidden cols) → cluster 1/2
- [x] test_expressions_w_set_ops.py::test_length
- [x] test_expressions_w_set_ops.py::test_select_mult_w_when — ToBooleanOp typing + "mult"→MulOp
      (vendor/jx_base/BUGS.md #4)
- [-] test_expressions_w_set_ops.py::test_select_average — MISFILED: edges on deep table → cluster 1
- [-] test_expressions_w_set_ops.py::test_select_average_on_none — same → cluster 1
- [x] test_expressions_w_set_ops.py::test_left_w_find
- [x] test_expressions_w_set_ops.py::test_not_left

## 10. Metadata (~3 tests)
- [ ] test_metadata.py::test_meta_tables — "broken"
- [ ] test_metadata.py::test_get_nested_columns — "broken"
- [ ] test_metadata.py::test_cardinality — "cardinality not tracked" (feature gap)

## 11. Schema merging (4 of 10 pass — the class-level skip was hiding them)

The class-level `@skipIf(... "not ready")` is gone; the six that still fail carry their own
reason. The failures are one theme: a name that exists in more than one shape (scalar / inner
object / nested array) resolves to *one* of them instead of the union.
- [x] test_schema_merging.py::test_mixed_primitives
- [x] test_schema_merging.py::test_dots_in_property_names2
- [x] test_schema_merging.py::test_sum
- [x] test_schema_merging.py::test_where — the "complicated where clause" works now
- [ ] test_schema_merging.py::test_select — "broken"
- [ ] test_schema_merging.py::test_select2 — merged schema does not expose the deep leaf
      (`a.b` not found in `[a]`)
- [ ] test_schema_merging.py::test_count — counts one shape only (1, want 6)
- [ ] test_schema_merging.py::test_dots_in_property_names — picks the wrong column
      (`world`, want `hello`) for `a..html` when both `a.html` (a literal dotted name) and
      `a: {html}` exist
- [ ] test_schema_merging.py::test_dots_in_property_names3 — "broken"
- [ ] test_schema_merging.py::test_edge — sum over an edge on a merged inner/nested column adds
      the parent value once per child row (b=2 → 8, want 4)

## 12. Joins (feature not implemented)
- [ ] test_joins.py::test_left_join
- [ ] test_joins.py::test_subtraction

## 13. Probably won't fix / not sqlite-relevant
- [-] test_others.py::TestOther — "meant for html endpoint (ES)"
- [-] test_set_ops.py::test_max_limit — "no need for limit when using own resources"
- [-] test_query_normalization.py::test_naming_select — "test is still unclear"

## 14. Filter ops (test_filters.py — inbound from svn 2026-07-27, WHERE-clause operators)

A new shared conformance module arrived via svn (`cfc8b08`) exercising operators inside
`where`. It also brought the `ExpOp`→`PowOp` rename (jx_base/jx_python) that broke every
jx_sqlite import until mirrored. Type-suffix reference for the remaining work: a value's typed
columns are `x.$B` (bool) / `x.$I` (int) / `x.$N` (number) / `x.$S` (string); a Variable's
`to_sql` COALESCEs across them (jx_type `JX_ANY`), so a type predicate must resolve the *one*
typed leaf, not the coalesced value.

- [x] test_where_pow / test_where_power_alias — `pow`/`power` → `PowOp`; SQLite has no `**`, use
  `POWER(base,exp)` (the old `exp`/`**` `_sql_operators` entry was dead: unparseable + wrong unpack).
- [x] test_where_mod{,_negative_dividend,_zero} — jx `mod` follows the *divisor* sign
  (`-7 mod 3 = 2`, like Python); SQLite `%` follows the dividend. `((x%y)+y)%y` normalizes.
- [x] test_where_max — the aggregate form `(SELECT MAX(c) FROM (SELECT (a) AS c UNION ALL SELECT (b)
  AS c))` was right; the `near ","` that made the first attempt look wrong was **not** this form.
  `SqlScript.__iter__` silently yielded *nothing* past 100 stack frames (`logger.alert("stack
  overflow?"); return`), so the script vanished mid-expression and SQLite saw `COALESCE( , 0)`; the
  subquery just added the frames that tripped it, and whether it tripped depended on how deep the
  interpreter already was (`python -m unittest` failed, a plain script passed). Guard removed —
  runaway recursion still raises RecursionError. `vendor/mo_sqlite/BUGS.md` (the twin guard in
  `__eq__` is still there). The decisive form is now per-op data (`_decisive_forms`) instead of
  assuming every multi-op has an identity to COALESCE to.
- [x] test_where_is_number / test_where_is_integer / test_where_is_boolean — type predicates now
  resolve the *one* typed leaf ($N/$B), not the COALESCE'd union. New `variable.typed_leaf(term,
  schema, target_types)` filters `schema.leaves` by es_type and returns just that column (NULL if
  absent) — this is the per-leaf resolution the old `value.jx_type == JX_NUMBER` (always `JX_ANY` for
  a union) could not do. Integers store as `$N` (REAL, no `$I` column), so is_integer takes the `$N`
  leaf and nulls fractionals (`CAST(n AS INTEGER)=n`). The load-bearing insight: the WHERE wraps every
  predicate in `ToBooleanOp`, whose old fallback used schema-agnostic `term.exists()`/`.missing()` —
  collapsing the narrowing back to the whole union. `ToBooleanOp` now keys off the *rendered* null-safe
  value (`NOT (<sql> IS NULL)`), identical to exists() for ordinary terms but honouring a to_sql that
  nulled a present-but-out-of-class row. (`is_number(x)==5` still returns the value, so eq works.)
- [x] test_where_to_integer / test_where_to_text / test_where_number_coercion — coercion semantics
  (integer truncates 2.9→2; text renders whole float 2.0→"2"; number parses "5"→5, "x"→null). Root fix
  was in the pure-SQL layer: `quote_value` rendered a numeric-looking *string* ("2", "0") as a bare
  number, so `text(x) == "2"` compiled to `'2' = 2` (false in SQLite) and RTRIM's strip-char arg was
  unquoted; a Python `str` now always quotes. `ToIntegerOp` now always CASTs (it only cast text before,
  so a float never truncated); `ToNumberOp` wrapped a whole SqlScript in SqlCastOp (→ `KeyError:
  'partial_eval'`), now parses via a GLOB numeric-string guard (`"x"→null`, not 0).
- [x] test_where_count_collection / test_where_cardinality_collection — count / distinct-count over a
  *nested array* referenced from the fact WHERE. New `ToListOp` (`jx_sqlite/expressions/to_list_op.py`,
  Kyle's name): the one-column relation behind a collection, from either N scalar expressions
  (UNION ALL) or the child rows of a multi-valued column (`SELECT c FROM child AS __list__ WHERE
  __list__.__parent__ = origin.__id__`). Every collection op is then one aggregate over it —
  count/cardinality (COUNT, COUNT DISTINCT) and decisive max/min (MAX, MIN) share the same code, and
  the null-skipping comes free because SQL aggregates ignore NULL rows. It is deliberately *not* a
  registered language op: it is relation-valued, and SqlScript can only carry a scalar.

- [x] `join_vars()` — the distinction the above needed, now built. `vars()` answers "which fields
  does this read"; setop was using it for a different question, "which tables must be joined", and a
  collection aggregate reads a nested column through its own FROM. `Expression.join_vars()` defaults
  to `vars()` (joining more than needed is slower, never wrong, so un-overridden ops keep today's
  behaviour); `CountOp`/`CardinalityOp` return empty; `BaseMultiOp`/`BaseBinaryOp`/`NotOp` forward to
  their children — **the forwarding is load-bearing**, without it the default `self.vars()` at the
  top swallows the override below, since `vars()` recursion never re-enters `join_vars()`. setop's
  `referenced_paths` and `where_tables` now read it. Fixes `{"count":"arr"} == 0` against a childless
  document (was: pushed to the child arm, branch marked `required`, parent dropped before the
  predicate ran); pinned by `test_where_count_empty_collection`.

  The weak spot: a second traversal parallel to `vars()`, whose payoff depends on every composite op
  remembering to forward. A missed forward is silent — it just reverts to the pessimistic join.
- [x] test_expressions_w_set_ops.py::test_select_count_of_collection, plus the new pair
  test_count_as_aggregate / test_count_as_expression — **an aggregate is a declaration, not an
  operator.** `{"value":"a","aggregate":"count"}` collapses over the rows of the `from`;
  `{"value":{"count":"a"}}` is an expression of one document. Both build a `CountOp`, and
  `SelectOne` was storing only that — `.aggregate` re-derived the declaration from the expression's
  *class* (`canonical_aggregates`), so the two spellings were one object and every collection op in
  a value position was read as a collapse (`query.py:128` then picks the query strategy, i.e. the
  result *shape*, from that). `SelectOne` now records the declaration (`_aggregate`, set by the
  constructor or `SelectOne.aggregated`); `.value`/`.default` read it instead of guessing.
  `setop.py` select-var resolution also moved to `join_vars` — otherwise the assembler builds a
  child branch for the aggregate's own nested column and tries to hang child docs off the scalar it
  produced. Multi-term `count` reaching `TallyOp` (not in `canonical_aggregates`) was the same
  distinction smuggled through arity; it is no longer load-bearing.

  Newly *visible* (not new): with the op form no longer reinterpreted as a collapse,
  `{"value":{"sum":"a"}}` and `{"value":{"union":[...]}}` now error with "no attribute to_sql" —
  `SumOp`/`UnionOp` have no JxSql expression implementation. They used to be silently routed to the
  aggregate path instead. `test_union_columns` still needs `UnionOp.to_sql` (cluster 6), but its
  routing is fixed. The `sql_aggs["null"]` KeyError for a mixed select is gone for ops that *do*
  have an implementation (count/cardinality both work beside a plain column now).

- [x] test_select_{sum,max,min,average}_of_collection — **the rest of the collection family on
  ToListOp.** `sum`/`max`/`min` had a `to_sql` from an earlier era (`SUM(self.term)`, `MAX(frum)`)
  that could only ever have worked in a GROUP BY; in expression position they crashed (`self.term`
  is `None`; `MAX(arr)` names a column that does not exist for a nested array). All four are now
  `ToListOp(self.frum).aggregate(AGG, schema)`, like count/cardinality. `avg` needed a jx_base op
  at all (`AvgOp` was a two-line stub with no `vars`/`__data__`/`partial_eval`) and the `average`
  and `mean` spellings needed to reach it (`operators` had only `avg`). Each also needs
  `join_vars() == set()` — same reason as CountOp: the aggregate resolves its nested column
  through its own FROM, and a branch for it makes the assembler hang child docs off the scalar
  ('float' object does not support item assignment).

  Two jx_base `missing()` overrides were dead on arrival (`SumOp` had no `return`; `MaxOp` called
  an undefined `Missing`) and `MinOp` claimed `FALSE` — a lie, min of an empty collection is null.
  All three are deleted, so the family inherits the default `MissingOp(self)`: *this value is
  missing exactly when it is null*, which is what a SQL aggregate does.

  That default was unusable before: `SqlScript` renders a non-FALSE miss as
  `CASE WHEN NOT (miss) THEN expr END`, and rendering `MissingOp(self)` renders `expr` again,
  whose miss is the same MissingOp — infinite recursion. The wrapper is provably a no-op there
  (`CASE WHEN NOT (expr IS NULL) THEN expr END == expr`), so `SqlScript._is_self_missing()` now
  skips it. It replaces a dead branch that tested the same shape but only for a Variable — dead
  because the line above it already returns for every Variable. Not applied to text, where
  MissingOp also counts `''` and the wrapper does change the value. Cost: one `Expression.__eq__`
  per aggregate script (logs "this is slow on SumOp"), because `partial_eval` rebuilds the op, so
  the cheap identity test does not hold.

  Left out: `product` (no native SQLite aggregate — `EXP(SUM(LN(x)))` breaks on 0 and negatives,
  so it needs a different shape), and `union`, which is set-valued (cluster 6). jx_base
  `ProductOp` is a copy of `SumOp` down to `__data__` returning `{"sum": ...}` and `__call__`
  calling `sum()` — fix those when product gets its SQL.

## Suggested order of attack

1. **Cluster 1** — the big one and the remaining bulk; multi-table join assembly in edges.py.
   Treat edges.py as scaffolding to decompose (see INTERSECTION_SURVEY / Names), not to extend.
2. **Cluster 4** — sort/edges coordination.
3. Clusters 3, 6, 7, 8 as they come.
4. Clusters 5, 10, 11, 12 are feature work, not repairs — schedule deliberately.
(Clusters 2 and 9 are cleared.)

## Open design questions (from commits/tests, in Kyle's head)

- How to combine (query, namespace, language)? — commit 7aafd1d/37768d8 says "missing
  namespace right now". Where does the namespace get attached to compilation?
- What are the intermediate operators between JX edges and SQL? edges.py must be broken up
  into component operators, but the higher-level ops are not yet known. Each language is
  simple on its own; the translation zone is the complexity. **See
  `docs/INTERSECTION_SURVEY.md`** — a side-by-side survey of all existing attacks on this
  layer, with an induced candidate operator set. Cluster 1 work should treat edges.py as
  scaffolding to decompose, not architecture to extend.
- `first()` of a nested column (test_shallow_with_deep_edge) — requires schema merging of
  `a.b.~n~` and `a.~a~.b.~n~`; semantics undecided.
- Copy vs don't-copy `a.*` columns into new child table on nesting (docs/The Future.md).
- Arrays of arrays (docs/The Future.md).
