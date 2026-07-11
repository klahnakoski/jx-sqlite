# Test Triage — the repair campaign

122 tests are skipped for sqlite (extracted from `@skipIf(global_settings.use == "sqlite", ...)`
and `@skip(...)` decorators, 2026-07-07). This document groups them by suspected root cause so
each work session can pick one cluster, fix it, and check items off. Update this file as tests
are un-skipped or reasons are refined.

Legend: `[ ]` skipped, `[x]` passing (decorator removed), `[-]` won't fix.

> BASELINE 2026-07-10 (after svn sync of upstream jx_python/jx_base/mo_json fixes, then
> same-day repairs): 351 tests, 201 pass, 3 errors, 147 skipped. The former 46-error NullOp
> cluster passes because upstream `get_schema_from_list` now treats a leaked jx NULL as
> missing — the jx-sqlite edge-query leak is masked, not root-caused. Fixed today:
> tuple-of-literals eq/in (jx_base `is_expression` fooled by FlatList); test_left/test_string
> (setop wrapped whole-value scalars in Data); test_id_select ×2, test_time_expression,
> test_empty_default_domain (jx.sort rewritten: sorts without Container.create/schema
> inference — rows may hold opaque values — and returns a plain list; harness untouched,
> see vendor/jx_python/BUGS.md). test_complex_edge_value: import restored to
> query_op._normalize_edges (37768d8 had pointed it at the unfinished edges_op).
> test_meta: query_metadata updated to the restored QueryOp.wrap(query, container, lang)
> signature. **Suite green: 0 errors.** Next work: the 147 skips (see clusters below).
>
> RESOLVED 2026-07-10 (was: "root-cause the masked NullOp leak in edge queries"): traced —
> there is no leak. Instrumented get_schema_from_list's JX_IS_NULL guard and scanned every
> result payload across the full suite: no NullOp ever comes out of jx-sqlite. The NullOp
> objects are in the tests' *expected* data (388 uses of `NULL`), by design: assertAlmostEqual
> matches subsets, so `NULL` is the only way to assert a property is missing. The 46 errors
> arose when the harness's order-normalization sort (old jx.sort → Container.create →
> get_schema_from_list) ran schema inference over that expectation data and choked on NullOp.
> Both fixes are correct, not masks: the upstream guard (nulls don't exist in JX schemas)
> and the jx.sort rewrite (sorting needs no schema). No jx-sqlite work remains here.

## 1. Deep / nested queries (~55 tests — the dominant cluster)

Queries that reach across tables of the snowflake: deep selects, deep wheres, aggs on
parent+child, relative (`..`) names. Almost all of `test_deep_ops.py`. Likely a family of
root causes in multi-table join assembly (`jx_sqlite/edges.py`, `mo_sqlite/sql_script.py`
SqlStep/SqlTree) rather than one bug; expect fixing the first few to reveal the pattern.

> 2026-07-11: the **name-resolution blocker is cleared**. The old `Schema.leaves()` mixed
> typed/untyped namespaces and (for `leaves(".")`) leaked hidden cols `__id__/__order__/
> __parent__` and doubled deep names (`_a._a.b`) — visible in whole-document / deep-perspective
> selects. `Schema.leaves()` now delegates to the resurrected **Names** binder
> (`vendor/jx_base/models/names.py`, `vendor/mo_sqlite/models/names.py`); all six
> `tests/test_leaves.py` contract cases pass. Treat Names as the P0 under the
> INTERSECTION_SURVEY operators; see `docs/NAMES.md` next-steps (esp. #4).
>
> 2026-07-11 (later): **fact-perspective `select *` fixed** — `test_select_whole_document`
> un-skipped and passing (all three formats), suite still green (366 ran, 128 skip). Three
> root causes, three targeted fixes:
> (a) `vendor/mo_sqlite/models/insert.py`: an inner object `{"b":..,"v":..}` (leaves() descends
>     into objects, NOT arrays) arrived as separate flattened leaves, each spawning its own
>     nested row → one doc split per typed column. The `len(nested_path) >` branch now REUSES
>     the row already made for this (parent, order) instead of appending per leaf.
> (b) `jx_sqlite/expressions/select_op.py`: plain `*` (LeavesOp, no prefix) is document
>     assembly — a nested array is itself one leaf-value — so it no longer descends into child
>     tables (`len(col.nested_path) > origin_depth`). Explicit `a.*` (prefix) still keeps deep
>     leaves.
> (c) `jx_sqlite/format.py`: `_deep_header`/table+cube projection built headers from each
>     column's leaf `push_column_name`, promoting nested `b`/`v` to top-level; now uses the
>     top-level container name relative to the query origin (`_top_name`), so `_a` is one column.
> **Still deep-perspective (`from testing._a select *`) does NOT switch origin** — returns the
> fact view; and nested-origin `*` never pulls ancestor scalars (`o`/`x`). That is a distinct
> root cause (leaves() from a child origin must return parents; query origin/relativization not
> applied) blocking `test_select_whole_nested_document`, `test_deep_star`, `test_deep_star_w_parent`,
> `test_deep_select_dot`, etc. — the next cluster-1 target.
>
> 2026-07-11 (evening): **the origin switch is FIXED — one line.** `from testing._a` never
> switched perspective because `meta_columns.py get_nested_path("testing._a")` compared the
> UNTYPED request against TYPED query paths (`testing._a.$A`): `startswith_field` matched only
> the fact, so every Table got `nested_path=['testing']`. Now compares both in the fixed
> (untyped) space. **19 tests un-skipped** (13 deep_ops, test_sort test_nested/
> test_single_nested/test_edge_and_sort, test_set_ops test_single_deep_select/
> test_select_w_deep_star, test_filters test_regexp_expression); suite 366 ran, 109 skips.
> Remaining cluster-1 failures regrouped by verified error (all 42 deep_ops skips were run):
> - **[12] "ambiguous column name: __id__"** — ALL deep edges/agg queries (aggs_on_parent*,
>   deep_agg*, deep_edge_w_shallow_var, deep_select_column, select_average_on_none):
>   edges.py join assembly emits unqualified `__id__`. THE next target; one shared cause.
> - **[6] nested-origin `*` misses ancestor scalars** (deep_star*, deep_names_w_star,
>   select_whole_nested_document, agg_w_complicated_where, deep_where_on_fact_table):
>   `leaves(".")` scope-granular rule stops at origin scope; `select *` needs parent scalars too.
> - **[2] "expecting miss to not be missing"** (deep_edge/nested_property_edge_w_shallow_expression)
> - singles: deep_select_dot + abs_shallow_select (set mismatch), select_in_w_multivalue
>   (GetOp.to_sql missing schema), select_when_on_multivalue (`testing.testing.a.$A.$S` —
>   doubled path), from_shallow_select_deep_column (float item assignment), id_select,
>   nested_document_selection, nested_filter_with_groupby, deep_select_column_w_groupby.
> TestNestedQueries (10 tests): all still fail — mostly deep aggs, likely the `__id__` cause.
>
> 2026-07-11 (night): **edges/groupby one-scope rework — 16 more un-skipped** (suite 366 ran,
> 93 skips). Two changes, both in the P1/P2 zone of `docs/INTERSECTION_SURVEY.md`:
> 1. `meta_columns.py find(fact)` gathered a snowflake's columns by table-name prefix; after
>    `rename_tables` to opaque aliases (`__t0__`) the prefixes share nothing, so the renamed
>    schema LOST every child-table column (edge values compiled to literal NULL). `find` now
>    consults `_snowflakes` (membership is data, not name algebra), prefix scan as fallback.
> 2. `edges.py`/`group.py` stopped renaming tables at all: `sql_join_chain` now aliases every
>    table AS ITSELF (setop precedent — Names/leaves algebra keeps working), joins the spanning
>    tree covering origin ancestors ∪ tables the query mentions (fan-out only when reached
>    into), and edges.py inlines the join chain instead of wrapping a facts subquery — the
>    facts subquery's bare-column select list was the literal "ambiguous __id__", and its alias
>    trick only ever worked for fact-table references. `__exists__` replaced by
>    COUNT(origin.__id__). rename_tables/table_alias are now unused by live code.
> Un-skipped: deep_ops aggs_on_parent, deep_agg_on_expression ×2, deep_edge_using_list,
> select_average_on_none, nested_filter_with_groupby; expressions_w_set_ops select_average ×2;
> sort groupby_and_sort/groupby_expression_and_sort/groupby2a_and_sort (cluster 4);
> groupby_1 groupby_star/groupby_object_star (cluster 8); edge_1 edge_using_tuple (cluster 7);
> edge_2 edge_using_missing_between1 (cluster 3); agg_ops select_agg_mult_w_when (cluster 6).
> (both_percentile looked fixed but was a false positive - its no-space skipIf dodged the
> decorator scan; verified individually, still failing.)
>
> 2026-07-11 (night, cont.): **count default — 4 more** (suite 366 ran, 89 skips). The
> "aggs_on_parent_and_child ×3 need a dedupe rule" guess was WRONG: from the deep origin there
> is no fan-out; the only defect was that empty coordinates returned missing instead of 0.
> edges.py standard-aggregates now defaults CountOp to ZERO (count of nothing is 0, never null
> — decisive count; mirrors the count-records branch). Un-skipped: aggs_on_parent_and_child
> ×3, test_edge_time::test_count_over_time_w_sort (cluster 4 now FULLY cleared except
> 2edge/2b/2c). Full both-spellings skip sweep confirms: no other currently-skipped test
> passes; remaining 89 skips all genuinely fail.
> Remaining deep_ops failures (23) regroup as: aggs_on_parent_and_child ×3 (parent+child in one
> query — needs the ORDER>0/DISTINCT dedupe rule edges never learned), the nested-origin-`*`
> group (unchanged), deep_agg_w_deeper_select_relative_name ×2 (`..` names), and the singles.

### test_deep_ops.py
- [x] test_select_gt_on_sub
- [ ] test_select_in_w_multivalue
- [ ] test_select_when_on_multivalue
- [ ] test_deep_select_column
- [ ] test_deep_select_column_w_groupby
- [x] test_bad_deep_select_column_w_groupby
- [ ] test_abs_shallow_select
- [x] test_select_whole_document — fixed (insert row-reuse + plain-`*` depth filter + deep header)
- [x] test_select_whole_nested_document
- [ ] test_deep_names_w_star — prefix-star on fact-absolute name from deep origin loses the container name
- [x] test_deep_names_select_value
- [x] test_deep_names
- [x] test_deep_agg_on_expression
- [x] test_deep_agg_on_expression_w_shallow_where
- [x] test_agg_w_complicated_where
- [ ] test_deep_where_on_fact_table — where {exists: deep.column} from fact origin returns no rows
- [ ] test_id_select
- [x] test_aggs_on_parent
- [x] test_aggs_on_parent_and_child
- [x] test_aggs_on_parent_and_child2
- [x] test_aggs_on_parent_and_child3
- [x] test_deep_edge_using_list
- [ ] test_deep_agg_w_deeper_select_relative_name_neop
- [x] test_setop_w_deep_select_value_neop
- [ ] test_deep_agg_w_deeper_select_relative_name
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
- [ ] test_deep_select_dot
- [ ] test_from_shallow_select_deep_column
- [x] test_setop_w_shallow_eq_string
- [ ] test_deep_edge_w_shallow_expression
- [ ] test_deep_edge_w_shallow_var
- [ ] test_nested_property_edge_w_shallow_expression
- [ ] test_nested_document_selection
- [x] test_nested_filter_with_groupby

### test_nested.py
- [ ] TestNestedQueries (whole class) — "broken"

### test_sort.py (nested subset)
- [ ] test_nested_array
- [x] test_nested
- [x] test_single_nested

### reclustered from cluster 9 (2026-07-10)
- [x] test_expressions_w_set_ops.py::test_select_average
- [x] test_expressions_w_set_ops.py::test_select_average_on_none
- [x] test_filters.py::test_regexp_expression

### test_set_ops.py (deep subset)
- [x] test_single_deep_select
- [x] test_select_w_deep_star
- [ ] test_select_w_nested_values — "fix me first"
- [ ] test_prefix_in_deep_where_clause — "fix me"
- [ ] test_exists_in_where_clause — "fix me"
- [ ] test_select_into_children — "Too complicated"

## 2. Selecting objects / stars / leaves (setop formatting) — CLEARED 2026-07-10 (8 fixed, 3 reclustered)

Shallow queries whose select clause is an object, `*`, leaves, or an array value.
`jx_sqlite/format.py` (result-shaping) + jx_base select normalization. Two real bugs fixed:
(1) `format_deep` alphabetically **sorted** the table/cube header (`jx.sort(set(push_column_name))`),
discarding select-clause order — now ordered by `push_column_index` (see `_deep_header`).
(2) jx_base `select_op.normalize_one`: bare-string selects never reached the `.*`/`*` wildcard
branches (value was eagerly parsed to a GetOp before `is_text(value)`), the `.*` branch had a
`root_nam` NameError, and `is_variable` was unimported — so `a.*`/`a*` returned all nulls.
Fixed + `jx_sqlite/expressions/select_op.py` LeavesOp branch now honors `expr.prefix` so
`a*` flattens to literal dotted keys `a.b`/`a.v` (vendor/jx_base/BUGS.md #5).

### test_set_ops.py
- [x] test_select_w_star
- [x] test_select_expression — header ordering (format_deep)
- [x] test_select_object
- [x] test_select_leaves — `a.*` wildcard expansion
- [x] test_select_leaves2 — `a*` flatten-to-dotted-keys (LeavesOp prefix)
- [x] test_select_value_object
- [x] test_select2_object
- [x] test_select3_object
- [-] test_select_array_as_value — MISFILED: nested array as value leaks hidden cols
      (`__id__`/`__order__`/`__parent__`) → cluster 1 join assembly; re-skipped
- [-] test_union_columns — MISFILED: UnionOp not registered in JxSql (no `.to_sql`) →
      missing operator, see cluster 6 test_union; re-skipped
- [-] test_select_id_and_source — MISFILED: `select "."` (_source) over doc with nested
      array leaks hidden cols → cluster 1 join assembly; re-skipped

## 3. `between` op broken (~6 tests)

BetweenOp (string-slicing between, and edge domains using between).
- [ ] test_edge_1.py::test_edge_using_between
- [x] test_edge_2.py::test_edge_using_missing_between1
- [ ] test_edge_2.py::test_edge_using_missing_between2
- [ ] test_expressions_w_set_ops.py::test_between_missing
- [ ] test_expressions_w_set_ops.py::test_between — "parser stack overflow"
- [ ] test_edge_1.py (edge_1:1521 skip "between is broken")

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

## 5. Statistical aggregates SQLite lacks (~7 tests)

Median/percentile/stats need an extension function or emulation (percentile via
window/subquery; sqlite has no MEDIAN).
- [ ] test_agg_ops.py::test_median — "not expected to pass yet"
- [ ] test_agg_ops.py::test_percentile
- [ ] test_agg_ops.py::test_both_percentile
- [ ] test_agg_ops.py::test_stats
- [ ] test_agg_ops.py::test_median_on_value — "sqlite does not have a median function"
- [ ] test_edge_1.py::test_percentile — "no median support"

## 6. Other aggregate ops (~4 tests)
- [x] test_agg_ops.py::test_select_agg_mult_w_when
- [ ] test_agg_ops.py::test_max_on_tuple — "broken"
- [ ] test_agg_ops.py::test_max_on_tuple2 — "broken"
- [ ] test_agg_ops.py::test_union — "broken"

## 7. Edge domains (~7 tests)

- [ ] test_edge_1.py::test_union_values — "deal with nested table as value"
- [ ] test_edge_1.py::test_union_nested_objects — same
- [ ] test_edge_1.py::test_multiple_union — same
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

## 9. Scalar expressions (~9 tests) — CLEARED 2026-07-10 (6 fixed, 3 reclustered)
- [x] test_filters.py::test_where_expression — harness zip fix (see note below)
- [x] test_filters.py::test_add_expression — harness zip fix
- [-] test_filters.py::test_regexp_expression — MISFILED: regex where compiles fine; failure
      is `select *` from nested table (returns parent doc w hidden cols) → cluster 1/2;
      re-skipped with accurate reason
- [x] test_expressions_w_set_ops.py::test_length — harness zip fix
- [x] test_expressions_w_set_ops.py::test_select_mult_w_when — two real bugs fixed:
      (1) jx_sqlite ToBooleanOp decided boolean-ness from the *abstract* term's jx_type
      (unresolved for a Variable before schema) and fell back to exists(), so `when: "b"`
      treated False as true — now decides from the compiled SqlScript's jx_type;
      (2) jx_base mapped "mult" → ProductOp (always decisive; `nulls` clause crashed) —
      now "mul"/"mult"/"multiply" → MulOp, matching "add" → AddOp (vendor/jx_base/BUGS.md #4)
- [-] test_expressions_w_set_ops.py::test_select_average — MISFILED: edges on deep table,
      "no such column: __parent__" → cluster 1; re-skipped with accurate reason
- [-] test_expressions_w_set_ops.py::test_select_average_on_none — same → cluster 1
- [x] test_expressions_w_set_ops.py::test_left_w_find — already passing; decorator removed
- [x] test_expressions_w_set_ops.py::test_not_left — already passing (the named
      "partial_eval(SQLang) before to_sql(schema)" defect survives in spirit: it was the
      ToBooleanOp bug above; the left/find path itself was fixed by earlier repairs)

> Harness fix (tests/__init__.py:224): table-format column remapping left `result.data` as a
> lazy py3 `zip`, which assertAlmostEqual cannot iterate — every table-format expectation
> combined with an explicit query `sort` failed regardless of engine correctness (py2
> leftover; engine output verified correct by direct query first). Several still-skipped
> cluster-1/2 tests combine table+sort — expect some to pass on un-skip now.

## 10. Metadata (~3 tests)
- [ ] test_metadata.py::test_meta_tables — "broken"
- [ ] test_metadata.py::test_get_nested_columns — "broken"
- [ ] test_metadata.py::test_cardinality — "cardinality not tracked" (feature gap)

## 11. Schema merging (whole area "not ready")
- [ ] test_schema_merging.py::TestSchemaMerging (whole class)
- [ ] test_schema_merging.py::test_select — "broken"
- [ ] test_schema_merging.py::test_dots_in_property_names3 — "broken"
- [ ] test_schema_merging.py::test_where — "complicated where clause needs support"

## 12. Joins (feature not implemented)
- [ ] test_joins.py::test_left_join
- [ ] test_joins.py::test_subtraction

## 13. Probably won't fix / not sqlite-relevant
- [-] test_others.py::TestOther — "meant for html endpoint (ES)"
- [-] test_set_ops.py::test_max_limit — "no need for limit when using own resources"
- [-] test_query_normalization.py::test_naming_select — "test is still unclear"

## Suggested order of attack

1. **Cluster 9 first** — `test_not_left`'s reason ("partial_eval(SQLang) before to_sql(schema)")
   names a pipeline-order defect; fixing it may un-block several expression tests cheaply.
2. **Cluster 2** — result formatting is self-contained (setop/format), no join machinery.
3. **Cluster 1** — the big one; start with the simplest (`test_deep_select_column`,
   `test_select_whole_document`'s "ambiguous __id__") and expect shared root causes.
4. **Cluster 4** — sort/edges coordination (already started: sort_op.py working-tree change).
5. Clusters 3, 6, 7, 8 as they come.
6. Clusters 5, 10, 11, 12 are feature work, not repairs — schedule deliberately.

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
