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
> Remaining error: QueryOp.wrap() signature (test_meta).

## 1. Deep / nested queries (~55 tests — the dominant cluster)

Queries that reach across tables of the snowflake: deep selects, deep wheres, aggs on
parent+child, relative (`..`) names. Almost all of `test_deep_ops.py`. Likely a family of
root causes in multi-table join assembly (`jx_sqlite/edges.py`, `mo_sqlite/sql_script.py`
SqlStep/SqlTree) rather than one bug; expect fixing the first few to reveal the pattern.

### test_deep_ops.py
- [ ] test_select_gt_on_sub
- [ ] test_select_in_w_multivalue
- [ ] test_select_when_on_multivalue
- [ ] test_deep_select_column
- [ ] test_deep_select_column_w_groupby
- [ ] test_bad_deep_select_column_w_groupby
- [ ] test_abs_shallow_select
- [ ] test_select_whole_document — "ambiguous column name: __id__"
- [ ] test_select_whole_nested_document
- [ ] test_deep_names_w_star
- [ ] test_deep_names_select_value
- [ ] test_deep_names
- [ ] test_deep_agg_on_expression
- [ ] test_deep_agg_on_expression_w_shallow_where
- [ ] test_agg_w_complicated_where
- [ ] test_deep_where_on_fact_table
- [ ] test_id_select
- [ ] test_aggs_on_parent
- [ ] test_aggs_on_parent_and_child
- [ ] test_aggs_on_parent_and_child2
- [ ] test_aggs_on_parent_and_child3
- [ ] test_deep_edge_using_list
- [ ] test_deep_agg_w_deeper_select_relative_name_neop
- [ ] test_setop_w_deep_select_value_neop
- [ ] test_deep_agg_w_deeper_select_relative_name
- [ ] test_shallow_and_ne_deep
- [ ] test_setop_w_deep_select_value
- [ ] test_select_average_on_none
- [ ] test_missing
- [ ] test_missing_on_not_exists
- [ ] test_exists
- [ ] test_deep_or
- [ ] test_sibling_nested_column
- [ ] test_deep_star
- [ ] test_deep_star_w_parent
- [ ] test_deep_select_dot
- [ ] test_from_shallow_select_deep_column
- [ ] test_setop_w_shallow_eq_string
- [ ] test_deep_edge_w_shallow_expression
- [ ] test_deep_edge_w_shallow_var
- [ ] test_nested_property_edge_w_shallow_expression
- [ ] test_nested_document_selection
- [ ] test_nested_filter_with_groupby

### test_nested.py
- [ ] TestNestedQueries (whole class) — "broken"

### test_sort.py (nested subset)
- [ ] test_nested_array
- [ ] test_nested
- [ ] test_single_nested — "nested are broken"

### test_set_ops.py (deep subset)
- [ ] test_single_deep_select
- [ ] test_select_w_deep_star
- [ ] test_select_w_nested_values — "fix me first"
- [ ] test_prefix_in_deep_where_clause — "fix me"
- [ ] test_exists_in_where_clause — "fix me"
- [ ] test_select_into_children — "Too complicated"

## 2. Selecting objects / stars / leaves (setop formatting, ~12 tests)

Shallow queries whose select clause is an object, `*`, leaves, or an array value.
Likely `jx_sqlite/setop.py` + `format.py` (result-shaping, not SQL generation).
Known symptom (comment in test_set_ops.py:1013): `timestamp.~s~` results in
`{"":{"":{"":{"":"..."}}}}` — untyping of column names during formatting.

### test_set_ops.py
- [ ] test_select_w_star
- [ ] test_select_expression
- [ ] test_select_object
- [ ] test_select_leaves
- [ ] test_select_leaves2
- [ ] test_select_value_object
- [ ] test_select2_object
- [ ] test_select3_object
- [ ] test_select_array_as_value
- [ ] test_union_columns
- [ ] test_select_id_and_source

## 3. `between` op broken (~6 tests)

BetweenOp (string-slicing between, and edge domains using between).
- [ ] test_edge_1.py::test_edge_using_between
- [ ] test_edge_2.py::test_edge_using_missing_between1
- [ ] test_edge_2.py::test_edge_using_missing_between2
- [ ] test_expressions_w_set_ops.py::test_between_missing
- [ ] test_expressions_w_set_ops.py::test_between — "parser stack overflow"
- [ ] test_edge_1.py (edge_1:1521 skip "between is broken")

## 4. Sort coordinated with edges / groupby (~7 tests)

"coordinate sort clause with matching edges" — the ORDER BY must be expressed in terms of
the edge/groupby output columns. Related to the uncommitted `jx_base/expressions/sort_op.py`
change.
- [ ] test_sort.py::test_edge_and_sort — "broken"
- [ ] test_sort.py::test_2edge_and_sort
- [ ] test_sort.py::test_groupby_and_sort — "fix me"
- [ ] test_sort.py::test_groupby_expression_and_sort — "fix me"
- [ ] test_sort.py::test_groupby2a_and_sort — "fix me"
- [ ] test_sort.py::test_groupby2b_and_sort
- [ ] test_sort.py::test_groupby2c_and_sort
- [ ] test_edge_time.py::test_count_over_time_w_sort — "broken"

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
- [ ] test_agg_ops.py::test_select_agg_mult_w_when — "broken"
- [ ] test_agg_ops.py::test_max_on_tuple — "broken"
- [ ] test_agg_ops.py::test_max_on_tuple2 — "broken"
- [ ] test_agg_ops.py::test_union — "broken"

## 7. Edge domains (~7 tests)

- [ ] test_edge_1.py::test_union_values — "deal with nested table as value"
- [ ] test_edge_1.py::test_union_nested_objects — same
- [ ] test_edge_1.py::test_multiple_union — same
- [ ] test_edge_1.py::test_multiple_union2 — same
- [ ] test_edge_1.py::test_empty_default_domain_w_groupby — "broken"
- [ ] test_edge_1.py::test_edge_using_tuple — "broken"
- [ ] test_edge_1.py::test_shallow_with_deep_edge — "not sure what first() of nested column
      would be; requires schema merging of a.b.~n~ and a.~a~.b.~n~" (design question)

## 8. Groupby advanced (~7 tests)
- [ ] test_groupby_1.py::test_groupby_left_id — "broken"
- [ ] test_groupby_1.py::test_count_values — "requires subqueries"
- [ ] test_groupby_1.py::test_groupby_multivalue_nested — "for coverage"
- [ ] test_groupby_1.py::test_groupby_object — "broken"
- [ ] test_groupby_1.py::test_groupby_star — "broken"
- [ ] test_groupby_1.py::test_groupby_object_star — "broken"
- [ ] test_groupby_1.py::test_groupby_multivalue_naive — "requires groupby sets"

## 9. Scalar expressions (~9 tests)
- [ ] test_filters.py::test_where_expression — "broken"
- [ ] test_filters.py::test_add_expression — "broken"
- [ ] test_filters.py::test_regexp_expression — "broken"
- [ ] test_expressions_w_set_ops.py::test_length — "broken"
- [ ] test_expressions_w_set_ops.py::test_select_mult_w_when — "broken"
- [ ] test_expressions_w_set_ops.py::test_select_average — "broken"
- [ ] test_expressions_w_set_ops.py::test_select_average_on_none — "broken"
- [ ] test_expressions_w_set_ops.py::test_left_w_find — (no reason)
- [ ] test_expressions_w_set_ops.py::test_not_left — "problem partial_eval(SQLang) before
      to_sql(schema)" ← pipeline-order issue; may explain others in this cluster

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
