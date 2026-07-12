# Test Triage — the repair campaign

Tests skipped for sqlite (`@skipIf(global_settings.use == "sqlite", ...)` / `@skip(...)`),
grouped by suspected root cause so each work session can pick one cluster, fix it, and check
items off. Update this file as tests are un-skipped or reasons are refined.

Legend: `[ ]` skipped, `[x]` passing (decorator removed), `[-]` won't fix.

> Baseline (dev, 2026-07-11 night): HEAD b8de874, 370 ran / 0 err / 86 skip. The checklists
> below are the source of truth for what remains; work one cluster per session.

## 1. Deep / nested queries (~55 tests — the dominant cluster)

Queries that reach across tables of the snowflake: deep selects, deep wheres, aggs on
parent+child, relative (`..`) names. Almost all of `test_deep_ops.py`. Likely a family of
root causes in multi-table join assembly (`jx_sqlite/edges.py`, `mo_sqlite/sql_script.py`
SqlStep/SqlTree) rather than one bug; expect fixing the first few to reveal the pattern.

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
- [ ] test_deep_where_on_fact_table — multi-value collapse (P7): explicit deep-column select must merge child values
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
- [ ] test_deep_origin_agg_on_child — NEW (2026-07-12): 3-level snowflake, origin = middle
      table, sum over its child per origin row; plain sibling select falls into edges.py
      aggregates (`sql_aggs[NULL.op]` KeyError). Pins the per-row accumulate model.

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

## 2. Selecting objects / stars / leaves (setop formatting) — CLEARED (8 fixed, 3 reclustered)

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
