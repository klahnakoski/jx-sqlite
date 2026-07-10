# jx_base — known defects

## 1. `is_expression` fooled by FlatList (FIXED in jx-sqlite vendored copy — needs upstream + tests)

`language.py is_expression(call)` tested `getattr(call, ID, None) != None`. A `FlatList`
broadcasts attribute access over its items, returning a FlatList (of Nulls), and
`FlatList != None` is True — so any non-empty FlatList passed as an expression. Verified
consequence: `{"eq": ["a", [{"literal": "4"}, {"literal": "2"}]]}` — `_jx_expression` returned
the raw literal list unconverted, `EqOp.rhs` became a FlatList, and
`self.rhs.partial_eval(lang)` raised `TypeError: 'FlatList' object is not callable`
(jx-sqlite `test_filters.test_eq_using_tuple_of_literals` / `test_in_using_tuple_of_literals`).

**Fix applied (2026-07-10):** `is_expression` now checks `isinstance(getattr(call, ID, None), int)`
(`_op_id` is always an int, assigned by `next_operator_id()`).

Coverage to add upstream: `is_expression` over a FlatList / list / Data / op instance / op
class; `jx_expression({"eq": [field, [literal, literal]]})` and the `in` form produce working
expressions.

## 2. `sort_op._normalize_sort` rejects Data-wrapped sort items (FIXED in jx-sqlite vendored copy — needs upstream + tests)

A sort item arriving as `Data('.')` (Data may hold a primitive; must be unwrapped asap) fell
past the `is_text` branch into the `{field: direction}` branch and failed. **Fix applied
(2026-07-10):** `s = from_data(s)` at the top of the normalization loop. Coverage to add
upstream: `_normalize_sort` over Data-wrapped fieldname, plain fieldname, list, `{field:
direction}`, `{"field":…, "sort":…}` forms.
