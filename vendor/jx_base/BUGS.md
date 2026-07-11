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

## 2. `query_metadata` still used the old 1-arg `QueryOp.wrap` (FIXED in jx-sqlite vendored copy — needs upstream + tests)

`meta_columns.py query_metadata` called `QueryOp.wrap(query)`; the restored signature is
`wrap(query, container, lang)`. Now passes the denormalized columns ListContainer and `JX`
(the `lang` parameter is currently unused in `wrap`'s body). Verified by jx-sqlite
`test_metadata.test_meta` (meta.columns queries in all three formats).

## 3. `sort_op._normalize_sort` rejects Data-wrapped sort items (FIXED in jx-sqlite vendored copy — needs upstream + tests)

A sort item arriving as `Data('.')` (Data may hold a primitive; must be unwrapped asap) fell
past the `is_text` branch into the `{field: direction}` branch and failed. **Fix applied
(2026-07-10):** `s = from_data(s)` at the top of the normalization loop. Coverage to add
upstream: `_normalize_sort` over Data-wrapped fieldname, plain fieldname, list, `{field:
direction}`, `{"field":…, "sort":…}` forms.

## 5. Bare-string select never reached `.*`/`*` wildcard handling; `.*` branch had a NameError; `is_variable` unimported (FIXED in jx-sqlite vendored copy — needs upstream + tests)

`select_op.normalize_one` handled `value.endswith(".*")` / `endswith("*")` (leaf-expansion into
a `LeavesOp`) **only when the value arrived as text** — but a bare-string select (`"a.*"`, as
opposed to `{"value": "a.*"}`) was eagerly converted via `select = SelectOne(select,
jx_expression(select))` at the top of the text branch, so `value` was already a `GetOp` and
`is_text(value)` was False. The wildcard branches were therefore dead for the common
`select: ["a.*"]` form and the query returned all nulls (`schema.leaves("a.*")` is empty →
`NULL`). Two further defects hid behind that dead code: the `.*` branch read
`jx_expression(root_nam)` (NameError typo for `root_name`), and `is_variable` (used in both
wildcard branches) was never imported. **Fix applied (2026-07-10):** text branch now keeps the
raw text as `{"value": select}` (name unset) so the value flows through `is_text(value)`
handling; fixed `root_nam` → `root_name`; imported `is_variable`. Verified by jx-sqlite
`test_set_ops` `test_select_leaves` (`a.*`), `test_select_leaves2` (`a*` → literal dotted keys),
`test_select_w_star`, `test_select_object`/`value_object`/`2_object`/`3_object`. Coverage to
add upstream: `normalize_one` over bare `"a.*"`, `"a*"`, `"*"`, and plain `"a"` (name defaults
and produced `LeavesOp`/`prefix`).

## 4. `"mult"`/`"mul"`/`"multiply"` mapped to ProductOp — forced decisive, `nulls` clause crashes (FIXED in jx-sqlite vendored copy — needs upstream + tests)

`operators["mult"]` pointed at `ProductOp`, whose `__new__` returns `MulOp(*terms, nulls=True)`
unconditionally — the JSON `mult` op was always decisive, and an explicit `{"mult": [...],
"nulls": true}` raised TypeError (`__new__` does not accept the clause). Asymmetric with
`"add" -> AddOp` (conservative by default, `nulls` clause honored). **Fix applied
(2026-07-10):** `"mul"/"mult"/"multiply" -> MulOp`; `"product"` remains the decisive
ProductOp. Verified by jx-sqlite `test_select_mult_w_when` (expects `mult(null, 0) = null`).
Coverage to add upstream: `{"mult": ["a", "b"]}` over a missing operand (expect null);
`{"mult": [...], "nulls": true}` (expect decisive, no crash).

## 6. `CaseOp.partial_eval` appends a bare then-value into `whens`; inner-else flatten duplicated per inner when (FIXED in jx-sqlite vendored copy — needs upstream + tests)

Two defects in `case_op.py partial_eval`:
(a) when a `when` clause folded to TRUE, the code did `whens.append(w.then.partial_eval(lang))`
— a bare VALUE (e.g. a Literal) in a list every consumer treats as WhenOps. Downstream
`whens[0].when` / constructor `w.els_` checks then raise `Literal object has no attribute ...`.
A TRUE when means the branch always fires: it is the `else` for the whens collected so far
(later whens and the original else are unreachable). **Fix applied (2026-07-12):** the folded
then becomes `_else` and the loop breaks; the original else is used only when no when folded
to TRUE.
(b) flattening a nested CaseOp then-clause appended the inner else (`WhenOp(when, then.els_)`)
INSIDE the loop over inner whens — duplicated per inner when, and shadowing the inner whens
after the first. Moved after the loop; also `then.els_` was passed positionally where `then=`
was meant (same in the nested-WhenOp arm).
Trigger: any `between` (its partial_eval builds CaseOps whose first when folds by literal
missing/IsNumber tests). Coverage to add upstream: CaseOp with a when folding TRUE
mid-sequence; nested CaseOp as then-clause with an else.
