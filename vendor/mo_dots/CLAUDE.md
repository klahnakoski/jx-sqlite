# mo_dots — null-safe data structures and the field-path algebra

Foundation for everything above it. Two things matter when working in this repo:

## Null semantics

- `Null` (NullType) is a null-safe None: attribute/index access on it returns `Null`, so
  chained access never raises. `Null == None` is **True** — code all over this repo tests
  `x == None` deliberately (do NOT "fix" it to `is None`; that breaks NullType/Data slots).
- `Data` is a dict with dot-path access; missing paths yield `Null`, and assigning to a deep
  path auto-creates intermediates. `to_data`/`from_data` convert at API boundaries.
- Empty containers and `Null` are falsey; `is_missing(x)` is the sanctioned test.

## Field paths (`fields.py`)

Property names may contain literal dots (escaped). Never `s.split(".")` on a field name —
use `split_field`/`join_field`/`concat_field`/`relative_field`/`startswith_field`/
`endswith_field`. `"."` names the root/fact table; `".."` walks toward the parent
(`relative_field("a", "a.b.c")` → `"..."`-style relative names). The entire snowflake naming
scheme (tables and columns) is built on these functions.
