# Null semantics: conservative vs decisive operators

JX has two null policies for multi-operand math/comparison operators. Which one an operator
uses **by default** is fixed by the operator's name; the `nulls` clause overrides it per call.

## The rule

- **Scalar / fixed-arity operators are CONSERVATIVE by default** — if *any* operand is null
  (missing / out-of-class), the result is null. Null is contagious.
- **Aggregates are DECISIVE by default** — null operands are *skipped*; the result is null
  only when *every* operand is null.

| operation | conservative (scalar) | decisive (aggregate) |
|-----------|-----------------------|----------------------|
| add       | `add`                 | `sum`                |
| multiply  | `mul` / `mult` / `multiply` | `product`       |
| minimum   | `least`               | `min`                |
| maximum   | `most`                | `max`                |

Examples (default behavior):

    {"add":      [42, null]}      => null     {"sum":     [42, null]}      => 42
    {"multiply": [2, null, 4]}    => null     {"product": [2, null, 4]}    => 8
    {"least":    [5, null, 3]}    => null     {"min":     [5, null, 3]}    => 3
    {"most":     [5, null, 3]}    => null     {"max":     [5, null, 3]}    => 5

## Overriding with `nulls`

Every one of these accepts a `nulls` clause that flips the default for that call:

    {"add":     ["a", "b"], "nulls": true}   # decisive add:  add(0, missing) => 0
    {"product": ["a", "b"], "nulls": false}  # conservative product: any missing => null

Pairs with `default`: when a conservative op yields null, a `default` clause supplies the
fallback — `{"add": ["a","b"], "default": -5}` gives -5 whenever `a` or `b` is missing.

## Why this split (SQL / BigQuery / Athena)

Every SQL dialect already draws this line the same way, and the names above mirror it:

- Scalar arithmetic `a + b`, `a * b` → **NULL propagates** (conservative). → `add`, `mul`.
- Aggregates `SUM(col)`, `MIN(col)`, `MAX(col)` → **ignore NULL rows** (decisive). →
  `sum`, `product`, `min`, `max`.
- BigQuery / Athena fixed-arity scalars `LEAST(...)` / `GREATEST(...)` → **NULL if any arg
  is NULL** (conservative). → `least`, `most`.

So: the operator you'd write inline in a row expression is conservative; the function you'd
apply across a collection is decisive. Same operation, two names, chosen by which null policy
you want.

## Known code inconsistencies (2026-07-11, to reconcile with this policy)

Empirically the scalar min/max family does **not** yet obey the rule:

- `least([5, null, 3])` returns `3` (decisive) — should be `null` (conservative), like `most`.
- scalar `max([5, null, 3])` returns `5` (decisive) — should be `null` if `max` is the scalar
  form; if `max` is intended purely as the aggregate, the scalar name is `most` and `max`
  should only appear over a collection.

`add` and `mul`/`mult`/`multiply` already obey it (conservative); `sum`/`product` already
obey it (decisive). See vendor/jx_base/expressions/__init__.py operator table.
