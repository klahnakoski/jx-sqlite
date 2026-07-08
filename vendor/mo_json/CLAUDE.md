# mo_json — typed JSON encoding and the JX type system

## Typed encoding (`typed_encoder.py`)

JSON is stored "typed": every leaf is keyed by a type marker so heterogeneous values of the
same property coexist. The type keys (defined in `types.py`):

| key | meaning  |            | key | meaning |
|-----|----------|------------|-----|---------|
| `~b~` | boolean |           | `~s~` | string |
| `~i~` | integer |           | `~a~` | array (→ nested table) |
| `~n~` | number  |           | `~e~` | exists (count marker) |
| `~t~` | time    |           | `~j~` | json |
| `~d~` | duration |          |     |        |

So `{"a": {"b": 1}}` becomes column `a.b.~n~`; an array property `a` becomes child table
`...a.~a~`. `~e~` records existence/cardinality of an object so "present but empty" is
distinguishable from absent. `IS_TYPE_KEY` / `IS_PRIMITIVE_KEY` regexes in `types.py` test for
these.

- `JxType` (`types.py`) is the type algebra carried by every `SqlScript` (`jx_type`);
  union types add multiple typed keys under one name.
- `TypedObject` (`typed_object.py`) wraps a value in its typed form.
- `scrubber.py` normalizes Python objects → JSON-safe (dates→unix, Decimal, loops detection;
  `MAX_DEPTH` recursion cap).

## Traps

- Never build typed names by string concatenation of `.` — property names may contain dots;
  use `mo_dots` field functions and `typed_column`/`untype_field` from `mo_sql.utils`.
- Decoding must strip type keys at every nesting level; result-formatting bugs (e.g. values
  wrapped as `{"":{"":...}}`) usually mean an untype step was missed.
