# Patterns from the wild

The project's house rule — *"prefer expanding data structures over adding if/else blocks; a
special case in code is a missing field in the model"* — is one instance of a family of known
design principles. This file names them and pins each to a concrete place in this repo, so the
rule has teeth beyond a slogan.

The unifying failure mode they all avoid: **the same distinction re-derived in many branches.**
The cure is to make the distinction live in exactly *one* place — usually a field, sometimes a
type, occasionally a single runtime check at the primitives.

## The principles

### Parse, don't validate (Alexis King)
Resolve a distinction *once*, at the boundary, into a typed structure — then downstream code
branches on that structure instead of re-deriving it from raw material every time.

- **In repo:** `ResolvedName` (`vendor/jx_base/models/names.py`). JX names are uniform property
  chains, so "where the relation stops and the value begins" is not expressible in the name
  string. The binder parses each name *once* into `push_name`/`push_child`. Code that used to
  re-parse the name string at every use site (the deleted `merge_term` scan in `setop.py`, the
  `full_name` reconstructions in `select_op.py`) becomes deletable. The setop→ResolvedName
  cleanup (commit `d5d8d51`) was literally moving a re-parse back to the parse.

### Make illegal states unrepresentable (Yaron Minsky)
A sentinel + nullable-flag soup permits combinations that can't mean anything. A sum
type/enum forbids them by construction.

- **In repo (offender):** the slot-kind of a `ColumnMapping` is encoded implicitly by the
  *combination* of `push_column_child == "."` (scalar) and `num_push_columns` truthy (tuple) —
  an unnamed three-state `{scalar | tuple | named}`. Nothing prevents a contradictory pair.

### Null Object (Fowler) — already lived here
Expand the *absent* value into a full participant so callers stop branching on absence.

- **In repo:** `mo_dots` `Null` (`vendor/mo_dots`). `Null == None` is deliberately True; attribute
  and index access on `Null` return `Null`, so chained access never raises and callers drop
  `if x is None`. `Data` auto-vivifies deep paths for the same reason. Consequence used in
  `format.py`: `d["."] = v` equals `d = v`, so a `push_column_child == "."` scalar branch is
  redundant, not load-bearing.

### Replace Conditional with Polymorphism (Fowler)
A switch on a type tag becomes one method per case.

- **In repo:** the operator double-dispatch (`vendor/jx_base/language.py` `register_ops`): one
  class per operator, dispatched by language id, instead of a giant switch on op-kind. See
  `vendor/jx_base/CLAUDE.md`.
- **Macro version:** `docs/INTERSECTION_SURVEY.md` shows the join-chain (P2) written inline
  **four times** across `setop.py`/`edges.py`/`group.py`. Duplicated *control flow* across files
  is a missing shared *data* structure (`SqlJoinChain` over `Source`/`Join`), emitter written
  once.

### Table-driven / data-driven design
Replace branches with a lookup structure the data walks through.

- **In repo:** the `DocumentDetails` tree + `_accumulate_nested` (`setop.py`) — the pull plan is
  a data structure mirroring the snowflake, not a nest of format-specific conditionals. The
  `ColumnMapping` `DataClass` and the candidate operator set in `INTERSECTION_SURVEY.md` §4 are
  the same move applied to the query strategies.

## The boundary — the rule is not "always add a field"

The goal is *one home for a distinction*, and sometimes that home is a collapse, not a field:

- **`es_column="."` for a nameless list** (`vendor/jx_base/CLAUDE.md`): a bare `[1,2,3]` is kept
  untyped rather than expanded into `~i~`/`~s~`/`~a~` variants, because the Python target is
  dynamically typed — the one distinguishing check lives at the primitives (`isinstance` via
  `to_data`/`delist`), not in the schema. Kyle notes this *may* be wrong long-term, but it is a
  deliberate collapse, not an oversight.
- **Inner object = one-element array** (root `CLAUDE.md`; `insert.py` scalar→array promotion;
  the `ORDER = 0` rule in `INTERSECTION_SURVEY.md` §2A): the schema *expands* the scalar into an
  array so no downstream code carries a scalar-vs-array branch. Expansion here removes branches.

Both avoid the same thing: a distinction re-decided in ten places. One expands, one collapses;
the test is always *"how many branches re-derive this?"*, not *"is there a new field?"*.

### A concrete non-merge (verified 2026-07-21)
`format.py::format_flat` places a pulled value into a slot with a three-way
`scalar / tuple / named` fork that *looks* identical in the `table` and `list` branches — an
obvious candidate to extract into one helper. It is not mergeable. The `list` branch writes
into a `Data` and relies on mo_dots **deferred** writes (`row[psn][child] = v` auto-vivifies,
and `row[psn]["."] = v` collapses to `row[psn] = v`); the `table` branch writes into a **plain
list** indexed by `push_column_index`, which has no deferral, so it needs the explicit scalar
assignment and the explicit `Data()` creation. A shared helper that pre-materialises the slot
kills the deferral the `list` branch depends on (empty `{}` where a scalar belonged). The
suite catches it immediately (`test_aggs_on_parent`: `o={}` vs `o=1`). Lesson: the fork is not
the distinction — the *container* is (plain list vs deferred `Data`), and that is real. Left
as two branches on purpose.
