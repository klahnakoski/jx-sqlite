# mo_sqlite — SQLite dialect + snowflake models

Two things live here: the **SQLang language** (ops that ARE SQLite SQL, strict semantics) and
the **relational models** (Container/Namespace/Snowflake/Facts/Schema over a SQLite db).

## Design direction (in progress — Kyle)

The recent code movement ("move insert.py to mo-sqlite", etc.) is a deliberate split, still
underway: **mo_sqlite should become pure SQL** — at this level everything is a concrete name,
strict SQL semantics, nothing decisive — while the **projection and null-safe transformations
stay in jx_sqlite**. When deciding where new code belongs, follow that line. The typed column
names (`a.b.~n~`) appearing here are NOT type logic: to mo_sqlite they are just concrete
column names; interpreting them is jx-level business.

## SQLang vs JxSql (do not confuse)

- `SQLang` (registered in `expressions/__init__.py`) contains `Sql*Op` classes — literal
  SQLite constructs with **strict SQL null semantics**, no decisiveness.
- `JxSql` (in jx_sqlite) compiles decisive JX ops down to these.
- Pipeline per expression: `jx_expression(...)` (JX) → `.partial_eval(SQLang)` →
  `.to_sql(schema)` → `SqlScript`.

## SqlScript (`expressions/sql_script.py`) — the lingua franca (and a kludge)

Kyle: SqlScript is a kludge to hold the compiled SQL plus some metadata about it; the hope is
it elegantly disappears in the long run. Don't build new structure that depends on it more
deeply — but while it exists, its constructor-enforced invariants (TYPE_CHECK) hold:

Carries `(jx_type, expr, frum, miss, schema)`.

- `expr` is `SQL` and **never** another SqlScript; `jx_type` is a `JxType`; `schema` required.
- `miss` is a jx_base Expression (never raw SQL), never itself missing; defaults to
  `frum.missing(SQLang).partial_eval(SQLang)`.
- Rendering: unless `miss` is TRUE/FALSE or `frum` is a Variable, `expr` is wrapped in
  `CASE WHEN NOT (miss) THEN expr END` — this is how decisive null-safety reaches SQL.
- `frum` is the original expression that produced `expr` (kept for identity/missing checks).

`sql_script.py` at package root is a different thing: `SqlStep`/`SqlTree` assemble the
multi-table document-retrieval query — one branch per snowflake leaf, `UNION ALL`, ordered by
`o{id}_{i}` (order), `i{id}_{i}` (uids), with value columns `c{id}_{i}`; NULL padding aligns
the select lists across branches.

## Models (`models/`)

- **Namespace** = metadata for the whole db (all snowflakes, all columns).
  **Snowflake** = one hierarchy: fact table + nested (array) tables. `nested_path` is a list
  of **full table names, leaf first** (e.g. `["fact.a.b", "fact"]`); never refer to a table by
  bare name.
- **Schema** (`models/schema.py`) = the snowflake seen from one origin table
  (`nested_path[0]`). All column-name resolution relative to a query origin happens in
  `Schema.leaves()` — the trickiest function in the package. Kyle suspects the difficulty is a
  symptom of a missing abstraction/module for reasoning about leaf values; if work here keeps
  fighting the code, proposing that abstraction may be more valuable than another patch.
- Schema evolution (`snowflake.change_schema`): `add` (ALTER TABLE ADD COLUMN) and `nest`
  (scalar→array promotion: create child table `<parent>.<col>.~a~`, move columns, copy data,
  rebuild parent without old columns).
- Hidden columns (`mo_sql.utils`): `_id` (GUID, user-visible), `__id__` (UID, per-db rowid),
  `__parent__` (FK to parent UID), `__order__` (array position).
- Insertion (`models/insert.py`, moved here from jx_sqlite) diffs incoming typed JSON against
  the schema and emits `required_changes`.

## Traps

- Column names in the db are **typed** (`a.b.~n~`); use `typed_column`/`untype_field`/
  `untyped_column` from `mo_sql.utils` — never string-split on `.` (property names may contain
  escaped dots; use mo_dots field functions).
- A column of union type has multiple typed columns; readers COALESCE across them.
- `transacfion.py` [sic] holds the transaction machinery — filename typo is historical.
