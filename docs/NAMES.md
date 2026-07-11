# Names — the namespace layer (resolve / leaves)

Status 2026-07-11: resurrected, wired into `Schema.leaves()`, suite green (366 ran, 129
skipped; the four skipped-broken `tests/test_leaves.py` contract cases now pass).

## What it is

`jx_base/models/names.py` — the rename-map namespace, recovered from
`4959d0c:vendor/jx_base/models/namespace.py` (June 2024, "use namespace" commits; killed
2025-02-04 when lib-update sync `d607078` clobbered the vendored file — it was never
upstreamed). It manages **shadowing of variables and changing perspective**:

- `Names` = list of `Scope`s, nearest first. A name binds at the first scope that knows it.
- `Scope.names` — enumerable bindings: untyped JX names → tuple of columns (a union type
  binds several typed columns to one name).
- `Scope.aliases` — exact-lookup-only bindings: typed/physical names (`a.$N`). Never
  enumerated, because `startswith_field("a.$N", "a")` is True and a prefix scan would leak
  typed duplicates.
- `stack(**renames)` — push a scope; new bindings shadow old.
- `union(**renames)` — merge at equal precedence; collisions become `AMBIGUOUS` (kept for
  future joins; untested conjecture).
- `add_free_var(name)` — mint `__$N` iteration variables; `resolve()` substitutes the real
  path. The 4959d0c idiom for entering a nested table:
  `var, ns = ns.add_free_var(table); ns = ns.stack(row=var, **{".": var})`.

`mo_sqlite/models/names.py` — `build_names(query_paths, columns, origin)` constructs the
namespace seen from one table of a snowflake.

## The rules (decided by tests/test_leaves.py + live behavior)

1. **Canonical coordinate**: every leaf column is identified by its fact-absolute typed path
   `concat(nested_path[0], es_column)` (e.g. `testing._a.$A.b.$S`). Every visible name is
   derived from it by path algebra in ONE namespace at a time. (The old `Schema.leaves`
   mixed typed table paths with untyped fact-relative names mid-computation — the direct
   cause of the `_a._a.b` doubling and the `leaves(".")` breakage.)
2. **Scope order**: origin, ancestors up to fact, then descendants of origin
   (shallowest first). Each scope covers its whole subtree, keyed relative to that table —
   so relative names shadow absolute ones, `o` climbs from a child origin to the fact, and
   bare `b` still reaches into a child from the fact (lowest precedence).
3. **Shadowing is scope-granular**: `leaves(prefix)` takes ALL its answers from the first
   scope with any match (matches the old search-order semantics; a flat merged dict would
   mix scopes).
4. **Typed names are physical addresses**: they resolve by exact match at the ORIGIN scope
   only; they do not travel the scope chain (`test_exact_match_parent/_child` expect `[]`).
5. **Hidden columns are not part of the namespace**: `_id`/`__id__`/`__order__`/`__parent__`
   are never bound; they are plumbing, reachable only by dedicated code paths
   (`Schema.leaves` keeps the GUID special case for now).

## What is wired

- `Schema.leaves()` (mo_sqlite/models/schema.py) is now a shim over `build_names(...)`,
  built per call (no cache — `self.columns` mutates under schema change). Returns a list
  (ordered) where it used to return a set.
- Contract tests: `tests/test_leaves.py` (all 6 un-skipped), `tests/test_namespace.py`
  (15 cases: the six contract cases + shadowing/perspective/stack/union/free-var).

## Next steps

1. **Consume `resolve()` at the expression layer.** `variable.py`, `get_op.py`,
   `select_op.py` call `leaves()` then re-derive typed/untyped names; exact-name lookups
   should be `resolve()`, enumeration (`*`, LeavesOp) stays `leaves()`.
   *Evidence this is overdue* (Kyle, 2026-07-12): jx_sqlite `select_op.to_sql` now carries
   per-branch dispatch kludges — `all_leaves` vs `leaves` chosen by
   `schema.nested_path[0] == select.frum.nested_path[0]`, and a `keep(col)`/`branch_prefix`
   filter re-rooting push names per branch. Both exist because setop re-compiles the SAME
   select once per branch and each branch re-interprets the names from its own perspective.
   The right shape: resolve the select ONCE at the query origin (names → canonical columns +
   push names), then each branch merely projects the columns it owns. The kludges are the
   projection rule written in the wrong layer.
2. **Retire the redundant resolution drafts** now that one binder exists:
   `Schema.map_to_sql` (references `c.names[origin]`, likely dead), `Schema.get_columns`,
   `Schema.keys`, `Snowflake.leaves` (snowflake.py — mixes `name`/`es_column` candidates),
   and eventually jx-elasticsearch's `model/schema.py` sibling (`split_values`, the
   "SHADOWED BY A RELATIVE NAME?" TODO).
3. **Cache.** `build_names` is O(tables × columns) per call. Cache on `ColumnList` keyed by
   (fact, origin) with an epoch counter bumped by add/remove/rename_tables. Lazy per-scope
   construction (the "thunk" idea, for thousands-of-columns schemas or remote metadata á la
   ES/BigQuery) is deliberately deferred — dict builds are cheap when columns are already
   in memory.
4. **Cluster 1 via Names.** Naming-layer symptoms are fixed (no hidden-column leak, no
   `_a._a.b`). Fact-perspective `select *` (`test_select_whole_document`) is now DONE — the two
   symptoms named here are fixed, but neither was in `setop.py`:
   (a) the flattened `_a.b`/`_a.v` at fact scope came from `select_op.py` expanding LeavesOp(".")
       over child-table leaves — plain `*` (no prefix) now stops at the array boundary
       (`len(col.nested_path) > origin_depth`);
   (b) the inner-object split (`_a: [{"b":"x"},{"v":5}]`) was an INSERT bug —
       `mo_sqlite/models/insert.py` spawned a nested row per flattened leaf; now reuses the row
       for a given (parent, order).
   Table/cube also needed `format.py::_deep_header` to key on the top-level container name, not
   the leaf. Still open: **deep-perspective `select *`** (`from testing._a`) does not switch
   origin and nested-origin `*` never pulls ancestor scalars — leaves() from a child origin
   must return parents (rule #2 in this doc claims it does; the shim doesn't yet). That blocks
   `test_select_whole_nested_document`/`test_deep_star`/`test_deep_star_w_parent` — next target.
5. **Free vars in anger.** Replace the ad-hoc `row.` prefix stripping (variable.py
   `partial_eval`, `tail_field == "row"`) with the `add_free_var`/`stack(row=...)` idiom;
   revive `sql_select_all_from_op.query()` (the algebra path) on top of Names.
6. **Ambiguity policy.** Only `union()` creates `AMBIGUOUS` today and no caller checks for
   it; decide where it surfaces as a user error (duplicate select names? join collisions?).
7. **GUID.** Decide whether `_id` becomes an explicit fact-scope binding instead of the
   special case in `Schema.leaves`.
8. **Partial typed paths.** Aliases are exact-match only; `leaves("a.$A.b")` is unsupported.
   Decide whether that addressing mode is ever needed.
9. **Upstream before the next svn sync.** ✅ DONE 2026-07-11 via svn-sync from jx-sqlite's
   vendor WCs: `vendor/jx_base/models/names.py` (r2856), `vendor/mo_sqlite/models/names.py`
   (r2859), `namespace.py` deletion (r2856), plus the accumulated cluster-2/9 vendored fixes
   (r2854–2859). jx-python and mo-sqlite pick these up on their next `svn update`. (This is
   exactly how the 2024 draft died — published this time so it survives.)
