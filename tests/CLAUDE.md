# tests — how the suite works

## Running

    set PYTHONPATH=.;vendor
    python -m unittest discover -v -s tests

Config: `tests/config/sqlite.json` (`global_settings.use == "sqlite"`). `tests/__init__.py`
must set `global_settings` and `utils` before `BaseTestCase` (a `FuzzyTestCase`) can run.
`requirements.lock` is the known-good dependency set (`pip install --no-deps -r`).

## Test shape

Most tests are data-driven: one dict with `data` (docs to insert), `query`, and up to three
expectations — `expecting_list`, `expecting_table`, `expecting_cube` — executed via
`utils.execute_tests(test)`. `expecting_resultset` additionally asserts the raw SQL rows
(before document assembly; shape per `docs/JSON in Database.md`). One logical test = three format assertions; a failure in only
one format points at `jx_sqlite/format.py`, a failure in all three points at SQL generation.
FuzzyTestCase compares structurally (subset/approximate), not strict equality.

## Skips = the work queue

Tests are disabled with `@skipIf(global_settings.use == "sqlite", "<reason>")` or `@skip`.
**`docs/TEST_TRIAGE.md` is the canonical inventory** — 122 skips grouped by root-cause
cluster with a suggested order of attack. When fixing: remove the decorator, fix, check the
box in the triage doc. When a reason string is vague ("broken"), improve it.
