# mo_json — known defects

## 1. `encoder.py` encoded `date`/`datetime` in LOCAL time (FIXED — coverage gaps remain)

`value2json` used `time.mktime(value.timetuple())`, which discarded `tzinfo` and interpreted
the naive tuple as local time — results were machine/timezone dependent. Fixed by delegating
to `scrubber.datetime2unix` (tz-aware subtraction from UTC epoch; naive treated as UTC).

Coverage so far: `tests/test_json.py::test_zoned_to_utc` (tz-aware datetime → true UTC unix,
host-independent).

**Still missing:**
- a **naive** `datetime` is treated as UTC (host-timezone independent);
- `date` (not just `datetime`) goes through the same path.
