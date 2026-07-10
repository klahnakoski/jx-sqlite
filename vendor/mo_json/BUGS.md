# mo_json — known defects

## 1. `encoder.py` encoded `date`/`datetime` in LOCAL time (FIXED — needs tests)

`value2json` (and everything built on it) converted `date`/`datetime` to unix with
`time.mktime(value.timetuple())`. Two verified problems:

- `datetime.timetuple()` **discards `tzinfo`**, so a timezone-aware value loses its zone.
- `time.mktime` interprets the naive time tuple as **local time**, not UTC.

Consequence: encoding a `datetime(2025, 2, 22, 5, 18, 38, tzinfo=utc)` produced
`1740219518` (`10:18:38 UTC`) on a UTC-5 machine — off by the machine's offset (here +5h),
and **machine/timezone dependent**, so results differ per host and per DST. The sibling
`scrubber.py:datetime2unix` already did this correctly (tz-aware subtraction from a UTC epoch;
naive treated as UTC).

Found 2026-07-09 while debugging jx-python `tests/test_jx_immediate.test_aws_complex`, whose
`Timestamp`/`expire` values came out +5h.

**Fix applied:** `encoder.py` now calls `datetime2unix(value)` for both `date` and `datetime`
(reusing the correct `scrubber` implementation). Verified: `value2json(datetime(2025,2,22,
5,18,38,tzinfo=utc))` → `1740201518`.

**REQUIRED — do not let this regress:**
- Add/expand `mo_json` tests asserting `value2json` of a **tz-aware** `datetime` equals the
  true UTC unix, and that a **naive** `datetime` is treated as UTC — both **independent of the
  host timezone** (set/override TZ in the test, or assert against a fixed UTC expectation, so
  a machine in another zone still passes).
- Cover `date` as well as `datetime`.
- This file lives in a vendored copy. The fix and its tests must reach the canonical `mo_json`
  repo (via `svn-sync`) or the next sync reverts them.
