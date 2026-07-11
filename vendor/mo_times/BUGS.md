# mo_times — known defects

## 1. Two independent clocks made "now" unmockable via one point (FIXED — needs tests)

`dates.py` had two time sources: `unix_now` (used by `Date.now/eod/today`) and `_utcnow`
(used by `unicode2Date`'s `"now"/"today"/"eod"/"tomorrow"` branches). So `Date("now")` read a
different clock than `Date.now()` — untestable with a single mock (`datetime.utcnow()` behind
`_utcnow` is also deprecated in 3.12+). Fix: those `unicode2Date` branches now delegate to
`Date.now()/today()/eod()`, making `unix_now` the single mockable clock (formulas unchanged).

**REQUIRED — do not let this regress:**
- Add `mo_times` tests that mock `dates.unix_now` to a fixed value and assert `Date("now")`,
  `Date("today")`, `Date("eod")`, `Date("tomorrow")`, `Date.now()`, `Date.today()`, and
  `Date.eod()` all resolve against that fixed clock (single mock, all paths).
- Guard against the two-clocks split returning: assert `Date("now").unix == Date.now().unix`
  under a mocked clock.
- This is a vendored copy; the fix and its tests must reach the canonical `mo_times` repo (via
  `svn-sync`) or the next sync reverts them.
