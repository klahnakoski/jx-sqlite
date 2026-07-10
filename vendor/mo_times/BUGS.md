# mo_times — known defects

## 1. Two independent clocks made "now" unmockable via one point (FIXED — needs tests)

`dates.py` had two separate sources of the current time:

- `unix_now` (`from time import time as unix_now`, line 15) — used by `Date.now()`,
  `Date.eod()`, `Date.today()`.
- `_utcnow` (`from mo_future import utcnow`, line 18) — used by `unicode2Date`'s string
  branches for `"now"/"today"/"eod"/"tomorrow"`.

Consequence: `Date("now")`/`Date("eod")` (and therefore the JX `{"date":"eod"}` operator)
read a **different** clock than `Date.now()`. Mocking one did nothing to the other, so time
could not be pinned in tests with a single mock. Verified via jx-python
`test_aws_complex`, whose `{"date":"eod"}` used the real clock even with `Date.now` mocked.
`datetime.utcnow()` (behind `_utcnow`) is also deprecated in Python 3.12+.

**Fix applied:** the `"now"/"today"/"eod"/"tomorrow"` branches of `unicode2Date` now delegate
to `Date.now()` / `Date.today()` / `Date.eod()`, so `unix_now` is the single clock. The
formulas were already identical, so behavior is unchanged except that everything is now driven
by one mockable function. Verified: with `unix_now` mocked to a fixed value, `Date("now")`,
`Date("today")`, and `Date("eod")` all track it.

**REQUIRED — do not let this regress:**
- Add `mo_times` tests that mock `dates.unix_now` to a fixed value and assert `Date("now")`,
  `Date("today")`, `Date("eod")`, `Date("tomorrow")`, `Date.now()`, `Date.today()`, and
  `Date.eod()` all resolve against that fixed clock (single mock, all paths).
- Guard against the two-clocks split returning: assert `Date("now").unix == Date.now().unix`
  under a mocked clock.
- This is a vendored copy; the fix and its tests must reach the canonical `mo_times` repo (via
  `svn-sync`) or the next sync reverts them.
