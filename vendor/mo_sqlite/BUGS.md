# mo_sqlite — known defects

## 1. `SqlScript.__iter__` silently truncated deep renders (FIXED)

```python
if TYPE_CHECK and len(inspect.stack()) > 100:
    logger.alert("stack overflow?")
    return
```

A render whose Python stack was deeper than 100 frames yielded **nothing** — the script
vanished from the middle of the SQL text, producing invalid SQL (`COALESCE( , 0)`) rather than
an error. 100 frames is nothing: rendering nests `yield from` several frames per SQL node, so
any moderately nested expression trips it, and whether it trips depends on how deep the
interpreter already was — the same test passed under a plain script and failed under
`python -m unittest` (deeper base stack from runpy). Found while giving decisive `max`/`min` a
null-skipping form (`SELECT MAX(c) FROM (SELECT (a) AS c UNION ALL SELECT (b) AS c)`), which
adds a few frames per nesting level.

Removed the guard: real runaway recursion (`SqlScript.expr` returns `self` when `_expr` is not
an Expression) still raises `RecursionError`, which is loud and correct. `inspect.stack()` also
builds source-context frame records — it was being paid on every render.

**Still open:** the same guard remains in `SqlScript.__eq__` (same file), where tripping it
`return`s `None` instead of a bool — a silent wrong answer rather than invalid SQL, so it was
left alone; it should get the same treatment.
