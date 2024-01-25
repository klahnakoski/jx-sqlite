from jx_sqlite import Container
from mo_logs import Log

container = Container()
table = container.get_or_create_facts("my_table")
table.add({"os":"linux", "value":42})
result = table.query({
    "select": "os",
    "where": {"gt": {"value": 0}}
})

Log.note("Result = {{result|json}}", result=result)
