from mo_logs import Log

from jx_sqlite.container import Container

container = Container()
table = container.get_or_create_facts("my_table")
table.add({"os":"linux", "value":42})
result = table.query({
    "select": "os",
    "where": {"gt": {"value": 0}}
})

Log.note("Result = {{result|json}}", result=result)