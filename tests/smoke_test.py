from mo_logs import logger

from jx_sqlite import Container

result = table = (
    Container()
    .get_or_create_facts("my_table")
    .add({"os": "linux", "value": 42})
    .query({"select": "os", "where": {"gt": {"value": 0}}})
)

logger.note("Result = {result|json}", result=result)
