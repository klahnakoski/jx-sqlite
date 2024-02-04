from jx_base.expressions import SqlLimitOp as _SqlLimitOp
from mo_sql import SQL_SELECT, sql_iso, SQL_FROM, SQL_STAR, SQL_LIMIT, SQL


class SqlLimitOp(SQL, _SqlLimitOp):

    def __iter__(self):
        yield from SQL_SELECT
        yield from SQL_STAR
        yield from SQL_FROM
        yield from sql_iso(self.frum)
        yield from SQL_LIMIT
        yield from self.limit