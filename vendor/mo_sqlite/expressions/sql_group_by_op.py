from jx_base import enlist

from jx_base.expressions import SqlGroupByOp as _SqlGroupByOp
from mo_sql import SQL_SELECT, sql_iso, SQL_FROM, SQL_STAR, sql_list, SQL_GROUPBY, SQL


class SqlGroupByOp(SQL, _SqlGroupByOp):
    def __iter__(self):
        yield from SQL_SELECT
        yield from SQL_STAR
        yield from SQL_FROM
        yield from sql_iso(self.frum)
        yield from SQL_GROUPBY
        yield from sql_list(enlist(self.order))
