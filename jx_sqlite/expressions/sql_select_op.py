# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from dataclasses import dataclass
from typing import Dict, Tuple, Optional

from jx_base.expressions import NULL
from jx_base.expressions.expression import Expression
from jx_base.expressions.select_op import SelectOp as _SelectOp
from jx_sqlite.expressions.sql_script import SqlScript
from mo_json import JxType, JX_INTEGER
from mo_sqlite import sql_alias, ConcatSQL, SQL_SELECT, sql_list, SQL_FROM, sql_iso


class SelectOp(_SelectOp):
    def __init__(self, frum, selects: Tuple[Dict[str, Expression]]):
        _SelectOp.__init__(self, frum, selects)

    def apply(self, container):
        print("details")

    def to_sql(self, schema):
        return SqlScript(
            data_type=self.type,
            expr=ConcatSQL(
                SQL_SELECT,
                sql_list([sql_alias(v.to_sql(schema), n) for n, v in self.selects]),
                sql_iso(SQL_FROM),
                self.frum.to_sql(schema),
            ),
            frum=self,
            schema=schema,
        )


@dataclass
class About:
    func_name: str
    zero: float
    type: Optional[JxType]


_count = About("COUNT", 0, JX_INTEGER)
_min = About("MIN", NULL, None)
_max = About("MAX", NULL, None)
_sum = About("SUM", NULL, None)
_avg = About("AVG", NULL, None)


sql_aggregates = {
    "count": _count,
    "min": _min,
    "minimum": _min,
    "max": _max,
    "maximum": _max,
    "add": _sum,
    "sum": _sum,
    "avg": _avg,
    "average": _avg,
    "mean": _avg,
}
