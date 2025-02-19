# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import LengthOp as _LengthOp, is_literal, IsTextOp
from jx_sqlite.expressions._utils import SqlScript
from jx_sqlite.expressions._utils import check
from mo_json import JX_INTEGER
from mo_sql import sql_call, SQL_NULL
from mo_sqlite import SQLang, quote_value


class LengthOp(_LengthOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        term = self.term.partial_eval(SQLang)
        if is_literal(term):
            val = term.value
            if isinstance(val, str):
                if not val:
                    sql = SQL_NULL
                else:
                    sql = quote_value(len(val))
            else:
                return SQL_NULL
        else:
            value = term.to_sql(schema)
            sql = sql_call("LENGTH", value.expr)
        return SqlScript(
            jx_type=JX_INTEGER, expr=sql, frum=self, miss=IsTextOp(self.term).missing(SQLang), schema=schema,
        )
