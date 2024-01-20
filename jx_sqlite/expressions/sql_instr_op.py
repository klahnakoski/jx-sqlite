# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import SqlInstrOp as SqlInstrOp_, OrOp
from jx_base.expressions._utils import simplified
from jx_sqlite.expressions._utils import check, SQLang, SqlScript
from mo_sqlite.sqlite import sql_call
from mo_json import JX_INTEGER


class SqlInstrOp(SqlInstrOp_):
    @check
    def to_sql(self, schema):
        value = self.value.to_sql(schema)
        find = self.find.to_sql(schema)

        return SqlScript(
            jx_type=JX_INTEGER,
            expr=sql_call("INSTR", value.expr, find.expr),
            frum=self,
            miss=OrOp(self.value.missing(SQLang), self.find.missing(SQLang)),
            schema=schema,
        )

    @simplified
    def partial_eval(self, lang):
        value = self.value.partial_eval(SQLang)
        find = self.find.partial_eval(SQLang)
        return SqlInstrOp(value, find)
