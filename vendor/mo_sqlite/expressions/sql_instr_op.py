# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import SqlInstrOp as _SqlInstrOp, FALSE
from jx_base.expressions._utils import simplified
from mo_json import JX_INTEGER
from mo_sqlite.expressions._utils import SQL
from mo_sql import sql_call
from mo_sqlite.expressions._utils import SQLang
from mo_sqlite.expressions.sql_script import SqlScript


class SqlInstrOp(_SqlInstrOp, SQL):
    def __iter__(self):
        yield from sql_call("INSTR", self.value, self.find)

    @simplified
    def partial_eval(self, lang):
        value = self.value.partial_eval(SQLang)
        find = self.find.partial_eval(SQLang)
        return SqlInstrOp(value, find)

    def to_sql(self, schema) -> SqlScript:
        value = self.value.partial_eval(SQLang).to_sql(schema)
        find = self.find.partial_eval(SQLang).to_sql(schema)
        return SqlScript(jx_type=JX_INTEGER, miss=FALSE, expr=SqlInstrOp(value, find), frum=self, schema=schema)
