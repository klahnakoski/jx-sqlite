# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import AndOp as _AndOp, ToBooleanOp
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite import SQL_AND, SQL_FALSE, SQL_TRUE, sql_iso
from mo_json.types import JX_BOOLEAN


class AndOp(_AndOp):
    @check
    def to_sql(self, schema):
        if not self.terms:
            return SqlScript(jx_type=JX_BOOLEAN, expr=SQL_TRUE, frum=self, schema=schema)
        elif all(self.terms):
            return SqlScript(
                jx_type=JX_BOOLEAN,
                expr=SQL_AND.join([sql_iso(ToBooleanOp(t).partial_eval(SQLang).to_sql(schema)) for t in self.terms]),
                frum=self,
                schema=schema,
            )
        else:
            return SqlScript(jx_type=JX_BOOLEAN, expr=SQL_FALSE, frum=self, schema=schema)
