# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions import InOp as InOp_, FALSE
from jx_base.language import is_op
from jx_sqlite.expressions._utils import SQLang, check, SQLScript
from jx_sqlite.expressions.literal import Literal
from jx_sqlite.sqlite import SQL_FALSE, SQL_IN, ConcatSQL
from jx_sqlite.sqlite import quote_list
from mo_json import T_BOOLEAN
from mo_logs import Log
from pyLibrary.convert import value2boolean


class InOp(InOp_):
    @check
    def to_sql(self, schema):
        if not is_op(self.superset, Literal):
            Log.error("Not supported")
        values = self.superset.value
        if values:
            var = self.value.partial_eval(SQLang).to_sql(schema)
            if var.data_type == T_BOOLEAN:
                values = [value2boolean(v) for v in values]

            sql = ConcatSQL(var, SQL_IN, quote_list(values))
        else:
            sql = SQL_FALSE

        return SQLScript(
            data_type=T_BOOLEAN, expr=sql, frum=self, miss=FALSE, schema=schema
        )
