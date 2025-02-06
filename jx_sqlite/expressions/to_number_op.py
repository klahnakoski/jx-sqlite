# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import ToNumberOp as _ToNumberOp, SqlScript
from mo_sqlite import SQLang
from mo_sqlite import check
from jx_sqlite.expressions._utils import SqlScript
from mo_imports import export
from mo_json import JX_NUMBER, base_type, NUMBER
from mo_sqlite import json_type_to_sqlite_type
from mo_sqlite.expressions.sql_cast_op import SqlCastOp


class ToNumberOp(_ToNumberOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        value = self.term.partial_eval(SQLang).to_sql(schema)
        if base_type(value.jx_type) == JX_NUMBER:
            return value

        return SqlScript(
            jx_type=JX_NUMBER,
            expr=SqlCastOp(value, json_type_to_sqlite_type[NUMBER]),
            frum=self,
            miss=self.term.missing(SQLang),
            schema=schema,
        )


export("jx_sqlite.expressions._utils", ToNumberOp)
