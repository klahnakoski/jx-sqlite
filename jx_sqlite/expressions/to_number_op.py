# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import ToNumberOp as NumberOp_
from jx_base.language import is_op
from jx_sqlite.expressions._utils import SQLang, check, SqlScript
from mo_sqlite.sqlite import (
    ConcatSQL,
    SQL_CAST,
    SQL_OP,
    SQL_CP,
    SQL_AS,
    TextSQL,
    json_type_to_sqlite_type,
)
from mo_imports import export
from mo_json import JX_NUMBER, base_type


class ToNumberOp(NumberOp_):
    @check
    def to_sql(self, schema):
        value = self.term.partial_eval(SQLang).to_sql(schema)
        if base_type(value.jx_type) == JX_NUMBER:
            return value

        return SqlScript(
            jx_type=JX_NUMBER,
            expr=ConcatSQL(
                SQL_CAST,
                SQL_OP,
                value,
                SQL_AS,
                TextSQL(json_type_to_sqlite_type[JX_NUMBER]),
                SQL_CP,
            ),
            frum=self,
            miss=self.term.missing(SQLang),
            schema=schema,
        )


export("jx_sqlite.expressions._utils", ToNumberOp)
