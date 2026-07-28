# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import ToNumberOp as _ToNumberOp
from jx_sqlite.expressions._utils import check, SqlScript
from mo_imports import export
from mo_json import JX_NUMBER, base_type, NUMBER
from mo_sqlite import json_type_to_sqlite_type, sql_cast


class ToNumberOp(_ToNumberOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        value = self.term.to_sql(schema)
        if base_type(value.jx_type) == JX_NUMBER:
            return value

        return SqlScript(
            jx_type=JX_NUMBER,
            expr=sql_cast(value.expr, json_type_to_sqlite_type[NUMBER]),
            frum=self,
            miss=value.miss,
            schema=schema,
        )


export("jx_sqlite.expressions._utils", ToNumberOp)
