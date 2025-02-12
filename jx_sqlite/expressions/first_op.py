# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base import is_op
from jx_base.expressions import FirstOp as _FirstOp, SqlScript
from mo_sqlite import SQLang
from jx_sqlite.expressions._utils import check
from jx_sqlite.expressions._utils import SqlScript
from mo_json import base_type, JX_ARRAY
from mo_logs import Log
from mo_sqlite.expressions import SqlAliasOp


class FirstOp(_FirstOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        value = self.frum.partial_eval(SQLang).to_sql(schema).expr
        if is_op(value, SqlAliasOp):
            value = value.value
        jx_type = base_type(value.jx_type)
        if jx_type == JX_ARRAY:
            Log.error("not handled yet")
        return SqlScript(jx_type=jx_type, expr=value, frum=self, schema=schema)
