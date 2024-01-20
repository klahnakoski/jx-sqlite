# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import FirstOp as _FirstOp
from jx_sqlite.expressions._utils import SQLang, check
from mo_json import base_type, JX_ARRAY
from mo_logs import Log


class FirstOp(_FirstOp):
    @check
    def to_sql(self, schema):
        value = self.frum.partial_eval(SQLang).to_sql(schema)
        type = base_type(value.jx_type)
        if type == JX_ARRAY:
            Log.error("not handled yet")
        return value
