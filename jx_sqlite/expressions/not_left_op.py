# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import (SqlScript,
    NotLeftOp as NotLeftOp_,
    GteOp,
    LengthOp,
    AddOp,
    MaxOp,
    ZERO,
    ONE,
)
from mo_sqlite import SQLang
from mo_sqlite import check
from jx_sqlite.expressions._utils import SqlScript, OrOp
from mo_json import JX_TEXT
from mo_sqlite import sql_call


class NotLeftOp(NotLeftOp_):
    @check
    def to_sql(self, schema) -> SqlScript:
        v = self.value.to_sql(schema)
        start = AddOp(MaxOp(ZERO, self.length), ONE, nulls=False).partial_eval(SQLang).to_sql(schema)

        expr = sql_call("SUBSTR", v, start)
        return SqlScript(
            jx_type=JX_TEXT,
            expr=expr,
            frum=self,
            miss=OrOp(
                self.value.missing(SQLang), self.length.missing(SQLang), GteOp(self.length, LengthOp(self.value)),
            ),
            schema=schema,
        )
