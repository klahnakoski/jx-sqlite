# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import NotOp as _NotOp, BasicNotOp
from jx_base.language import is_op, JX
from mo_json import JX_BOOLEAN
from mo_sqlite import SQLang, check, SqlScript


class NotOp(_NotOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        term = self.partial_eval(SQLang)
        if not is_op(term, NotOp):
            return term.to_sql(schema)

        return SqlScript(
            jx_type=JX_BOOLEAN,
            expr=BasicNotOp(term.term).to_sql(schema).expr,
            frum=self,
            miss=term.term.missing(JX),
            schema=schema,
        )
