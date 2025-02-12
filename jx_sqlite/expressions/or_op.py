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
from jx_base.expressions import OrOp as _OrOp, SqlScript
from jx_base.expressions.false_op import FALSE
from mo_imports import export
from mo_json import JX_BOOLEAN
from jx_sqlite.expressions._utils import check
from mo_sqlite import SQLang
from mo_sqlite import SqlScript
from mo_sqlite.expressions import SqlOrOp


class OrOp(_OrOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        this = self.partial_eval(SQLang)
        if not is_op(this, OrOp):
            return this.to_sql(schema)
        return SqlScript(
            jx_type=JX_BOOLEAN,
            miss=FALSE,
            expr=SqlOrOp(*(t.to_sql(schema).expr for t in self.terms)),
            frum=self,
            schema=schema,
        )


export("jx_sqlite.expressions._utils", OrOp)
