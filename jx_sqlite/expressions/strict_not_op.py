# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import StrictNotOp as _StrictNotOp, FALSE, SqlScript
from mo_json.types import JX_BOOLEAN
from mo_sqlite import SQLang
from jx_sqlite.expressions._utils import check
from mo_sqlite.expressions import SqlNotOp, SqlScript


class StrictNotOp(_StrictNotOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        term = self.term.partial_eval(SQLang).to_sql(schema)
        return SqlScript(jx_type=JX_BOOLEAN, miss=FALSE, expr=SqlNotOp(term.expr), frum=self, schema=schema,)
