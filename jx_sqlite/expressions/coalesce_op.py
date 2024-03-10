# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_sqlite import SQLang, check, SqlScript
from mo_sqlite.expressions import SqlCoalesceOp as _CoalesceOp
from mo_json import union_type, base_type


class CoalesceOp(_CoalesceOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        terms = [t.partial_eval(SQLang).to_sql(schema).expr for t in self.terms]
        data_type = union_type(*(base_type(t.jx_type) for t in terms))

        return SqlScript(jx_type=data_type, expr=_CoalesceOp(*terms), frum=self, schema=schema)
