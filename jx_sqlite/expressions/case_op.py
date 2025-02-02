# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base import JX
from jx_base.expressions import CaseOp as _CaseOp, SqlScript
from mo_json import union_type
from mo_sqlite import SQLang, check
from mo_sqlite.expressions import SqlWhenOp, SqlCaseOp, SqlScript


class CaseOp(_CaseOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        if len(self.whens) == 1:
            return self.whens[-1].partial_eval(SQLang).to_sql(schema)

        whens = []
        data_type = []
        for w in self.whens[:-1]:
            when = w.when.partial_eval(SQLang).to_sql(schema).expr
            value = w.then.partial_eval(SQLang).to_sql(schema)
            data_type.append(value.jx_type)
            whens.append(SqlWhenOp(when, value.expr))

        value = self.whens[-1].partial_eval(SQLang).to_sql(schema)
        data_type.append(value.jx_type)
        _else = value.expr
        miss = self.missing(JX)
        return SqlScript(
            jx_type=union_type(*data_type), expr=SqlCaseOp(*whens, _else=_else), frum=self, miss=miss, schema=schema,
        )
