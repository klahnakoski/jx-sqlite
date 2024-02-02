# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import CaseOp as _CaseOp
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite import (
    SQL_CASE,
    SQL_ELSE,
    SQL_END,
    SQL_THEN,
    SQL_WHEN,
    ConcatSQL,
)
from mo_json import union_type


class CaseOp(_CaseOp):
    @check
    def to_sql(self, schema):
        if len(self.whens) == 1:
            return self.whens[-1].partial_eval(SQLang).to_sql(schema)

        acc = [SQL_CASE]
        data_type = []
        for w in self.whens[:-1]:
            when = w.when.partial_eval(SQLang).to_sql(schema)
            value = w.then.partial_eval(SQLang).to_sql(schema)
            data_type.append(value.jx_type)
            acc.append(ConcatSQL(SQL_WHEN, when, SQL_THEN, value))

        value = self.whens[-1].partial_eval(SQLang).to_sql(schema)
        data_type.append(value.jx_type)
        acc.append(ConcatSQL(SQL_ELSE, value, SQL_END,))

        return SqlScript(
            jx_type=union_type(*data_type), expr=ConcatSQL(*acc), frum=self, miss=self.missing(SQLang), schema=schema,
        )
