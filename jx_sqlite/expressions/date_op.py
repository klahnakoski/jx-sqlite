# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import DateOp as _DateOp
from jx_sqlite.expressions._utils import check
from jx_sqlite.expressions.sql_script import SqlScript
from mo_sqlite.sqlite import quote_value


class DateOp(_DateOp):
    @check
    def to_sql(self, schema):
        value = self.value
        return SqlScript(jx_type=self.data_type, expr=quote_value(value), frum=self, schema=schema)
