# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions import MaxOp as MaxOp_, MissingOp
from jx_sqlite.expressions._utils import SQLang, check, SQLScript
from jx_sqlite.sqlite import sql_iso, sql_list


class MaxOp(MaxOp_):
    @check
    def to_sql(self, schema):
        miss = MissingOp(self).partial_eval(SQLang)
        expr = sql_iso(sql_list(t.to_sql(schema) for t in self.terms.partial_eval()))
        return SQLScript(missing=miss, expr=expr, frum=self)
