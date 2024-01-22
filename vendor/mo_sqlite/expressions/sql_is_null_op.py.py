# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import SqlIsNullOp as SqlIsNullOp_
from mo_sqlite.utils import sql_iso, SQL_IS_NULL, SQL


class SqlIsNullOp(SqlIsNullOp_, SQL):
    def __iter__(self):
        yield from sql_iso(self.term)
        yield from SQL_IS_NULL
