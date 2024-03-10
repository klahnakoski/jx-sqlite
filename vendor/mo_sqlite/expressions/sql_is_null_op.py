# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import SqlIsNullOp as _SqlIsNullOp
from mo_sql import SQL_IS_NULL, sql_iso
from mo_sqlite.expressions._utils import SQL


class SqlIsNullOp(_SqlIsNullOp, SQL):
    def __iter__(self):
        yield from sql_iso(self.term)
        yield from SQL_IS_NULL
