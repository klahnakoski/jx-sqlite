# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import SqlCastOp as _SqlCastOp
from mo_sql import SQL_OP, SQL_CP, SQL_CAST
from mo_sql import TextSQL
from mo_sqlite import SQL, SQL_AS


class SqlCastOp(_SqlCastOp, SQL):
    def __iter__(self):
        yield from SQL_CAST
        yield from SQL_OP
        yield from self.value
        yield from SQL_AS
        yield from TextSQL(self.es_type)
        yield from SQL_CP


