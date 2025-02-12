# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from jx_base.expressions.sql_select_op import SqlSelectOp as _SqlSelectOp
from mo_sql import sql_list
from mo_sqlite.expressions._utils import SQL
from mo_sqlite.utils import SQL_SELECT, sql_iso, SQL_FROM


class SqlSelectOp(_SqlSelectOp, SQL):
    def __iter__(self):
        yield from SQL_SELECT
        yield from sql_list(self.terms)
        yield from SQL_FROM
        yield from sql_iso(self.frum)
