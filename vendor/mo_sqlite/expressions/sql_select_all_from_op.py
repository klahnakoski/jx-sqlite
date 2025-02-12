# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_base.expressions import SqlSelectAllFromOp as _SqlSelectAllFrom
from mo_sqlite import SQL_SELECT, SQL_FROM, SQL_STAR


class SqlSelectAllFromOp(_SqlSelectAllFrom):
    def __iter__(self):
        yield from SQL_SELECT
        yield from SQL_STAR
        yield from SQL_FROM
        yield from self.table
