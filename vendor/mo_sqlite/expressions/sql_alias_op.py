# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import SqlAliasOp as _SqlAliasOp
from mo_sqlite.expressions._utils import SQL
from mo_sql import SQL_AS
from mo_sql import SQL_OP, SQL_CP
from mo_sqlite.utils import quote_column


class SqlAliasOp(_SqlAliasOp, SQL):
    def __iter__(self):
        yield from SQL_OP
        yield from self.value
        yield from SQL_CP
        yield from SQL_AS
        yield from quote_column(self.name)
