# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import SqlConcatOp as _SqlConcatOp
from mo_sql import NO_SQL, SQL_CONCAT, SQL


class SqlConcatOp(_SqlConcatOp, SQL):
    def __iter__(self):
        op = NO_SQL
        for term in self.terms:
            yield from op
            yield from term
            op = SQL_CONCAT
