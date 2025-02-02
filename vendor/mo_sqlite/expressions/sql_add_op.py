# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import AddOp as _AddOp
from mo_sql import NO_SQL, SQL_ADD
from mo_sqlite.expressions._utils import SQL


class AddOp(_AddOp, SQL):
    def __iter__(self):
        op = NO_SQL
        for term in self.terms:
            yield from op
            yield from term
            op = SQL_ADD
