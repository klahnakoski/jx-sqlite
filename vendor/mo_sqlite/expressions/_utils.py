# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from jx_base.expressions import FalseOp, NullOp, TrueOp
from mo_future import extend, decorate
from mo_logs import Log
from mo_sqlite.expressions.sql_script import SqlScript, SQLang, SQL
from mo_sqlite.utils import SQL_NULL, SQL_TRUE, SQL_FALSE, TYPE_CHECK


__all__ = ["SQLang", "SqlScript", "SQL"]


@extend(NullOp)
def __iter__(self):
    yield from SQL_NULL


@extend(TrueOp)
def __iter__(self):
    yield from SQL_TRUE


@extend(FalseOp)
def __iter__(self):
    yield from SQL_FALSE
