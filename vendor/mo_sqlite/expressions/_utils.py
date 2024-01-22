# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from jx_base.expressions.false_op import FalseOp
from jx_base.expressions.null_op import NullOp
from jx_base.expressions.true_op import TrueOp
from jx_base.language import Language
from mo_future import extend
from mo_sqlite.utils import SQL_NULL, SQL_TRUE, SQL_FALSE


@extend(NullOp)
def __iter__(self):
    yield from SQL_NULL


@extend(TrueOp)
def __iter__(self):
    yield from SQL_TRUE


@extend(FalseOp)
def __iter__(self):
    yield from SQL_FALSE


Sqlite = Language("Sqlite")
