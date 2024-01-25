# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_sql import SQL

from jx_base.expressions import Variable as Variable_
from mo_sqlite import quote_column


class Variable(Variable_, SQL):

    __new__ = object.__new__

    def __init__(self, var):
        Variable_.__init__(self, var)

    def __iter__(self):
        yield from quote_column(self.var)
