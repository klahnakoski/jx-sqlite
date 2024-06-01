from typing import List

from mo_future import flatten

from jx_base.expressions import Expression
from jx_base.expressions.sql_inner_join_op import SqlJoinOne


class SqlLeftJoinOp(Expression):
    def __init__(self, frum, *joins: List[SqlJoinOne]):

        Expression.__init__(self, frum, *flatten((j.join, j.on) for j in joins))
        self.frum = frum
        self.joins = joins

    def __data__(self):
        return {"from": [self.frum.__data__(), *(j.__data__() for j in self.joins)]}
