# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import inspect

from jx_base.expressions import (
    FALSE,
    SqlScript as _SqlScript,
    TRUE,
    MissingOp,
)
from jx_base.expressions._utils import TYPE_CHECK
from jx_base.expressions.sql_not_op import SqlNotOp
from jx_base.expressions.variable import is_variable
from jx_base.language import is_op, Expression, Language
from mo_future import extend
from mo_imports import expect
from mo_json import JxType
from mo_logs import Log, logger
from mo_sql import SQL
from mo_sql import SQL_CASE, SQL_END, SQL_NULL, SQL_THEN, SQL_WHEN, SQL_NOT, SQL_OP, SQL_CP

SqlCaseOp, SqlWhenOp = expect("SqlCaseOp", "SqlWhenOp")


SQLang = Language("SQLang")


@extend(SQL)
def to_sql(self, schema):
    return SqlScript(jx_type=self.jx_type, expr=self, frum=self, schema=schema)


class SqlScript(_SqlScript, SQL):
    """
    DESCRIBE AN UNSTRUCTURED SQL SCRIPT
    """

    __slots__ = ("_jx_type", "_expr", "frum", "miss", "schema")

    def __init__(self, jx_type, expr, frum, miss=None, schema=None):
        object.__init__(self)
        if TYPE_CHECK:
            if expr == None or expr is self:
                Log.error("expecting expr")
            if isinstance(expr, _SqlScript):
                Log.error("no SqlScript allowed")
            if not isinstance(expr, SQL):
                Log.error("Expecting SQL")
            if not isinstance(jx_type, JxType):
                Log.error("Expecting JsonType")
            if schema is None:
                Log.error("expecting schema")
        if miss is None:
            miss = frum.missing(SQLang).partial_eval(SQLang)
        if TYPE_CHECK:
            if miss not in [TRUE, FALSE] and isinstance(miss, SQL):
                Log.error("expecting miss to not be SQL")
            if miss.missing(SQLang).partial_eval(SQLang) is not FALSE:
                Log.error("expecting miss to not be missing")

        self.miss = miss
        self._jx_type = jx_type
        self._expr = expr
        self.frum = frum  # THE ORIGINAL EXPRESSION THAT MADE expr
        self.schema = schema

    @property
    def name(self):
        return "."

    @property
    def expr(self):
        if isinstance(self._expr, SQL) and isinstance(self._expr, Expression):
            return self._expr
        return self

    def __getitem__(self, item):
        if not self.many:
            if item == 0:
                return self
            else:
                Log.error("this is a primitive value")
        else:
            Log.error("do not know how to handle")

    def __iter__(self):
        self.miss = self.miss.partial_eval(SQLang)
        if self.miss is TRUE:
            yield from SQL_NULL
            return
        if self.miss is FALSE or is_variable(self.frum):
            yield from self._expr
            return

        if TYPE_CHECK and len(inspect.stack()) > 100:
            logger.alert("stack overflow?")
            return

        if is_op(self.miss, MissingOp) and is_variable(self.frum) and self.miss.expr == self.frum:
            yield from self._expr
            return

        yield from SQL_CASE
        yield from SQL_WHEN
        yield from SQL_NOT
        yield from SQL_OP
        yield from self.miss.to_sql(self.schema)
        yield from SQL_CP
        yield from SQL_THEN
        yield from self._expr
        yield from SQL_END

    @property
    def sql(self):
        return self._sql()

    def _sql(self):
        self.miss = self.miss.partial_eval(SQLang)
        if self.miss is TRUE:
            return SQL_NULL
        elif self.miss is FALSE or is_variable(self.frum):
            return self._expr

        if is_op(self.miss, MissingOp) and is_variable(self.frum) and self.miss.expr == self.frum:
            return self._expr

        try:
            return SqlCaseOp(SqlWhenOp(SqlNotOp(self.miss.to_sql(self.schema).expr, self._expr)))
        except Exception as cause:
            Log.error("not expected", cause=cause)

    def __str__(self):
        return str(self._sql())

    def to_sql(self, schema) -> "SqlScript":
        return self

    def missing(self, lang):
        return self.miss

    def __data__(self):
        return {"script": self.expr}

    def __eq__(self, other):
        if isinstance(self._expr, _SqlScript):
            Log.error("no SqlScript allowed")

        if not isinstance(other, _SqlScript):
            return False

        if TYPE_CHECK and len(inspect.stack()) > 100:
            logger.alert("stack overflow?")
            return

        return self.expr == other.expr
