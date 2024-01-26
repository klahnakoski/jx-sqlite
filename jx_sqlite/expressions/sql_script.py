# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import (
    FALSE,
    SqlScript as _SQLScript,
    TRUE,
    MissingOp,
)
from jx_base.expressions.variable import is_variable
from jx_base.language import is_op
from jx_sqlite.expressions._utils import SQLang, check
from mo_imports import export
from mo_json import JxType
from mo_sqlite import *


class SqlScript(_SQLScript, SQL):
    __slots__ = ("_jx_type", "expr", "frum", "miss", "schema")

    def __init__(self, jx_type, expr, frum, miss=None, schema=None):
        object.__init__(self)
        if expr == None:
            Log.error("expecting expr")
        if not isinstance(expr, SQL):
            Log.error("Expecting SQL")
        if not isinstance(jx_type, JxType):
            Log.error("Expecting JsonType")
        if schema is None:
            Log.error("expecting schema")

        if miss is None:
            self.miss = frum.missing(SQLang)
        else:
            self.miss = miss
        self._jx_type = jx_type
        self.expr = expr
        self.frum = frum  # THE ORIGINAL EXPRESSION THAT MADE expr
        self.schema = schema

    @property
    def name(self):
        return "."

    def __getitem__(self, item):
        if not self.many:
            if item == 0:
                return self
            else:
                Log.error("this is a primitive value")
        else:
            Log.error("do not know how to handle")

    def __iter__(self):
        """
        ASSUMED TO OVERRIDE SQL.__iter__()
        """
        return self._sql().__iter__()

    def to_sql(self, schema):
        return self

    @property
    def sql(self):
        return self._sql()

    def _sql(self):
        self.miss = self.miss.partial_eval(SQLang)
        if self.miss is TRUE:
            return SQL_NULL
        elif self.miss is FALSE or is_variable(self.frum):
            return self.expr

        if is_op(self.miss, MissingOp) and is_variable(self.frum) and self.miss.expr == self.frum:
            return self.expr

        missing = self.miss.to_sql(self.schema)
        if str(missing) == str(SQL_ZERO):
            self.miss = FALSE
            return self.expr
        if str(missing) == str(SQL_ONE):
            self.miss = TRUE
            return SQL_NULL

        return ConcatSQL(SQL_CASE, SQL_WHEN, SQL_NOT, sql_iso(missing), SQL_THEN, self.expr, SQL_END,)

    def __str__(self):
        return str(self._sql())

    def __add__(self, other):
        return text(self) + text(other)

    def __radd__(self, other):
        return text(other) + text(self)

    @check
    def to_sql(self, schema):
        return self

    def missing(self, lang):
        return self.miss

    def __data__(self):
        return {"script": self.script}

    def __eq__(self, other):
        if not isinstance(other, _SQLScript):
            return False
        return self.expr == other.expr


export("jx_sqlite.expressions._utils", SqlScript)
export("jx_sqlite.expressions.or_op", SqlScript)
export("jx_sqlite.expressions.abs_op", SqlScript)
