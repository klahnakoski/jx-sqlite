# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions import FALSE, FalseOp, NULL, NullOp, TrueOp, extend
from jx_base.language import Language
from jx_sqlite.sqlite import (
    SQL,
    SQL_FALSE,
    SQL_NULL,
    SQL_TRUE,
    sql_iso,
    SQL_ZERO,
    SQL_ONE,
    SQL_PLUS,
    SQL_STAR,
    SQL_LT,
    ConcatSQL,
    SQL_END,
    SQL_THEN,
    SQL_CASE,
    SQL_WHEN,
    SQL_ELSE,
    quote_column,
    SQL_AS,
    SQL_SELECT,
    SQL_FROM,
    SQL_WITH,
)
from jx_sqlite.sqlite import sql_call
from mo_dots import wrap
from mo_future import decorate
from mo_json import BOOLEAN, NESTED, OBJECT, STRING, NUMBER, IS_NULL, TIME, INTERVAL
from mo_json.types import T_IS_NULL, T_BOOLEAN, T_NUMBER, T_TIME, T_INTERVAL, T_STRING
from mo_logs import Log

ToNumberOp, OrOp, SQLScript = [None] * 3


def check(func):
    """
    TEMPORARY TYPE CHECKING TO ENSURE to_sql() IS OUTPUTTING THE CORRECT FORMAT
    """

    @decorate(func)
    def to_sql(self, schema):
        try:
            output = func(self, schema)
        except Exception as e:
            # output = func(self, schema)
            raise Log.error("not expected", cause=e)
        if not isinstance(output, SQLScript):
            output = func(self, schema)
            Log.error("expecting SQLScript")
        return output

    return to_sql


@extend(NullOp)
@check
def to_sql(self, schema):
    return SQLScript(
        data_type=T_IS_NULL, expr=SQL_NULL, frum=self, miss=TrueOp, schema=schema
    )


@extend(TrueOp)
@check
def to_sql(self, schema):
    return SQLScript(
        data_type=BOOLEAN, expr=SQL_TRUE, frum=self, miss=FalseOp, schema=schema
    )


@extend(FalseOp)
@check
def to_sql(self, schema):
    return SQLScript(
        data_type=BOOLEAN, expr=SQL_FALSE, frum=self, miss=FalseOp, schema=schema
    )


def _inequality_to_sql(self, schema):
    op, identity = _sql_operators[self.op]
    lhs = ToNumberOp(self.lhs).partial_eval(SQLang).to_sql(schema, not_null=True)
    rhs = ToNumberOp(self.rhs).partial_eval(SQLang).to_sql(schema, not_null=True)
    sql = sql_iso(lhs) + op + sql_iso(rhs)

    output = SQLScript(
        data_type=BOOLEAN,
        expr=sql,
        frum=self,
        miss=OrOp([self.lhs.missing(), self.rhs.missing()]),
        schema=schema,
    )
    return output


@check
def _binaryop_to_sql(self, schema):
    op, identity = _sql_operators[self.op]

    lhs = ToNumberOp(self.lhs).partial_eval(SQLang).to_sql(schema, not_null=True)
    rhs = ToNumberOp(self.rhs).partial_eval(SQLang).to_sql(schema, not_null=True)
    script = sql_iso(lhs) + op + sql_iso(rhs)
    if not_null:
        sql = script
    else:
        missing = OrOp([self.lhs.missing(), self.rhs.missing()]).partial_eval(SQLang)
        if missing is FALSE:
            sql = script
        else:
            sql = ConcatSQL(
                SQL_CASE,
                SQL_WHEN,
                missing.to_sql(schema, boolean=True),
                SQL_THEN,
                SQL_NULL,
                SQL_ELSE,
                script,
                SQL_END,
            )
    return wrap([{"name": ".", "sql": {"n": sql}}])


def multiop_to_sql(self, schema, many=False):
    sign, zero = _sql_operators[self.op]
    if len(self.terms) == 0:
        return self.default.partial_eval(SQLang).to_sql(schema)
    elif self.default is NULL:
        return sign.join(
            sql_call("COALESCE", t.partial_eval(SQLang).to_sql(schema), zero)
            for t in self.terms
        )
    else:
        return sql_call(
            "COALESCE",
            sign.join(
                sql_iso(t.partial_eval(SQLang).to_sql(schema)) for t in self.terms
            ),
            self.default.partial_eval(SQLang).to_sql(schema),
        )


def with_var(var, expression, eval):
    """
    :param var: NAME GIVEN TO expression
    :param expression: THE EXPRESSION TO COMPUTE FIRST
    :param eval: THE EXPRESSION TO COMPUTE SECOND, WITH var ASSIGNED
    :return: PYTHON EXPRESSION
    """
    x = SQL("x")

    return ConcatSQL(
        sql_iso(
            SQL_WITH,
            x,
            SQL_AS,
            sql_iso(SQL_SELECT, sql_iso(expression), SQL_AS, quote_column(var)),
        ),
        SQL_SELECT,
        eval,
        SQL_FROM,
        x,
    )


def basic_multiop_to_sql(self, schema, many=False):
    op, identity = _sql_operators[self.op.split("basic.")[1]]
    sql = op.join(sql_iso(t.partial_eval(SQLang).to_sql(schema)) for t in self.terms)
    return wrap([{"name": ".", "sql": {"n": sql}}])


SQLang = Language("SQLang")


_sql_operators = {
    # (operator, zero-array default value) PAIR
    "add": (SQL_PLUS, SQL_ZERO),
    "sum": (SQL_PLUS, SQL_ZERO),
    "mul": (SQL_STAR, SQL_ONE),
    "sub": (SQL(" - "), None),
    "div": (SQL(" / "), None),
    "exp": (SQL(" ** "), None),
    "mod": (SQL(" % "), None),
    "gt": (SQL(" > "), None),
    "gte": (SQL(" >= "), None),
    "lte": (SQL(" <= "), None),
    "lt": (SQL_LT, None),
}

SQL_IS_NULL_TYPE = "0"
SQL_BOOLEAN_TYPE = "b"
SQL_NUMBER_TYPE = "n"
SQL_TIME_TYPE = "t"
SQL_INTERVAL_TYPE = "n"
SQL_STRING_TYPE = "s"
SQL_OBJECT_TYPE = "j"
SQL_NESTED_TYPE = "a"

json_type_to_sql_type = {
    IS_NULL: SQL_IS_NULL_TYPE,
    BOOLEAN: SQL_BOOLEAN_TYPE,
    NUMBER: SQL_NUMBER_TYPE,
    TIME: SQL_TIME_TYPE,
    INTERVAL: SQL_INTERVAL_TYPE,
    STRING: SQL_STRING_TYPE,
    OBJECT: SQL_OBJECT_TYPE,
    NESTED: SQL_NESTED_TYPE,
    T_IS_NULL: SQL_IS_NULL_TYPE,
    T_BOOLEAN: SQL_BOOLEAN_TYPE,
    T_NUMBER: SQL_NUMBER_TYPE,
    T_TIME: SQL_TIME_TYPE,
    T_INTERVAL: SQL_INTERVAL_TYPE,
    T_STRING: SQL_STRING_TYPE,
}

sql_type_to_json_type = {
    None: None,
    "0": IS_NULL,
    "b": BOOLEAN,
    "n": NUMBER,
    "s": STRING,
    "j": OBJECT,
}
