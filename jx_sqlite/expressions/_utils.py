# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import FalseOp, FALSE, ZERO, ONE, SqlScript
from jx_base.expressions.and_op import AndOp
from jx_base.expressions.null_op import NULL, NullOp
from jx_base.expressions.true_op import TrueOp, TRUE
from jx_base.language import Language
from mo_future import extend
from mo_imports import expect
from mo_json.types import JX_IS_NULL, JX_BOOLEAN, JX_NUMBER, JX_INTEGER
from mo_sql import *
from mo_sqlite.expressions import SqlCoalesceOp, SqlScript, SqlGtOp, SqlGteOp, SqlLteOp, SqlLtOp
from mo_sqlite import check, SQLang

ToNumberOp, OrOp = expect("ToNumberOp", "OrOp")

JxSql = Language("JxSql")


@extend(NullOp)
@check
def to_sql(self, schema) -> SqlScript:
    return SqlScript(jx_type=JX_IS_NULL, expr=NULL, frum=self, miss=TRUE, schema=schema)


@extend(TrueOp)
@check
def to_sql(self, schema) -> SqlScript:
    return SqlScript(jx_type=JX_BOOLEAN, expr=TRUE, frum=self, miss=FALSE, schema=schema)


@extend(FalseOp)
@check
def to_sql(self, schema) -> SqlScript:
    return SqlScript(jx_type=JX_BOOLEAN, expr=FALSE, frum=self, miss=FALSE, schema=schema)


def _inequality_to_sql(self, schema):
    iso, op, identity, jx_type = _sql_operators[self.op]

    lhs = ToNumberOp(self.lhs).partial_eval(SQLang).to_sql(schema)
    rhs = ToNumberOp(self.rhs).partial_eval(SQLang).to_sql(schema)

    sql = SqlCoalesceOp(op(lhs.expr, rhs.expr), ZERO)

    return SqlScript(jx_type=JX_BOOLEAN, expr=sql, frum=self, miss=FALSE, schema=schema)


@check
def _binaryop_to_sql(self, schema):
    op, identity = _sql_operators[self.op]

    lhs = ToNumberOp(self.lhs).partial_eval(SQLang).to_sql(schema)
    rhs = ToNumberOp(self.rhs).partial_eval(SQLang).to_sql(schema)

    sql = ConcatSQL(sql_iso(lhs.expr), op, sql_iso(rhs.expr))
    missing = OrOp(self.lhs.missing(SQLang), self.rhs.missing(SQLang))

    return SqlScript(jx_type=JX_NUMBER, expr=sql, frum=self, miss=missing, schema=schema,)


def multiop_to_sql(self, schema):
    iso, sign, zero, jx_type = _sql_operators[self.op]
    if len(self.terms) == 0:
        return NULL.to_sql(schema)

    if self.decisive:
        miss = AndOp(*(t.missing(SQLang) for t in self.terms))
        temp = [SqlCoalesceOp(t, zero).partial_eval(SQLang).to_sql(schema).expr for t in self.terms]
        expr = iso(sign.join(sql_iso(t) for t in temp))
    else:
        miss = OrOp(*(t.missing(SQLang) for t in self.terms), nulls=False)
        expr = iso(sign.join(sql_iso(t.partial_eval(SQLang).to_sql(schema)) for t in self.terms))

    return SqlScript(jx_type=jx_type, expr=expr, frum=self, miss=miss, schema=schema)


def with_var(var, expression, eval):
    """
    :param var: NAME (AS SQL) GIVEN TO expression
    :param expression: THE EXPRESSION TO COMPUTE FIRST
    :param eval: THE EXPRESSION TO COMPUTE SECOND, WITH var ASSIGNED
    :return: PYTHON EXPRESSION
    """
    x = SQL("x")

    return sql_iso(
        SQL_WITH, x, SQL_AS, sql_iso(SQL_SELECT, sql_iso(expression), SQL_AS, var), SQL_SELECT, eval, SQL_FROM, x,
    )


def strict_multiop_to_sql(self, schema, many=False):
    iso, op, identity, jx_type = _sql_operators[self.op.split("strict.")[1]]
    sql = iso(op.join(sql_iso(t.partial_eval(SQLang).to_sql(schema)) for t in self.terms))
    return SqlScript(jx_type=jx_type, frum=self, expr=sql, miss=FALSE, schema=schema,)  # basic operations are "strict"


_sql_operators = {
    # (operator, zero-array default value) TUPLE
    "add": (sql_iso, SQL_PLUS, ZERO, JX_NUMBER),
    "sum": (sql_iso, SQL_PLUS, ZERO, JX_NUMBER),
    "mul": (sql_iso, SQL_STAR, ONE, JX_NUMBER),
    "sub": (sql_iso, SQL(" - "), None, JX_NUMBER),
    "div": (sql_iso, SQL_DIV, NULL, JX_NUMBER),
    "exp": (sql_iso, SQL(" ** "), NULL, JX_NUMBER),
    "mod": (sql_iso, SQL(" % "), NULL, JX_NUMBER),
    "gt": (sql_iso, SqlGtOp, FALSE, JX_BOOLEAN),
    "gte": (sql_iso, SqlGteOp, FALSE, JX_BOOLEAN),
    "lte": (sql_iso, SqlLteOp, FALSE, JX_BOOLEAN),
    "lt": (sql_iso, SqlLtOp, FALSE, JX_BOOLEAN),
    "most": (lambda x: ConcatSQL(SQL("MAX"), SQL_OP, x, SQL_CP), SQL_COMMA, NULL, JX_NUMBER),
    "least": (lambda x: ConcatSQL(SQL("MIN"), SQL_OP, x, SQL_CP), SQL_COMMA, NULL, JX_NUMBER),
    "tally": (sql_iso, SQL_PLUS, ZERO, JX_INTEGER),
}


_v2b = {True: True, "true": True, "T": True, 1: True, False: False, "false": False, "F": False, 0: False, None: None}


def value2boolean(value):
    return _v2b.get(value, True)
