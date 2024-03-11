# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from typing import Tuple, Dict

from mo_logs import Log
from mo_sql import sql_list, ENABLE_TYPE_CHECKING, SQL_CR, SQL_COMMA, SQL_AS

from jx_base.expressions import SqlVariable
from jx_base.expressions.sql_select_op import SqlSelectOp as _SqlSelectOp
from jx_base.language import is_expression, is_op, Expression
from mo_json import union_type
from mo_sqlite.expressions.sql_alias_op import SqlAliasOp
from mo_sqlite.utils import SQL_SELECT, sql_iso, SQL_FROM, quote_column, TYPE_CHECK
from mo_sqlite.expressions._utils import SQL


class SqlSelectOp(_SqlSelectOp, SQL):
    def __init__(self, frum, *terms: Tuple[SqlAliasOp], **kwargs: Dict[str, Expression]):
        """
        :param terms: list OF SelectOne DESCRIPTORS
        """
        terms = tuple(SqlAliasOp(term, term.es_path[-1]) if is_op(term, SqlVariable) else term for term in terms)

        if TYPE_CHECK and (
            not all(is_op(term, SqlAliasOp) for term in terms) or any(term.name is None for term in terms)
        ):
            Log.error("expecting list of SqlAliasOp")
        Expression.__init__(self, frum, *[t.value for t in terms], *kwargs.values())
        self.frum = frum
        self.terms = terms + tuple(*(SqlAliasOp(v, k) for k, v in kwargs.items()))
        self._jx_type = union_type(*(t.name + t.value.jx_type for t in terms))

    def __iter__(self):
        yield from SQL_SELECT
        yield from sql_list(self.terms)
        yield from SQL_FROM
        yield from sql_iso(self.frum)


class SelectSQL(SQL):
    __slots__ = ["terms", "schema"]

    def __init__(self, terms, schema):
        if ENABLE_TYPE_CHECKING:
            if not isinstance(terms, list) or not all(isinstance(term, dict) for term in terms):
                Log.error("expecting list of dicts")
            if not all(is_expression(term["value"]) for term in terms):
                Log.error("expecting list of dicts with expressions")
        self.terms = terms
        self.schema = schema

    def __iter__(self):
        yield from SQL_SELECT
        comma = SQL_CR
        for term in self.terms:
            name, value = term["name"], term["value"]
            yield from comma
            comma = SQL_COMMA
            yield from value.to_sql(self.schema)
            yield from SQL_AS
            yield from quote_column(name)
