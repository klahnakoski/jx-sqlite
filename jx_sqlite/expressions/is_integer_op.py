# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import IsIntegerOp as _IsIntegerOp
from jx_sqlite.expressions._utils import check, SqlScript
from jx_sqlite.expressions.variable import typed_leaf
from mo_json.types import JX_INTEGER, JX_NUMBER, base_type
from mo_sql import ConcatSQL, SQL, sql_iso, sql_cast


class IsIntegerOp(_IsIntegerOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        num = typed_leaf(self.term, schema, (JX_NUMBER, JX_INTEGER))
        if base_type(num.jx_type) not in (JX_NUMBER, JX_INTEGER):
            return num  # no numeric leaf -> already NULL

        # integers store as $N (REAL); an integer is a whole number (2.7 -> null, 5.0 -> 5).
        # the wholeness lives in the value (null for a fractional); raw SQL so partial_eval leaves
        # it be. miss stays the leaf null-check (a valid jx expr); ToBooleanOp keys off the rendered
        # null-safe value, so the where-clause drops fractionals too.
        n = num.expr
        expr = ConcatSQL(
            SQL("CASE WHEN CAST("), sql_iso(n), SQL(" AS INTEGER) = "), sql_iso(n),
            SQL(" THEN "), sql_cast(n, "INTEGER"), SQL(" END"),
        )
        return SqlScript(jx_type=JX_INTEGER, expr=expr, frum=self, miss=num.miss, schema=schema)
