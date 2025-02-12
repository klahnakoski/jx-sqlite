# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base import builtin_ops, simplified
from jx_base.expressions import EqOp as _EqOp, FALSE, TRUE, is_literal, NotOp
from jx_sqlite.expressions._utils import value2boolean
from mo_json import JX_ARRAY, ARRAY, JX_BOOLEAN
from mo_logs import logger
from jx_sqlite.expressions._utils import check
from mo_sqlite import SQLang, SqlScript
from mo_sqlite.expressions import SqlEqOp, SqlCaseOp, SqlWhenOp, SqlInOp, SqlNotOp, SqlAndOp, SqlOrOp


class EqOp(_EqOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        if is_literal(self.rhs) and self.rhs.jx_type == ARRAY:
            return SqlInOp(self.lhs, self.rhs).to_sql(schema)

        lhs = self.lhs.to_sql(schema)
        if is_literal(self.rhs) and lhs.jx_type == JX_BOOLEAN:
            # SPECIAL CASE FOR BOOLEAN
            rhs = value2boolean(self.rhs.value)
            lhs_exists = NotOp(lhs.missing(SQLang)).to_sql(schema).expr
            if rhs is True:
                return SqlScript(
                    jx_type=JX_BOOLEAN, expr=SqlAndOp(lhs_exists, lhs.expr), frum=self, miss=FALSE, schema=schema
                )
            elif rhs is False:
                return SqlScript(
                    jx_type=JX_BOOLEAN,
                    expr=SqlAndOp(lhs_exists, SqlNotOp(lhs.expr)),
                    frum=self,
                    miss=FALSE,
                    schema=schema,
                )
            else:
                logger.error("not expected")

        m_rhs = self.rhs.missing(SQLang).to_sql(schema).expr
        output = SqlCaseOp(
            SqlWhenOp(self.lhs.missing(SQLang).to_sql(schema).expr, then=m_rhs),
            SqlWhenOp(m_rhs, then=FALSE),
            _else=SqlEqOp(self.lhs.to_sql(schema).expr, self.rhs.to_sql(schema).expr),
        ).partial_eval(SQLang)
        return SqlScript(jx_type=JX_BOOLEAN, expr=output, frum=self, miss=FALSE, schema=schema)

    @simplified
    def partial_eval(self, lang):
        lhs = self.lhs.partial_eval(SQLang)
        rhs = self.rhs.partial_eval(SQLang)

        if is_literal(lhs):
            if is_literal(rhs):
                return TRUE if builtin_ops["eq"](lhs.value, rhs.value) else FALSE
            lhs, rhs = rhs, lhs
        if is_literal(rhs) and rhs.jx_type in JX_ARRAY:
            return lang.InOp(lhs, rhs).partial_eval(lang)

        rhs_missing = rhs.missing(SQLang)
        output = (
            lang
            .CaseOp(
                lang.WhenOp(lhs.missing(SQLang), then=rhs_missing),
                lang.WhenOp(rhs_missing, then=FALSE),
                lang.SqlEqOp(lhs, rhs),
            )
            .partial_eval(SQLang)
        )
        return output
