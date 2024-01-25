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
    EqOp as _EqOp,
    FALSE,
    TRUE,
    is_literal
)
from jx_base.expressions._utils import builtin_ops, simplified
from jx_sqlite.expressions._utils import SQLang, check
from jx_sqlite.expressions.basic_eq_op import BasicEqOp
from jx_sqlite.expressions.case_op import CaseOp
from jx_sqlite.expressions.in_op import InOp
from jx_sqlite.expressions.when_op import WhenOp
from mo_json import JX_ARRAY


class EqOp(_EqOp):
    @check
    def to_sql(self, schema):
        m_rhs = self.rhs.missing(SQLang)
        output = (
            CaseOp(
                WhenOp(self.lhs.missing(SQLang), then=m_rhs),
                WhenOp(m_rhs, then=FALSE),
                BasicEqOp(self.lhs, self.rhs),
            )
            .partial_eval(SQLang)
            .to_sql(schema)
        )
        return output

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
        output = lang.CaseOp(
            lang.WhenOp(lhs.missing(SQLang), then=rhs_missing),
            lang.WhenOp(rhs_missing, then=FALSE),
            lang.BasicEqOp(lhs, rhs),
        ).partial_eval(SQLang)
        return output
