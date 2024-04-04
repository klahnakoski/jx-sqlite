# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import IsTextOp as IsTextOp_, NULL, SqlScript, SqlSelectOp
from jx_base.expressions.select_op import SelectOp
from jx_base.expressions.variable import is_variable
from jx_base.language import is_op
from mo_sqlite import check
from mo_json.types import JX_TEXT
from mo_logs import logger


class IsTextOp(IsTextOp_):
    @check
    def to_sql(self, schema) -> SqlScript:
        if is_variable(self.term):
            var_name = self.term.var
        else:
            var_name = "."
        # todo - schema has a.$S and a.b.$N inner proberty, but to_sql returns jx_type {$S, $N}, no inner property b
        value = self.term.to_sql(schema)
        if is_op(value.expr, SqlSelectOp):
            for t in value.expr.terms:
                if t.jx_type[var_name] == JX_TEXT:
                    return t.value.to_sql(schema)
            return NULL.to_sql(schema)
        elif is_variable(value.frum):
            if value.jx_type == JX_TEXT:
                return value
            else:
                return NULL.to_sql(schema)

        logger.error("not implemented yet")
