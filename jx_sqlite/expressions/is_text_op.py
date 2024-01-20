# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions.select_op import SelectOp

from jx_base.expressions import IsTextOp as IsTextOp_, NULL
from jx_base.expressions.variable import is_variable
from jx_base.language import is_op
from jx_sqlite.expressions._utils import check
from mo_json.types import JX_TEXT
from mo_logs import logger


class IsTextOp(IsTextOp_):
    @check
    def to_sql(self, schema):
        if is_variable(self.term):
            var_name = self.term.var
        else:
            var_name = "."

        value = self.term.to_sql(schema)
        if is_op(value.frum, SelectOp):
            for t in value.frum.terms:
                if t.jx_type[var_name] == JX_TEXT:
                    return t.value.to_sql(schema)
            return NULL.to_sql(schema)
        elif is_variable(value.frum):
            if value.jx_type == JX_TEXT:
                return value
            else:
                return NULL.to_sql(schema)

        logger.error("not implemented yet")
