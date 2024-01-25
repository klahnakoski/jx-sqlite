# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import GetOp as _GetOp, Variable, SelectOp, CoalesceOp
from jx_base.expressions.select_op import SelectOne
from jx_base.expressions.variable import is_variable
from jx_sqlite.expressions._utils import check, SQLang
from mo_dots import Null, concat_field
from mo_logs import Log


class GetOp(_GetOp):
    @check
    def to_sql(self, schema):
        if not is_variable(self):
            Log.error("Can only handle Variable")
        var_name = self.var
        leaves = list(schema.leaves(var_name))
        unique = set(r for r, _ in leaves)

        flat = SelectOp(
            Null,
            *(
                SelectOne(
                    concat_field(var_name, r),
                    CoalesceOp(*(Variable(c.es_column) for rr, c in leaves if rr == r)).partial_eval(SQLang),
                )
                for r in unique
            )
        )
        if len(flat.terms) == 1:
            return flat.terms[0].value.to_sql(schema)

        return flat.partial_eval(SQLang).to_sql(schema)
