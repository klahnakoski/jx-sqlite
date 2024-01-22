# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import NULL, Variable as Variable_, SelectOp, FALSE
from jx_base.expressions._utils import simplified
from jx_base.expressions.select_op import SelectOne
from jx_sqlite.expressions._utils import check, SqlScript
from jx_sqlite.utils import GUID
from mo_dots import concat_field, tail_field, Null
from mo_json.types import JX_INTEGER, JxType, to_jx_type
from mo_sqlite import quote_column


class Variable(Variable_):

    @simplified
    def partial_eval(self, lang):
        first, rest = tail_field(self.var)
        if first == "row":
            return Variable(rest)
        return Variable(self.var)

    @check
    def to_sql(self, schema):
        var_name = self.var
        if var_name == GUID:
            output = SqlScript(
                jx_type=JX_INTEGER,
                expr=quote_column(GUID),
                frum=self,
                miss=FALSE,
                schema=schema,
            )
            return output
        cols = list(schema.leaves(var_name))
        select = []
        for rel_name, col in cols:
            select.append(SelectOne(
                concat_field(var_name, rel_name),
                Variable(col.es_column, col.json_type)
            ))

        if len(select) == 0:
            return NULL.to_sql(schema)
        elif len(select) == 1:
            rel_name0, col0 = cols[0]
            base_type = to_jx_type(col0.json_type)
            type0 = JxType(**{concat_field(col0.name, rel_name0): base_type})
            output = SqlScript(
                jx_type=type0,
                expr=quote_column(col0.es_column),
                frum=Variable(self.var, base_type),
                schema=schema,
            )
            return output
        else:
            return SelectOp(Null, *select).to_sql(schema)
