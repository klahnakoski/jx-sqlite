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
from jx_sqlite.expressions._utils import SqlScript
from jx_sqlite.utils import GUID
from mo_dots import concat_field, tail_field, startswith_field
from mo_json.types import JX_INTEGER, JxType, to_jx_type, STRING, union_type, JX_TEXT
from mo_logs import logger
from mo_sqlite import check
from mo_sqlite import json_type_to_sqlite_type
from mo_sqlite.expressions import SqlVariable, SqlCoalesceOp


class Variable(Variable_):
    @simplified
    def partial_eval(self, lang):
        first, rest = tail_field(self.var)
        if first == "row":
            return Variable(rest)
        return Variable(self.var)

    @check
    def to_sql(self, schema) -> SqlScript:
        var_name = self.var
        if startswith_field(var_name, "row"):
            _, var_name = tail_field(var_name)

        if var_name == GUID:
            output = SqlScript(
                jx_type=JX_INTEGER,
                expr=SqlVariable(schema.nested_path[0], GUID, jx_type=JX_TEXT),
                frum=self,
                miss=FALSE,
                schema=schema,
            )
            return output
        cols = list(schema.leaves(var_name))
        select = []

        if len(cols) == 0:
            return NULL.to_sql(schema)
        elif len(cols) == 1:
            _, col = cols[0]
            return SqlScript(
                jx_type=to_jx_type(col.es_type),
                expr=SqlVariable(col.es_index, col.es_column, jx_type=to_jx_type(col.es_type)),
                frum=self,
                schema=schema,
            )
        elif len(set(n for n, _ in cols)) == 1:
            return SqlScript(
                jx_type = union_type(*(to_jx_type(c.es_type) for _, c in cols)),
                expr = SqlCoalesceOp(*(
                    SqlVariable(c.es_index, c.es_column, jx_type=to_jx_type(c.es_type))
                    for _, c in cols
                )),
                frum = self,
                schema = schema,
            )

        logger.warning("not expected")
        for rel_name, col in cols:
            select.append(SelectOne(
                concat_field(var_name, rel_name), SqlVariable(col.es_index, col.es_column, jx_type=to_jx_type(col.es_type))
            ))
        return SelectOp(schema, *select).to_sql(schema)
