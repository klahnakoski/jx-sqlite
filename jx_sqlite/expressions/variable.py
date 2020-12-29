# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions import NULL, Variable as Variable_, SelectOp, FALSE
from jx_sqlite.expressions._utils import check, SQLScript
from jx_sqlite.sqlite import Log, quote_column
from jx_sqlite.utils import GUID
from mo_json.types import ToJsonType, T_INTEGER


class Variable(Variable_):
    @check
    def to_sql(self, schema):
        var_name = self.var
        if var_name == GUID:
            output = SQLScript(
                data_type=T_INTEGER,
                expr=quote_column(GUID),
                frum=self,
                miss=FALSE,
                schema=schema,
            )
            return output
        cols = list(schema.leaves(var_name))
        select = []
        for col in cols:
            select.append({
                "name": col.name,
                "value": Variable(col.es_column, ToJsonType(col.jx_type)),
            })

        if len(select) == 0:
            return NULL.to_sql(schema)
        elif len(select) == 1:
            col0 = cols[0]
            output = SQLScript(
                data_type=col0.name + ToJsonType(col0.jx_type),
                expr=quote_column(col0.es_column),
                frum=self,
                schema=schema,
            )
            return output
        else:
            return SelectOp(select).to_sql(schema)
