# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import (
    SqlScript,
    StrictInOp as _StrictInOp,
    FALSE,
    Literal,
)
from jx_base.expressions.variable import is_variable
from jx_base.language import is_op
from mo_sqlite import SQLang
from jx_sqlite.expressions._utils import check
from jx_sqlite.expressions._utils import value2boolean
from jx_sqlite.expressions.to_list_op import sql_membership
from mo_sqlite.expressions.sql_script import SqlScript
from mo_json.types import JX_BOOLEAN
from mo_logs import Log
from mo_sql import ConcatSQL, SQL_IN
from mo_sqlite import quote_list


class StrictInOp(_StrictInOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        value = self.value.partial_eval(SQLang).to_sql(schema)
        superset = self.superset.partial_eval(SQLang)
        if is_op(superset, Literal):
            values = superset.value
            if value.jx_type == JX_BOOLEAN:
                values = [value2boolean(v) for v in values]
            # TODO: DUE TO LIMITED BOOLEANS, TURN THIS INTO EqOp
            sql = ConcatSQL(value, SQL_IN, quote_list(values))
            return SqlScript(jx_type=JX_BOOLEAN, expr=sql, frum=self, miss=FALSE, schema=schema)

        if not is_variable(superset):
            Log.error("Do not know how to hanldle")

        # MEMBERSHIP IN THE NAME'S COLLECTION - THE STRICT FORM, SO NO null GUARD (SAME AS THE
        # LITERAL SUPERSET ABOVE).  WAS AN ExistsOp/NestedOp DRAFT THAT ASKED schema.get_table FOR
        # THE *NAME*, WHICH IS NOT A TABLE
        return SqlScript(
            jx_type=JX_BOOLEAN,
            expr=sql_membership(value, superset, schema),
            frum=self,
            miss=FALSE,
            schema=schema,
        )
