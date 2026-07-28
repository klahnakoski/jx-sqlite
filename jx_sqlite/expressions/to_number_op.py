# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import ToNumberOp as _ToNumberOp
from jx_sqlite.expressions._utils import check, SqlScript
from mo_imports import export
from mo_json import JX_NUMBER, base_type, NUMBER
from mo_sql import ConcatSQL, SQL, sql_iso
from mo_sqlite import json_type_to_sqlite_type, sql_cast


class ToNumberOp(_ToNumberOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        value = self.term.to_sql(schema)
        if base_type(value.jx_type) == JX_NUMBER:
            return value

        x = value.expr
        cast = sql_cast(x, json_type_to_sqlite_type[NUMBER])
        # text -> number PARSES the string, null when it is not a number (decisive).
        # SQLite CAST('x' AS REAL) is 0 not null, so guard with GLOB: the value must
        # hold a digit and contain only numeric characters. Built as one raw SQL leaf
        # (not Sql*Op) so partial_eval treats it as already-simplified, like sql_cast.
        expr = ConcatSQL(
            SQL("CASE WHEN "), sql_iso(x), SQL(" GLOB '*[0-9]*' AND NOT "), sql_iso(x),
            SQL(" GLOB '*[^0-9.eE+-]*' THEN "), cast, SQL(" END"),
        )
        return SqlScript(jx_type=JX_NUMBER, expr=expr, frum=self, miss=value.miss, schema=schema)


export("jx_sqlite.expressions._utils", ToNumberOp)
