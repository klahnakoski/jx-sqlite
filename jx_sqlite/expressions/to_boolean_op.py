# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import ToBooleanOp as _ToBooleanOp, TRUE, FALSE
from mo_json import JX_BOOLEAN
from jx_sqlite.expressions._utils import check
from mo_sqlite import SQLang, SqlScript
from mo_sql import ConcatSQL, SQL, sql_iso


class ToBooleanOp(_ToBooleanOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        term = self.term.partial_eval(SQLang)
        if term.missing(SQLang) is TRUE:
            return term.to_sql(schema)
        sql = term.to_sql(schema)
        # ONLY THE SCHEMA KNOWS IF term IS BOOLEAN; term.jx_type IS UNRESOLVED FOR A Variable
        if sql.jx_type == JX_BOOLEAN:
            return sql
        # keep rows where the SCHEMA-RESOLVED, null-safe value is present. Iterating `sql` renders
        # its miss-wrapped value, so for ordinary terms this equals term.exists(), but it also
        # respects a to_sql that made the value null for a present-but-out-of-class row (is_number's
        # non-numeric leaf, is_integer's fractional) — which the schema-agnostic .missing() cannot see.
        expr = ConcatSQL(SQL("NOT "), sql_iso(ConcatSQL(sql_iso(sql), SQL(" IS NULL"))))
        return SqlScript(jx_type=JX_BOOLEAN, expr=expr, frum=self, miss=FALSE, schema=schema)
