# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import IsBooleanOp as ToBooleanOp_, FALSE, TRUE, is_literal, SqlScript
from mo_sqlite import SQLang
from jx_sqlite.expressions._utils import check
from mo_json.types import JX_BOOLEAN


class IsBooleanOp(ToBooleanOp_):
    @check
    def to_sql(self, schema) -> SqlScript:
        term = self.term.partial_eval(SQLang)
        if term.jx_type is JX_BOOLEAN:
            return term.to_sql(schema)
        elif is_literal(term) and term.value in ("T", "F"):
            if term.value == "T":
                return TRUE
            else:
                return FALSE
        else:
            return term.exists().partial_eval(SQLang).to_sql(schema)
