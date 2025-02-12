# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import ToBooleanOp as _ToBooleanOp, TRUE
from mo_json import JX_BOOLEAN
from jx_sqlite.expressions._utils import check
from mo_sqlite import SQLang, SqlScript


class ToBooleanOp(_ToBooleanOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        term = self.term.partial_eval(SQLang)
        if term.jx_type == JX_BOOLEAN or term.missing(SQLang) is TRUE:
            return term.to_sql(schema)
        else:
            return term.exists().to_sql(schema)
