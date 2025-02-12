# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import IsNumberOp as _IsNumberOp, NULL, SqlScript
from jx_sqlite.expressions._utils import check
from mo_json.types import JX_NUMBER


class IsNumberOp(_IsNumberOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        value = self.term.to_sql(schema)
        if value.jx_type == JX_NUMBER:
            return value
        else:
            return NULL
