# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import IsBooleanOp as _IsBooleanOp, SqlScript
from jx_sqlite.expressions._utils import check
from jx_sqlite.expressions.variable import typed_leaf
from mo_json.types import JX_BOOLEAN


class IsBooleanOp(_IsBooleanOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        # the boolean leaf of a (possibly union) column; NULL when the value is not boolean
        return typed_leaf(self.term, schema, (JX_BOOLEAN,))
