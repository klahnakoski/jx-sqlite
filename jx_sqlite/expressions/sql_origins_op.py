# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_base.expressions import SqlOriginsOp as _SqlOriginsOp, SqlScript
from jx_base.expressions.sql_left_joins_op import Source


class SqlOriginsOp(_SqlOriginsOp):
    def query(self, query):
        origin = Source(self.origin.alias, self.origin.frum.apply(query), [])
        root = self.root.copy_and_replace(self.origin, origin)
        return SqlOriginsOp(root, origin)
