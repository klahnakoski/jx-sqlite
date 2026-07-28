# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import CardinalityOp as _CardinalityOp, FALSE, SqlScript
from jx_sqlite.expressions._utils import check
from jx_sqlite.expressions.to_list_op import ToListOp
from mo_sqlite.expressions.sql_script import SqlScript
from mo_json import JX_INTEGER


class CardinalityOp(_CardinalityOp):
    """
    HOW MANY *DISTINCT* VALUES THE COLLECTION HAS - count WITH DISTINCT ROWS
    """

    @check
    def to_sql(self, schema) -> SqlScript:
        return SqlScript(
            jx_type=JX_INTEGER,
            expr=ToListOp(self.frum).aggregate("COUNT", schema, distinct=True),
            frum=self,
            miss=FALSE,
            schema=schema,
        )
