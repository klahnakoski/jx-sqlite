# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import AvgOp as _AvgOp, SqlScript
from jx_sqlite.expressions._utils import check
from jx_sqlite.expressions.to_list_op import ToListOp
from mo_sqlite.expressions.sql_script import SqlScript
from mo_json import JX_NUMBER


class AvgOp(_AvgOp):
    """
    THE MEAN OF THE COLLECTION; AVG SKIPS NULL ROWS, SO IT IS DECISIVE, AND IT IS NULL ONLY WHEN
    THE COLLECTION HAS NO VALUES
    """

    @check
    def to_sql(self, schema) -> SqlScript:
        return SqlScript(
            jx_type=JX_NUMBER, expr=ToListOp(self.frum).aggregate("AVG", schema), frum=self, schema=schema,
        )
