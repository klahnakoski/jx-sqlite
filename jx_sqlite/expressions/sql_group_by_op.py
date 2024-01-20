# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#



from jx_base.expressions import SqlGroupByOp as _SqlGroupByOp, AddOp
from jx_base.language import LanguageElement
from mo_json import JxType, ARRAY, base_type, JX_NUMBER
from mo_logs import logger
from mo_sql.utils import json_type_to_sql_type_key


class SqlGroupByOp(_SqlGroupByOp):
    """
    without aggregation
    this is a hierarchical result; with the grouped columns and the rows that belong to each group
    the sql sould show that one-to-many result

    SELECT
        p.*
    FROM
        parent_table p
    GROUP BY
        p.id
    UNION ALL
    SELECT
        c.parent
        c.b
    FROM
        parent_table p
    LEFT JOIN
        child_table c on c.parent==p.id
    ORDER BY
        p.id,
        c.id
    """

    def apply(self, expr):
        if isinstance(expr, LanguageElement) and expr.op in [AddOp.op]:
            if not base_type(self.frum.type) == JX_NUMBER:
                logger.error("can not handle multiple columns")
            result = AddOp(self.frum)
            SqlGroupByOp(result, self.group)

        result = self.frum.query(expr)
        return SqlGroupByOp(result, self.group)

    @property
    def type(self):
        return JxType(
            group=self.group.type, **{json_type_to_sql_type_key[ARRAY]: self.frum.type},
        )
