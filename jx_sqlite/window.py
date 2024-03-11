# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_dots import coalesce
from mo_future import text
from mo_sql.utils import sql_aggs
from mo_sqlite import SQL_ORDERBY, sql_iso, sql_list
from mo_sqlite import quote_column


def _window_op(window, schema):
    # http://www2.sqlite.org/cvstrac/wiki?p=UnsupportedSqlAnalyticalFunctions
    if window.value == "rownum":
        return (
            "ROW_NUMBER()-1 OVER ("
            + " PARTITION BY "
            + sql_iso(sql_list(window.edges.values))
            + SQL_ORDERBY
            + sql_iso(sql_list(window.edges.sort))
            + ") AS "
            + quote_column(window.name)
        )

    range_min = str(coalesce(window.range.min, "UNBOUNDED"))
    range_max = str(coalesce(window.range.max, "UNBOUNDED"))

    return (
        sql_aggs[window.aggregate]
        + sql_iso(window.value.to_sql(schema))
        + " OVER ("
        + " PARTITION BY "
        + sql_iso(sql_list(window.edges.values))
        + SQL_ORDERBY
        + sql_iso(sql_list(window.edges.sort))
        + " ROWS BETWEEN "
        + range_min
        + " PRECEDING AND "
        + range_max
        + " FOLLOWING "
        + ") AS "
        + quote_column(window.name)
    )
