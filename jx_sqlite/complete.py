# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_sqlite.expressions import EqOp
from mo_sql import *
from mo_sqlite import *
from mo_sqlite.expressions import SqlVariable, SqlAndOp


def sql_complete(primary, inner_domains, all_domain_names, allow_nulls, index_to_column, schema):
    """
    DOMAIN COMPLETION (P6b, docs\INTERSECTION_SURVEY.md): EMULATE THE FULL OUTER JOIN
    SQLITE LACKS.  CROSS-JOIN THE COMPLETED DOMAINS (EACH PADDED WITH A NULL ROW WHEN
    allow_nulls) AND LEFT JOIN THE primary AGGREGATE BACK ON, SO EVERY COORDINATE HAS A
    RESULT ROW EVEN WHERE THE DATA HAS NONE
    :param primary: PHASE-1 SQL - AGGREGATES OVER THE FACTS JOINED TO THE DOMAINS
    :param inner_domains: THE DOMAIN TABLE SQL, PER EDGE
    :param all_domain_names: THE DOMAIN COLUMN ALIASES, PER EDGE
    :param allow_nulls: PER EDGE - PAD THE DOMAIN WITH THE NULL PART?
    :param index_to_column: EVERY RESULT COLUMN (EDGES AND AGGREGATES), FOR THE SELECT LIST
    :param schema: TO COMPILE THE DECISIVE (NULL-MATCHES-NULL) JOIN EQUALITY
    :return: SQL
    """
    edge_names = [f"e{edge_index}" for edge_index in range(len(inner_domains))]

    outer_domains = []
    for domains_sql, domain_aliases, allow_null in zip(inner_domains, all_domain_names, allow_nulls):
        if allow_null:
            outer_domains.append(ConcatSQL(
                SQL_SELECT,
                sql_list([quote_column(domain_alias) for domain_alias in domain_aliases]),
                SQL_FROM,
                sql_iso(domains_sql),
                SQL_UNION_ALL,
                SQL_SELECT,
                sql_list([SQL_NULL for _ in domain_aliases]),
            ))
        else:
            outer_domains.append(domains_sql)

    clauses = [ConcatSQL(
        SQL_SELECT,
        sql_list([
            quote_column("e" + str(i.push_column_index) if i.is_edge else "p", i.column_alias,)
            for i in index_to_column.values()
        ]),
        SQL_FROM,
        sql_iso(outer_domains[0]),
        SQL_AS,
        quote_column(edge_names[0]),
    )]
    for edge_name, outer_domain in zip(edge_names[1:], outer_domains[1:]):
        clauses.append(ConcatSQL(
            SQL_LEFT_JOIN, sql_iso(outer_domain), SQL_AS, quote_column(edge_name), SQL_ON, SQL_TRUE,
        ))
    clauses.append(ConcatSQL(
        SQL_LEFT_JOIN,
        sql_iso(primary),
        SQL_AS,
        quote_column("p"),
        SQL_ON,
        SqlAndOp(
            *(
                EqOp(SqlVariable("p", d), SqlVariable(e, d)).to_sql(schema).expr
                for e, domain_aliases in zip(edge_names, all_domain_names)
                for d in domain_aliases
            )
        ),
    ))
    return ConcatSQL(*clauses)
