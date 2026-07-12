# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import mo_math
from jx_base.expressions import (
    ToBooleanOp,
    NULL,
    Literal,
    MinOp,
    FALSE,
    SelectOp,
    WhenOp,
    CaseOp,
)
from jx_base.language import is_op
from jx_sqlite.aggregates import aggregates
from jx_sqlite.expressions import EqOp
from jx_sqlite.expressions.tuple_op import TupleOp
from jx_sqlite.utils import (
    ColumnMapping,
    sql_join_chain,
)
from jx_sqlite.window import _window_op
from mo_dots import (
    is_missing,
    Null, coalesce,
)
from mo_future import extend
from mo_json import NUMBER, jx_type_to_json_type, JX_INTEGER
from mo_sql.utils import DIGITS_TABLE
from mo_sql import *
from mo_sqlite import *
from mo_sqlite import quote_value
from mo_sqlite.expressions import SqlVariable, SqlEqOp, SqlAliasOp, SqlAndOp

@extend(Facts)
def _edges_op(self, query, schema):
    query = query.copy()  # WE WILL BE MARKING UP THE QUERY
    index_to_column = {}  # MAP FROM INDEX TO COLUMN (OR SELECT CLAUSE)
    outer_selects = []  # EVERY SELECT CLAUSE (NOT TO BE USED ON ALL TABLES, OF COURSE)
    # TABLES ALIAS AS THEMSELVES: EVERY COMPILED REFERENCE IS VALID IN ONE SHARED SCOPE,
    # AND NO SCHEMA RENAME IS NEEDED (rename_tables TO OPAQUE ALIASES BREAKS Names/leaves)
    required_tables = {c.nested_path[0] for v in query.vars() for _, c in schema.leaves(v)}
    nest_to_alias, from_sql = sql_join_chain(self.snowflake, schema.nested_path[0], required_tables)
    inner_schema = schema

    main_filter = ToBooleanOp(query.where).partial_eval(SQLang).to_sql(inner_schema).expr

    column_index = 0
    edge_names = []
    all_domain_names = []
    ons = []
    join_types = []
    groupby = []
    orderby = []
    inner_domains = []
    outer_domains = []

    for edge_index, query_edge in enumerate(query.edges):
        domain_aliases = []

        def get_domain_alias(c):
            return f"d{edge_index}c{c}"

        edge_alias = f"e{edge_index}"
        edge_names.append(edge_alias)
        query_edge_domain = query_edge.domain
        ###################################################################
        # DOMAIN
        ###################################################################
        if query_edge_domain.type == "set":
            domain_alias = get_domain_alias(column_index)
            domain_aliases.append(domain_alias)
            column_index += 1

            if query_edge.value:
                edge_sql = query_edge.value.partial_eval(SQLang).to_sql(inner_schema)
                domains_sql = SQL_UNION_ALL.join(
                    ConcatSQL(
                        SQL_SELECT,
                        sql_alias(quote_value(coalesce(p.dataIndex, i)), domain_alias),
                        SQL_COMMA,
                        sql_alias(quote_value(p.value), domain_alias + "v"),
                    )
                    for i, p in enumerate(query_edge_domain.partitions)
                )
                join_type = SQL_LEFT_JOIN if query_edge.allowNulls else SQL_INNER_JOIN
                on_clause = SqlEqOp(SqlVariable(edge_alias, domain_alias + "v"), edge_sql)
            elif any(p.where for p in query_edge_domain.partitions):
                if not all(p.where for p in query_edge_domain.partitions):
                    Log.error("expecting all partitions to have `where` clause")

                domains_sql = SQL_UNION_ALL.join(
                    ConcatSQL(SQL_SELECT, sql_alias(quote_value(coalesce(p.dataIndex, i)), domain_alias),)
                    for i, p in enumerate(query_edge_domain.partitions)
                )
                join_type = SQL_LEFT_JOIN if query_edge.allowNulls else SQL_INNER_JOIN
                on_clause = SqlEqOp(
                    SqlVariable(edge_alias, domain_alias, jx_type=JX_INTEGER),
                    CaseOp(*(WhenOp(p.where, then=Literal(p.dataIndex)) for p in query_edge_domain.partitions))
                    .partial_eval(SQLang)
                    .to_sql(inner_schema),
                )
            else:
                raise Log.error("do not know what to do")

            num_sql_columns = len(index_to_column)

            index_to_column[num_sql_columns] = ColumnMapping(
                is_edge=True,
                push_list_name=query_edge.name,
                push_column_name=query_edge.name,
                push_column_index=edge_index,
                push_column_child=".",
                pull=get_pull_func(num_sql_columns, query_edge_domain),
                type=NUMBER,
                sql=FALSE,
                column_alias=domain_alias,
            )

        elif query_edge_domain.type == "default":
            edge_sql = query_edge.value.partial_eval(SQLang).to_sql(inner_schema)

            if is_op(edge_sql.frum, TupleOp):
                domain_aliases = [get_domain_alias(column_index + i) for i, term in enumerate(edge_sql.frum.terms)]
                select_columns = sql_list([
                    sql_alias(term.to_sql(inner_schema), domain_alias)
                    for domain_alias, term in zip(domain_aliases, edge_sql.frum.terms)
                ])
                where_columns = SQL_OR.join([
                    ConcatSQL(quote_column(domain_alias), SQL_IS_NOT_NULL) for domain_alias in domain_aliases
                ])
                groupby_columns = sql_list([quote_column(domain_alias) for domain_alias in domain_aliases])
                orderby_columns = sql_list([quote_column(domain_alias) for domain_alias in domain_aliases])
                on_clause = SQL_AND.join([
                    sql_iso(
                        quote_column(edge_alias, domain_alias),
                        SQL_EQ,
                        term.to_sql(inner_schema),
                        SQL_OR,
                        quote_column(edge_alias, domain_alias),
                        SQL_IS_NULL,
                        SQL_AND,
                        term.to_sql(inner_schema),
                        SQL_IS_NULL,
                    )
                    for domain_alias, term in zip(domain_aliases, edge_sql.frum.terms)
                ])

                for i, term in enumerate(edge_sql.frum.terms):
                    index_to_column[column_index + i] = ColumnMapping(
                        is_edge=True,
                        push_list_name=query_edge.name,
                        push_column_name=query_edge.name,
                        push_column_index=edge_index,
                        num_push_columns=len(query_edge.value.terms),
                        push_column_child=i,
                        pull=get_pull_func(column_index + i),
                        type=jx_type_to_json_type(term.jx_type),
                        sql=FALSE,
                        column_alias=get_domain_alias(column_index + i),
                    )
                column_index += len(edge_sql.frum.terms)
            elif is_op(edge_sql.frum, SelectOp):
                domain_aliases = [get_domain_alias(column_index + i) for i, term in enumerate(edge_sql.frum.terms)]
                select_columns = sql_list([
                    sql_alias(term.value.to_sql(inner_schema), domain_alias)
                    for domain_alias, term in zip(domain_aliases, edge_sql.frum.terms)
                ])
                where_columns = SQL_OR.join([
                    ConcatSQL(quote_column(domain_alias), SQL_IS_NOT_NULL) for domain_alias in domain_aliases
                ])
                groupby_columns = sql_list([quote_column(domain_alias) for domain_alias in domain_aliases])
                orderby_columns = sql_list([quote_column(domain_alias) for domain_alias in domain_aliases])
                on_clause = SQL_AND.join([
                    sql_iso(
                        quote_column(edge_alias, domain_alias),
                        SQL_EQ,
                        term.value.to_sql(inner_schema),
                        SQL_OR,
                        quote_column(edge_alias, domain_alias),
                        SQL_IS_NULL,
                        SQL_AND,
                        term.value.to_sql(inner_schema),
                        SQL_IS_NULL,
                    )
                    for domain_alias, term in zip(domain_aliases, edge_sql.frum.terms)
                ])
                for i, term in enumerate(edge_sql.frum.terms):
                    index_to_column[column_index + i] = ColumnMapping(
                        is_edge=True,
                        push_list_name=query_edge.name,
                        push_column_name=query_edge.name,
                        push_column_index=edge_index,
                        push_column_child=term.name,
                        pull=get_pull_func(column_index + i),
                        type=jx_type_to_json_type(term.jx_type),
                        sql=FALSE,
                        column_alias=get_domain_alias(column_index + i),
                    )
                column_index += len(edge_sql.frum.terms)
            else:
                domain_alias = get_domain_alias(column_index)
                domain_aliases.append(domain_alias)
                select_columns = SqlAliasOp(edge_sql, domain_alias)
                column_index += 1
                where_columns = ConcatSQL(quote_column(domain_alias), SQL_IS_NOT_NULL)
                groupby_columns = quote_column(domain_alias)
                orderby_columns = quote_column(domain_alias)
                on_clause = SqlEqOp(SqlVariable(edge_alias, domain_alias), edge_sql)
                num_sql_columns = len(index_to_column)

                index_to_column[num_sql_columns] = ColumnMapping(
                    is_edge=True,
                    push_list_name=query_edge.name,
                    push_column_name=query_edge.name,
                    push_column_index=edge_index,
                    push_column_child=".",
                    pull=get_pull_func(num_sql_columns, query_edge_domain),
                    type=NUMBER,
                    sql=FALSE,
                    column_alias=domain_alias,
                )

            limit = MinOp(query.limit, query_edge_domain.limit).partial_eval(SQLang).to_sql(inner_schema)

            domains_sql = ConcatSQL(
                SQL_SELECT,
                sql_alias(sql_count(SQL_ONE), "num"),
                SQL_COMMA,
                select_columns,
                SQL_FROM,
                ConcatSQL(*from_sql),
                SQL_WHERE,
                where_columns,
                SQL_GROUPBY,
                groupby_columns,
                SQL_ORDERBY,
                sql_count(SQL_ONE),
                SQL_DESC,
                SQL_COMMA,
                orderby_columns,
                SQL_LIMIT,
                limit,
            )

            join_type = SQL_LEFT_JOIN if query_edge.allowNulls else SQL_INNER_JOIN
        elif query_edge_domain.type in ("duration", "time", "range"):
            domain_alias = get_domain_alias(column_index)
            domain_aliases.append(domain_alias)
            column_index += 1

            if query_edge.value:
                edge_sql = query_edge.value.partial_eval(SQLang).to_sql(inner_schema)
                domains_sql = range_sql(
                    domain=query_edge_domain,
                    min_value_name=domain_alias + "v",
                    max_value_name=domain_alias + "max",
                    index_name=domain_alias,
                )
                limit = MinOp(query.limit, query_edge_domain.limit).partial_eval(SQLang).to_sql(inner_schema)
                if limit is not NULL:
                    domains_sql = ConcatSQL(domains_sql, SQL_LIMIT, limit)
                join_type = SQL_LEFT_JOIN if query_edge.allowNulls else SQL_INNER_JOIN
                on_clause = ConcatSQL(
                    quote_column(edge_alias, domain_alias + "v"),
                    SQL_LE,
                    edge_sql,
                    SQL_AND,
                    edge_sql,
                    SQL_LT,
                    quote_column(edge_alias, domain_alias + "max"),
                )
            elif query_edge.range:
                min_sql = query_edge.range.min.partial_eval(SQLang).to_sql(inner_schema)
                max_sql = query_edge.range.max.partial_eval(SQLang).to_sql(inner_schema)
                domains_sql = range_sql(
                    domain=query_edge_domain,
                    min_value_name=domain_alias + "v",
                    max_value_name=domain_alias + "max",
                    index_name=domain_alias,
                )
                limit = MinOp(query.limit, query_edge_domain.limit).partial_eval(SQLang).to_sql(inner_schema)
                domains_sql = ConcatSQL(domains_sql, SQL_LIMIT, limit,)
                join_type = SQL_INNER_JOIN
                on_clause = ConcatSQL(
                    quote_column(edge_alias, domain_alias + "v"),
                    SQL_LT,
                    max_sql,
                    SQL_AND,
                    min_sql,
                    SQL_LT,
                    quote_column(edge_alias, domain_alias + "max"),
                )
            else:
                raise Log.error("do not know how to handle")

            num_sql_columns = len(index_to_column)

            index_to_column[num_sql_columns] = ColumnMapping(
                is_edge=True,
                push_list_name=query_edge.name,
                push_column_name=query_edge.name,
                push_column_index=edge_index,
                push_column_child=".",
                pull=get_pull_func(num_sql_columns, query_edge_domain),
                type=NUMBER,
                sql=FALSE,
                column_alias=domain_alias,
            )
        else:
            raise Log.error("not handled")

        ###################################################################
        # AGGREGATE CLAUSE PARTS
        ###################################################################

        all_domain_names.append(domain_aliases)
        inner_domains.append(domains_sql)
        if query_edge.allowNulls:
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

        ons.append(on_clause)
        join_types.append(join_type)

        groupby.append(sql_list([quote_column(edge_alias, domain_alias) for domain_alias in domain_aliases]))

        full_domain_aliases = [quote_column(edge_alias, domain_alias) for domain_alias in domain_aliases]
        outer_selects.extend([
            sql_alias(full_domain_alias, domain_alias)
            for full_domain_alias, domain_alias in zip(full_domain_aliases, domain_aliases)
        ])

        for full_domain_alias in full_domain_aliases:
            orderby.append(ConcatSQL(full_domain_alias, SQL_IS_NULL))
            if hasattr(query_edge_domain, "sort") and query_edge_domain.sort == -1:
                orderby.append(full_domain_alias + SQL_DESC)
            else:
                orderby.append(full_domain_alias)

    ###################################################################
    # AGGREGATE CLAUSE PARTS
    ###################################################################
    offset = len(query.edges)
    aggregates(self, index_to_column, offset, outer_selects, query, inner_schema)

    for w in query.window:
        outer_selects.append(_window_op(w, schema))

    edge_sql = []
    for edge_index, query_edge in enumerate(query.edges):
        edge_alias = "e" + str(edge_index)
        domains_sql = inner_domains[edge_index]
        edge_sql.append(sql_alias(sql_iso(domains_sql), edge_alias))

    # COORDINATES OF ALL primary DATA
    # THE JOIN CHAIN IS INLINED (NO FACTS SUBQUERY): EVERY COMPILED REFERENCE - AGGREGATES,
    # EDGE VALUES IN THE ON CLAUSES, THE WHERE - RESOLVES IN THIS ONE SCOPE
    clauses = [ConcatSQL(SQL_SELECT, sql_list(outer_selects), SQL_FROM, ConcatSQL(*from_sql))]
    for t, s, j in zip(join_types, edge_sql, ons):
        clauses.append(ConcatSQL(t, s, SQL_ON, j))
    clauses.append(ConcatSQL(SQL_WHERE, main_filter))
    if groupby:
        clauses.append(ConcatSQL(SQL_GROUPBY, sql_list(groupby)))
    command = ConcatSQL(*clauses)

    # ALL COORDINATES MISSED BY primary DATA
    if query.edges:
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
            sql_iso(command),
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
        command = ConcatSQL(*clauses)

    if orderby:
        command = ConcatSQL(command, SQL_ORDERBY, sql_list(orderby))

    return command, index_to_column


def range_sql(domain, min_value_name, max_value_name, index_name):
    if domain.interval == None:
        # IRREGULAR RANGE, SPECIFIC PARTITIONS EXPECTED
        return SQL_UNION_ALL.join(
            ConcatSQL(
                SQL_SELECT,
                sql_alias(quote_value(part.min), min_value_name),
                SQL_COMMA,
                sql_alias(quote_value(part.max), max_value_name),
                SQL_COMMA,
                sql_alias(quote_value(part.dataIndex), index_name),
            )
            for part in domain.partitions
        )

    width = (domain.max - domain.min) / domain.interval
    digits = mo_math.floor(mo_math.log10(width - 1))
    if digits == 0:
        rownum_sql = quote_column("a", "value")
    else:
        rownum_sql = SQL_PLUS.join(
            ConcatSQL(quote_value(pow(10, j)), SQL_STAR, quote_column(str(chr(ord(b"a") + j)), "value"),)
            for j in range(digits + 1)
        )

    if domain.interval == 1:
        if domain.min == 0:
            domain = ConcatSQL(
                SQL_SELECT,
                sql_list([
                    sql_alias(rownum_sql, index_name),
                    sql_alias(rownum_sql, min_value_name),
                    sql_alias(ConcatSQL(rownum_sql, SQL_PLUS, SQL_ONE), max_value_name),
                ]),
                SQL_FROM,
                sql_alias(quote_column(DIGITS_TABLE), "a"),
            )
        else:
            domain = ConcatSQL(
                SQL_SELECT,
                sql_list([
                    sql_alias(rownum_sql, index_name),
                    sql_alias(ConcatSQL(rownum_sql, SQL_PLUS, quote_value(domain.min)), min_value_name,),
                    sql_alias(ConcatSQL(rownum_sql, SQL_PLUS, quote_value(domain.min + 1)), max_value_name,),
                ]),
                SQL_FROM,
                sql_alias(quote_column(DIGITS_TABLE), "a"),
            )
    else:
        if domain.min == 0:
            domain = ConcatSQL(
                SQL_SELECT,
                sql_list([
                    sql_alias(rownum_sql, index_name),
                    sql_alias(ConcatSQL(rownum_sql, SQL_STAR, quote_value(domain.interval)), min_value_name,),
                    sql_alias(
                        ConcatSQL(
                            rownum_sql, SQL_STAR, quote_value(domain.interval), SQL_PLUS, quote_value(domain.interval),
                        ),
                        max_value_name,
                    ),
                ]),
                SQL_FROM,
                sql_alias(quote_column(DIGITS_TABLE), "a"),
            )
        else:
            domain = ConcatSQL(
                SQL_SELECT,
                sql_list([
                    sql_alias(rownum_sql, index_name),
                    sql_alias(
                        ConcatSQL(
                            sql_iso(rownum_sql, SQL_STAR, quote_value(domain.interval)),
                            SQL_PLUS,
                            quote_value(domain.min),
                        ),
                        min_value_name,
                    ),
                    sql_alias(
                        ConcatSQL(
                            sql_iso(rownum_sql, SQL_STAR, quote_value(domain.interval)),
                            SQL_PLUS,
                            quote_value(domain.min + domain.interval),
                        ),
                        max_value_name,
                    ),
                ]),
                SQL_FROM,
                sql_alias(quote_column(DIGITS_TABLE), "a"),
            )

    for j in range(digits):
        domain = ConcatSQL(
            domain,
            SQL_INNER_JOIN,
            sql_alias(quote_column(DIGITS_TABLE), str(chr(ord(b"a") + j + 1))),
            SQL_ON,
            SQL_TRUE,
        )
    domain = ConcatSQL(
        domain, SQL_WHERE, rownum_sql, SQL_LT, quote_value(width), SQL_ORDERBY, quote_column(index_name),
    )
    return domain


def get_pull_func(column, domain=None):
    if domain is None:

        def simple_pull_func(row):
            value = row[column]
            if is_missing(value):
                return Null
            return value

        return simple_pull_func
    else:

        def pull_func(row):
            index = row[column]
            if is_missing(index):
                return Null
            # TODO: OPPORTUNITY TO RETURN DOMAIN OBJECT, NOT JUST KEY VALUE
            return domain.getKeyByIndex(index)

        return pull_func
