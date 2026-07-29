# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import NULL, ToBooleanOp
from jx_sqlite.aggregates import aggregates, per_document_aggregates
from jx_sqlite.domain import sql_domain, sql_complete
from jx_sqlite.utils import sql_join_chain, aggregate_frame
from jx_sqlite.window import _window_op
from mo_dots import first, startswith_field
from mo_future import extend
from mo_logs import Log
from mo_sql import *
from mo_sql.utils import UID
from mo_sqlite import *

# THE ALIAS OF THE INNER (ONE ROW PER COORDINATE PER DOCUMENT) QUERY
DOC = "d"

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
    all_domain_names = []
    ons = []
    join_types = []
    groupby = []
    orderby = []
    inner_domains = []

    for edge_index, query_edge in enumerate(query.edges):
        edge_alias = f"e{edge_index}"
        query_edge_domain = query_edge.domain

        ###################################################################
        # DOMAIN
        ###################################################################
        domain = sql_domain(query, query_edge, edge_index, column_index, inner_schema, from_sql)
        domain_aliases = domain.domain_aliases
        index_to_column.update(domain.column_mappings)
        column_index = domain.next_column_index

        ###################################################################
        # AGGREGATE CLAUSE PARTS
        ###################################################################

        all_domain_names.append(domain_aliases)
        inner_domains.append(domain.domains_sql)

        ons.append(domain.on_clause)
        join_types.append(domain.join_type)

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
    # THE DOCUMENT A COORDINATE COUNTS ONE BY ONE.  A PLAIN TERM CAN NOT COLLAPSE THE DOCUMENTS
    # OF A COORDINATE, AND AN AGGREGATE OVER AN ANCESTOR'S VALUE MUST NOT COUNT THE COPIES THE
    # JOIN CHAIN MADE OF IT - EITHER WAY THAT DOCUMENT BECOMES AN IMPLICIT EDGE: ONE INNER ROW
    # PER (COORDINATE, DOCUMENT), WHICH THE OUTER QUERY COLLAPSES.  AN ALL-AGGREGATE SELECT OVER
    # THE ORIGIN'S OWN VALUES KEEPS THE CLASSIC ONE LEVEL
    frames = {aggregate_frame(s, inner_schema) for s in query.select.terms}
    if frames == {schema.nested_path[0]} and not any(s.aggregate is NULL for s in query.select.terms):
        frame = None
        inner_selects = outer_selects
        aggregates(self, index_to_column, offset, outer_selects, query, inner_schema)
    else:
        if len(frames) > 1:
            # ONE INNER GROUPING SERVES EVERY TERM, SO THE TERMS MUST AGREE ON THE DOCUMENT
            Log.error("select terms count different documents: {{frames}}", frames=sorted(frames))
        frame = first(frames)
        inner_selects = list(outer_selects)  # BOTH LEVELS CARRY THE DOMAIN COLUMNS
        outer_selects = [
            sql_alias(quote_column(DOC, domain_alias), domain_alias)
            for domain_aliases in all_domain_names
            for domain_alias in domain_aliases
        ]
        per_document_aggregates(
            self, index_to_column, offset, outer_selects, inner_selects, query, inner_schema, DOC, frame,
        )

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
    clauses = [ConcatSQL(SQL_SELECT, sql_list(inner_selects), SQL_FROM, ConcatSQL(*from_sql))]
    for t, s, j in zip(join_types, edge_sql, ons):
        clauses.append(ConcatSQL(t, s, SQL_ON, j))
    clauses.append(ConcatSQL(SQL_WHERE, main_filter))
    inner_groupby = list(groupby)
    if frame:
        # THE WHOLE UID CHAIN FROM THE FACT DOWN TO frame, NOT JUST frame's OWN UID: A DOCUMENT
        # WITH NO frame ROW HAS A *NULL* UID, AND SQLITE PUTS EVERY SUCH DOCUMENT IN ONE GROUP
        inner_groupby.extend(
            quote_column(table, UID)
            for table in sorted((t for t in self.snowflake.query_paths if startswith_field(frame, t)), key=len)
        )
    if inner_groupby:
        clauses.append(ConcatSQL(SQL_GROUPBY, sql_list(inner_groupby)))
    command = ConcatSQL(*clauses)

    if frame:
        outer_clauses = [ConcatSQL(
            SQL_SELECT, sql_list(outer_selects), SQL_FROM, sql_alias(sql_iso(command), DOC),
        )]
        if groupby:
            outer_clauses.append(ConcatSQL(
                SQL_GROUPBY,
                sql_list([
                    quote_column(DOC, domain_alias)
                    for domain_aliases in all_domain_names
                    for domain_alias in domain_aliases
                ]),
            ))
        command = ConcatSQL(*outer_clauses)

    # ALL COORDINATES MISSED BY primary DATA
    if query.edges:
        command = sql_complete(
            command,
            inner_domains,
            all_domain_names,
            [query_edge.allowNulls for query_edge in query.edges],
            index_to_column,
            schema,
        )

    if orderby:
        command = ConcatSQL(command, SQL_ORDERBY, sql_list(orderby))

    return command, index_to_column
