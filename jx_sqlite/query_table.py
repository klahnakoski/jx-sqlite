# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import mo_json
from jx_base import Column, Facts
from jx_base.domains import SimpleSetDomain
from jx_base.expressions import TupleOp, jx_expression, QueryOp, SelectOp, NULL
from jx_base.expressions.select_op import normalize_one
from jx_base.language import is_op
from jx_base.models.container import type2container
from jx_python import jx
from jx_sqlite.expressions._utils import SQLang
from jx_sqlite.groupby_table import GroupbyTable
from jx_sqlite.utils import GUID, sql_aggs, unique_name, untyped_column
from mo_collections.matrix import Matrix, index_to_coordinate
from mo_dots import (
    Data,
    Null,
    to_data,
    coalesce,
    concat_field,
    is_list,
    listwrap,
    relative_field,
    startswith_field,
    unwraplist,
    wrap,
    list_to_data, from_data,
)
from mo_future import text, transpose, is_text
from mo_json import STRING, STRUCT
from mo_logs import Log
from mo_sqlite import (
    SQL_FROM,
    SQL_ORDERBY,
    SQL_SELECT,
    SQL_WHERE,
    sql_count,
    sql_iso,
    sql_list,
    SQL_CREATE,
    SQL_AS,
    SQL_DELETE,
    ConcatSQL,
    JoinSQL,
    SQL_COMMA,
)
from mo_sqlite import quote_column, sql_alias
from mo_threads import register_thread


class QueryTable(GroupbyTable):
    def __init__(self, name, container):
        Facts.__init__(self, name, container)

    def get_column_name(self, column):
        return relative_field(column.name, self.snowflake.fact_name)

    @register_thread
    def __len__(self):
        counter = self.container.db.query(ConcatSQL(
            SQL_SELECT, sql_count("*"), SQL_FROM, quote_column(self.snowflake.fact_name)
        ))[0][0]
        return counter

    def __nonzero__(self):
        return bool(self.__len__())

    def delete(self, where):
        filter = jx_expression(where).partial_eval(SQLang).to_sql(self.schema)
        with self.container.db.transaction() as t:
            t.execute(ConcatSQL(
                SQL_DELETE,
                SQL_FROM,
                quote_column(self.snowflake.fact_name),
                SQL_WHERE,
                filter,
            ))

    def vars(self):
        return set(self.schema.columns.keys())

    def map(self, map_):
        return self

    def where(self, filter):
        """
        WILL NOT PULL WHOLE OBJECT, JUST TOP-LEVEL PROPERTIES
        :param filter:  jx_expression filter
        :return: list of objects that match
        """
        select = []
        column_names = []
        for c in self.schema.columns:
            if c.json_type in STRUCT:
                continue
            if len(c.nested_path) != 1:
                continue
            column_names.append(c.name)
            select.append(sql_alias(quote_column(c.es_column), c.name))

        where_sql = jx_expression(filter).partial_eval(SQLang).to_sql(self.schema)
        result = self.container.db.query(ConcatSQL(
            SQL_SELECT,
            JoinSQL(SQL_COMMA, select),
            SQL_FROM,
            quote_column(self.snowflake.fact_name),
            SQL_WHERE,
            where_sql,
        ))

        return list_to_data([
            {c: v for c, v in zip(column_names, r)} for r in result.data
        ])

    def query(self, query=None):
        """
        :param query:  JSON Query Expression, SET `format="container"` TO MAKE NEW TABLE OF RESULT
        :return:
        """
        if not query:
            query = {}

        if not query.get("from"):
            query["from"] = self.name

        if is_text(query["from"]) and not startswith_field(query["from"], self.name):
            Log.error("Expecting table, or some nested table")
        normalized_query = QueryOp.wrap(query, self, SQLang)

        if normalized_query.format == "container":
            new_table = "temp_" + unique_name()
            create_table = SQL_CREATE + quote_column(new_table) + SQL_AS
        else:
            create_table = ""

        if normalized_query.groupby and normalized_query.format != "cube":
            op, index_to_columns = self._groupby_op(normalized_query, self.schema)
            command = create_table + op
        elif normalized_query.groupby:
            normalized_query.edges, normalized_query.groupby = (
                normalized_query.groupby,
                normalized_query.edges,
            )
            op, index_to_columns = self._edges_op(normalized_query, self.schema)
            command = create_table + op
            normalized_query.edges, normalized_query.groupby = (
                normalized_query.groupby,
                normalized_query.edges,
            )
        elif normalized_query.edges or any(
            t.aggregate is not NULL for t in listwrap(normalized_query.select.terms)
        ):
            op, index_to_columns = self._edges_op(
                normalized_query, normalized_query.frum.schema
            )
            command = create_table + op
        else:
            op = self._set_op(normalized_query)
            return op

        result = self.container.db.query(command)

        if normalized_query.format == "container":
            output = QueryTable(new_table, container=self.container)
        elif normalized_query.format == "cube" or (
            not normalized_query.format and normalized_query.edges
        ):
            column_names = [None] * (
                max(c.push_column_index for c in index_to_columns.values()) + 1
            )
            for c in index_to_columns.values():
                column_names[c.push_column_index] = c.push_column_name

            if len(normalized_query.edges) == 0 and len(normalized_query.groupby) == 0:
                data = {n: Data() for n in column_names}
                for s in index_to_columns.values():
                    data[s.push_list_name][s.push_column_child] = from_data(s.pull(result.data[0]))
                select = [{"name": s.name} for s in normalized_query.select.terms]

                return Data(data=from_data(data), select=select, meta={"format": "cube"})

            if not result.data:
                edges = []
                dims = []
                for i, e in enumerate(
                    normalized_query.edges + normalized_query.groupby
                ):
                    allowNulls = coalesce(e.allowNulls, True)

                    if e.domain.type == "set" and e.domain.partitions:
                        domain = SimpleSetDomain(partitions=e.domain.partitions.name)
                    elif e.domain.type == "range":
                        domain = e.domain
                    elif is_op(e.value, TupleOp):
                        pulls = (
                            jx
                            .sort(
                                [
                                    c
                                    for c in index_to_columns.values()
                                    if c.push_list_name == e.name
                                ],
                                "push_column_child",
                            )
                            .pull
                        )
                        parts = [tuple(p(d) for p in pulls) for d in result.data]
                        domain = SimpleSetDomain(partitions=jx.sort(set(parts)))
                    else:
                        domain = SimpleSetDomain(partitions=[])

                    dims.append(1 if allowNulls else 0)
                    edges.append(Data(
                        name=e.name, allowNulls=allowNulls, domain=domain
                    ))

                data = {}
                for si, s in enumerate(normalized_query.select.terms):
                    if s.aggregate == "count":
                        data[s.name] = Matrix(dims=dims, zeros=0)
                    else:
                        data[s.name] = Matrix(dims=dims)

                select = [{"name": s.name} for s in normalized_query.select.terms]

                return Data(
                    meta={"format": "cube"},
                    edges=edges,
                    select=select,
                    data={k: v.cube for k, v in data.items()},
                )

            columns = None

            edges = []
            dims = []
            for g in normalized_query.groupby:
                g.is_groupby = True

            for i, e in enumerate(normalized_query.edges + normalized_query.groupby):
                allowNulls = coalesce(e.allowNulls, True)

                if e.domain.type == "set" and e.domain.partitions:
                    domain = e.domain
                elif e.domain.type == "range":
                    domain = e.domain
                elif e.domain.type == "time":
                    domain = wrap(mo_json.scrub(e.domain))
                elif e.domain.type == "duration":
                    domain = to_data(mo_json.scrub(e.domain))
                elif is_op(e.value, TupleOp):
                    pulls = (
                        jx
                        .sort(
                            [
                                c
                                for c in index_to_columns.values()
                                if c.push_list_name == e.name
                            ],
                            "push_column_child",
                        )
                        .pull
                    )
                    parts = [tuple(p(d) for p in pulls) for d in result.data]
                    domain = SimpleSetDomain(partitions=jx.sort(set(parts)))
                else:
                    if not columns:
                        columns = transpose(*result.data)
                    parts = set(columns[i])
                    if e.is_groupby and None in parts:
                        allowNulls = True
                    parts -= {None}

                    if normalized_query.sort[i].sort == -1:
                        domain = SimpleSetDomain(partitions=wrap(sorted(
                            parts, reverse=True
                        )))
                    else:
                        domain = SimpleSetDomain(partitions=jx.sort(parts))

                dims.append(len(domain.partitions) + (1 if allowNulls else 0))
                edges.append(Data(name=e.name, allowNulls=allowNulls, domain=domain))

            data_cubes = {
                s.name: Matrix(dims=dims)
                for s in normalized_query.select.terms
            }

            r2c = index_to_coordinate(dims)  # WORKS BECAUSE THE DATABASE SORTED THE EDGES TO CONFORM
            for record, row in enumerate(result.data):
                coord = r2c(record)

                for i, s in enumerate(index_to_columns.values()):
                    if s.is_edge:
                        continue
                    if s.push_column_child == ".":
                        data_cubes[s.push_list_name][coord] = s.pull(row)
                    else:
                        data_cubes[s.push_list_name][coord][s.push_column_child] = s.pull(row)

            select = [{"name": s.name} for s in normalized_query.select.terms]

            return Data(
                meta={"format": "cube"},
                edges=edges,
                select=select,
                data={k: v.cube for k, v in data_cubes.items()},
            )
        elif normalized_query.format == "table" or (
            not normalized_query.format and normalized_query.groupby
        ):
            column_names = [None] * (
                max(c.push_column_index for c in index_to_columns.values()) + 1
            )
            for c in index_to_columns.values():
                column_names[c.push_column_index] = c.push_column_name
            data = []
            for d in result.data:
                row = [None for _ in column_names]
                for s in index_to_columns.values():
                    if s.push_column_child == ".":
                        row[s.push_column_index] = s.pull(d)
                    elif s.num_push_columns:
                        tuple_value = row[s.push_column_index]
                        if tuple_value == None:
                            tuple_value = row[s.push_column_index] = (
                                [None] * s.num_push_columns
                            )
                        tuple_value[s.push_column_child] = s.pull(d)
                    elif row[s.push_column_index] == None:
                        row[s.push_column_index] = Data()
                        row[s.push_column_index][s.push_column_child] = s.pull(d)
                    else:
                        row[s.push_column_index][s.push_column_child] = s.pull(d)
                data.append(tuple(from_data(r) for r in row))

            output = Data(meta={"format": "table"}, header=column_names, data=data)
        elif normalized_query.format == "list" or (
            not normalized_query.edges and not normalized_query.groupby
        ):
            if (
                not normalized_query.edges
                and not normalized_query.groupby
                    and any(s.aggregate is not NULL for s in normalized_query.select.terms)
            ):
                data = Data()
                for s in index_to_columns.values():
                    if not data[s.push_column_name][s.push_column_child]:
                        data[s.push_column_name][s.push_column_child] = s.pull(result.data[0])
                    else:
                        data[s.push_column_name][s.push_column_child] += [s.pull(result.data[0])]
                output = Data(meta={"format": "value"}, data=unwraplist(from_data(data)))
            else:
                data = []
                for record in result.data:
                    row = Data()
                    for c in index_to_columns.values():
                        if c.num_push_columns:
                            # APPEARS TO BE USED FOR PULLING TUPLES (GROUPBY?)
                            tuple_value = row[c.push_list_name]
                            if not tuple_value:
                                tuple_value = row[c.push_list_name] = (
                                    [None] * c.num_push_columns
                                )
                            tuple_value[c.push_column_child] = c.pull(record)
                        else:
                            row[c.push_list_name][c.push_column_child] = c.pull(record)

                    data.append(row)

                output = Data(meta={"format": "list"}, data=data)
        else:
            Log.error("unknown format {{format}}", format=normalized_query.format)

        return output

    def query_metadata(self, query):
        frum, query["from"] = query["from"], self
        schema = self.snowflake.tables["."].schema
        query = QueryOp.wrap(query, schema)
        columns = self.snowflake.columns
        where = query.where
        table_name = None
        column_name = None

        if query.edges or query.groupby:
            raise Log.error("Aggregates(groupby or edge) are not supported")

        if where.op == "eq" and where.lhs.var == "table":
            table_name = mo_json.json2value(where.rhs.json)
        elif where.op == "eq" and where.lhs.var == "name":
            column_name = mo_json.json2value(where.rhs.json)
        else:
            raise Log.error(
                'Only simple filters are expected like: "eq" on table and column name'
            )

        tables = [concat_field(self.snowflake.fact_name, i) for i in self.tables.keys()]

        metadata = []
        if columns[-1].es_column != GUID:
            columns.append(Column(
                name=GUID,
                json_type=STRING,
                es_column=GUID,
                es_index=self.snowflake.fact_name,
                nested_path=["."],
            ))

        for tname, table in zip(t, tables):
            if table_name != None and table_name != table:
                continue

            for col in columns:
                cname, ctype = untyped_column(col.es_column)
                if column_name != None and column_name != cname:
                    continue

                metadata.append((
                    table,
                    relative_field(col.name, tname),
                    col.jx_type,
                    unwraplist(col.nested_path),
                ))

        if query.format == "cube":
            num_rows = len(metadata)
            header = ["table", "name", "type", "nested_path"]
            temp_data = dict(zip(header, zip(*metadata)))
            return Data(
                meta={"format": "cube"},
                data=temp_data,
                edges=[{
                    "name": "rownum",
                    "domain": {
                        "type": "rownum",
                        "min": 0,
                        "max": num_rows,
                        "interval": 1,
                    },
                }],
            )
        elif query.format == "table":
            header = ["table", "name", "type", "nested_path"]
            return Data(meta={"format": "table"}, header=header, data=metadata)
        else:
            header = ["table", "name", "type", "nested_path"]
            return Data(
                meta={"format": "list"}, data=[dict(zip(header, r)) for r in metadata]
            )

    def _window_op(self, query, window):
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

        range_min = text(coalesce(window.range.min, "UNBOUNDED"))
        range_max = text(coalesce(window.range.max, "UNBOUNDED"))

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

    def _normalize_select(self, select) -> SelectOp:
        return normalize_one(Null, select, "list")

    def transaction(self):
        """
        PERFORM MULTIPLE ACTIONS IN A TRANSACTION
        """
        return Transaction(self)


class Transaction:
    def __init__(self, table):
        self.transaction = None
        self.table = table

    def __enter__(self):
        self.transaction = self.container.db.transaction()
        self.table.db = self.transaction  # REDIRECT SQL TO TRANSACTION
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.table.db = self.table.container.db
        self.transaction.__exit__(exc_type, exc_val, exc_tb)
        self.transaction = None

    def __getattr__(self, item):
        return getattr(self.table, item)

# TODO: use dependency injection
type2container["sqlite"] = QueryTable
