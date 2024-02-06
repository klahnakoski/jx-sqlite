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
from jx_base import Column
from jx_base.domains import SimpleSetDomain
from jx_base.expressions import TupleOp, jx_expression, QueryOp, SelectOp, NULL
from jx_base.expressions.select_op import normalize_one
from jx_base.language import is_op
from jx_python import jx
from jx_sqlite.expressions._utils import SQLang
from jx_sqlite.jx_table import QueryTable
from jx_sqlite.utils import GUID, unique_name, untyped_column
from mo_collections.matrix import Matrix, index_to_coordinate
from mo_dots import (
    Data,
    Null,
    to_data,
    coalesce,
    concat_field,
    listwrap,
    relative_field,
    startswith_field,
    unwraplist,
    wrap,
    list_to_data,
    from_data, literal_field,
)
from mo_future import text, transpose, is_text, extend
from mo_json import STRING, STRUCT
from mo_logs import Log
from mo_sql.utils import sql_aggs
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


@extend(QueryTable)
def format_flat(self, normalized_query, command, index_to_columns):
    if normalized_query.format == "container":
        new_table = "temp_" + unique_name()
        create_table = SQL_CREATE + quote_column(new_table) + SQL_AS
        self.container.db.query(create_table + command)
        return QueryTable(new_table, container=self.container)

    result = self.container.db.query(command)

    if normalized_query.format == "cube" or (not normalized_query.format and normalized_query.edges):
        column_names = [None] * (max(c.push_column_index for c in index_to_columns.values()) + 1)
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
            for i, e in enumerate(normalized_query.edges + normalized_query.groupby):
                allowNulls = coalesce(e.allowNulls, True)

                if e.domain.type == "set" and e.domain.partitions:
                    domain = SimpleSetDomain(partitions=e.domain.partitions.name)
                elif e.domain.type == "range":
                    domain = e.domain
                elif is_op(e.value, TupleOp):
                    pulls = (
                        jx
                        .sort(
                            [c for c in index_to_columns.values() if c.push_list_name == e.name], "push_column_child",
                        )
                        .pull
                    )
                    parts = [tuple(p(d) for p in pulls) for d in result.data]
                    domain = SimpleSetDomain(partitions=jx.sort(set(parts)))
                else:
                    domain = SimpleSetDomain(partitions=[])

                dims.append(1 if allowNulls else 0)
                edges.append(Data(name=e.name, allowNulls=allowNulls, domain=domain))

            data = {}
            for si, s in enumerate(normalized_query.select.terms):
                if s.aggregate == "count":
                    data[s.name] = Matrix(dims=dims, zeros=0)
                else:
                    data[s.name] = Matrix(dims=dims)

            select = [{"name": s.name} for s in normalized_query.select.terms]

            return Data(
                meta={"format": "cube"}, edges=edges, select=select, data={k: v.cube for k, v in data.items()},
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
                    .sort([c for c in index_to_columns.values() if c.push_list_name == e.name], "push_column_child",)
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
                    domain = SimpleSetDomain(partitions=wrap(sorted(parts, reverse=True)))
                else:
                    domain = SimpleSetDomain(partitions=jx.sort(parts))

            dims.append(len(domain.partitions) + (1 if allowNulls else 0))
            edges.append(Data(name=e.name, allowNulls=allowNulls, domain=domain))

        data_cubes = {s.name: Matrix(dims=dims) for s in normalized_query.select.terms}

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
            meta={"format": "cube"}, edges=edges, select=select, data={k: v.cube for k, v in data_cubes.items()},
        )
    elif normalized_query.format == "table" or (not normalized_query.format and normalized_query.groupby):
        column_names = [None] * (max(c.push_column_index for c in index_to_columns.values()) + 1)
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
                        tuple_value = row[s.push_column_index] = [None] * s.num_push_columns
                    tuple_value[s.push_column_child] = s.pull(d)
                elif row[s.push_column_index] == None:
                    row[s.push_column_index] = Data()
                    row[s.push_column_index][s.push_column_child] = s.pull(d)
                else:
                    row[s.push_column_index][s.push_column_child] = s.pull(d)
            data.append(tuple(from_data(r) for r in row))

        output = Data(meta={"format": "table"}, header=column_names, data=data)
    elif normalized_query.format == "list" or (not normalized_query.edges and not normalized_query.groupby):
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
                            tuple_value = row[c.push_list_name] = [None] * c.num_push_columns
                        tuple_value[c.push_column_child] = c.pull(record)
                    else:
                        row[c.push_list_name][c.push_column_child] = c.pull(record)

                data.append(row)

            output = Data(meta={"format": "list"}, data=data)
    else:
        Log.error("unknown format {{format}}", format=normalized_query.format)

    return output

@extend(QueryTable)
def format_metadata(self, metadata, query):
    if query.format == "cube":
        num_rows = len(metadata)
        header = ["table", "name", "type", "nested_path"]
        temp_data = dict(zip(header, zip(*metadata)))
        return Data(
            meta={"format": "cube"},
            data=temp_data,
            edges=[{"name": "rownum", "domain": {"type": "rownum", "min": 0, "max": num_rows, "interval": 1,},}],
        )
    elif query.format == "table":
        header = ["table", "name", "type", "nested_path"]
        return Data(meta={"format": "table"}, header=header, data=metadata)
    else:
        header = ["table", "name", "type", "nested_path"]
        return Data(meta={"format": "list"}, data=[dict(zip(header, r)) for r in metadata])

@extend(QueryTable)
def format_deep(self, data, cols, query):
    if query.format == "cube":
        num_rows = len(data)
        header = tuple(jx.sort(set(c.push_column_name for c in cols)))
        if header == (".",):
            temp_data = {".": data}
        else:
            locs = tuple(literal_field(h) for h in header)
            temp_data = {h: [None] * num_rows for h in header}
            for rownum, d in enumerate(data):
                for h, l in zip(header, locs):
                    temp_data[h][rownum] = d[l]
        return Data(
            meta={"format": "cube"},
            data=temp_data,
            edges=[{"name": "rownum", "domain": {"type": "rownum", "min": 0, "max": num_rows, "interval": 1,},}],
        )
    elif query.format == "table":
        header = tuple(jx.sort(set(c.push_column_name for c in cols)))
        if header == (".",):
            temp_data = data
        else:
            locs = tuple(literal_field(h) for h in header)
            temp_data = [tuple(d[l] for l in locs) for d in data]

        return Data(meta={"format": "table"}, header=header, data=temp_data,)
    else:
        return Data(meta={"format": "list"}, data=data)


