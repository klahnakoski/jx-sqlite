# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import mo_json
from jx_base.domains import SimpleSetDomain
from jx_base.expressions import TupleOp, NULL, SqlScript
from jx_base.language import is_op
from jx_python import jx
from mo_collections.matrix import Matrix, index_to_coordinate
from mo_dots import (
    Data,
    to_data,
    coalesce,
    unwraplist,
    from_data,
    literal_field,
    unliteral_field,
    relative_field,
    startswith_field,
    tail_field,
)
from mo_sql.utils import untype_field
from mo_future import transpose
from mo_logs import Log


def format_flat(result, query, index_to_columns):
    if query.format == "cube" or (not query.format and query.edges):
        column_names = [None] * (max(c.push_column_index for c in index_to_columns.values()) + 1)
        for c in index_to_columns.values():
            column_names[c.push_column_index] = c.push_column_name

        if len(query.edges) == 0 and len(query.groupby) == 0:
            data = {n: Data() for n in column_names}
            for s in index_to_columns.values():
                data[s.push_column_name][s.push_column_child] = from_data(s.pull(result.data[0]))
            select = [{"name": s.name} for s in query.select.terms]

            return Data(data=from_data(data), select=select, meta={"format": "cube"})

        if not result.data:
            edges = []
            dims = []
            for i, e in enumerate(query.edges + query.groupby):
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
            for si, s in enumerate(query.select.terms):
                if s.aggregate == "count":
                    data[s.name] = Matrix(dims=dims, zeros=0)
                else:
                    data[s.name] = Matrix(dims=dims)

            select = [{"name": s.name} for s in query.select.terms]

            return Data(
                meta={"format": "cube"},
                edges=edges,
                select=select,
                data={unliteral_field(k): v.cube for k, v in data.items()},
            )

        columns = None

        edges = []
        dims = []
        for g in query.groupby:
            g.is_groupby = True

        for i, e in enumerate(query.edges + query.groupby):
            allowNulls = coalesce(e.allowNulls, True)

            if e.domain.type == "set" and e.domain.partitions:
                domain = e.domain
            elif e.domain.type == "range":
                domain = e.domain
            elif e.domain.type == "time":
                domain = to_data(mo_json.scrub(e.domain))
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

                if query.sort[i].sort == -1:
                    domain = SimpleSetDomain(partitions=to_data(sorted(parts, reverse=True)))
                else:
                    domain = SimpleSetDomain(partitions=jx.sort(parts))

            dims.append(len(domain.partitions) + (1 if allowNulls else 0))
            edges.append(Data(name=e.name, allowNulls=allowNulls, domain=domain))

        data_cubes = {s.name: Matrix(dims=dims) for s in query.select.terms}

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

        select = [{"name": s.name} for s in query.select.terms]

        return Data(
            meta={"format": "cube"},
            edges=edges,
            select=select,
            data={unliteral_field(k): v.cube for k, v in data_cubes.items()},
        )
    elif query.format == "table" or (not query.format and query.groupby):
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
    elif query.format == "list" or (not query.edges and not query.groupby):
        if not query.edges and not query.groupby and any(s.aggregate is not NULL for s in query.select.terms):
            data = Data()
            for s in index_to_columns.values():
                if s.num_push_columns:
                    # SLOTS ARRIVE IN TUPLE ORDER: mo_dots ASSEMBLES THE POSITIONAL
                    # LIST BY APPENDING (Null += [x] CREATES IT, THEN EXTENDS)
                    data[s.push_column_name] += [s.pull(result.data[0])]
                elif not data[s.push_column_name][s.push_column_child]:
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
        Log.error("unknown format {format}", format=query.format)

    return output


def format_metadata(metadata, query):
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


def _top_name(c, origin):
    # THE TOP-LEVEL DOCUMENT KEY THIS COLUMN LANDS UNDER, RELATIVE TO THE QUERY ORIGIN.
    # A COLUMN LIVING IN A DEEPER (CHILD-ARRAY) TABLE IS ASSEMBLED UNDER ITS CONTAINER,
    # NOT ITS OWN LEAF NAME (`b`) - AND THE CONTAINER IS THE COLUMN'S SLOT ADDRESS, WHICH
    # IS THE TABLE'S PATH (`_a`) ONLY UNTIL A SELECT TERM NAMES THE BRANCH SOMETHING ELSE
    # (`{"name":"x","value":"_a"}` -> `x`).  AN ANCESTOR COLUMN (UP-REACH: ONE VALUE PER
    # ORIGIN ROW) LANDS UNDER ITS OWN PUSH NAME.
    if startswith_field(origin, c.nested_path[0]):
        # ORIGIN ITSELF, OR AN ANCESTOR OF IT
        return c.push_column_name
    # THE HEADER IS A NAME, NOT A PATH: A SELECT TERM WHOSE NAME HAS DOTS IS ONE COLUMN, AND
    # normalize_one ESCAPED THAT NAME (`a.b` -> `a..b`) SO THE PATH ALGEBRA WOULD KEEP IT WHOLE
    return unliteral_field(tail_field(relative_field(c.slot_path, untype_field(origin)[0]))[0])


def _deep_header(cols, origin):
    # PRESERVE SELECT-CLAUSE ORDER (push_column_index), NOT ALPHABETICAL
    order = {}
    for c in cols:
        name = _top_name(c, origin)
        prev = order.get(name)
        if prev is None or c.push_column_index < prev:
            order[name] = c.push_column_index
    return tuple(sorted(order, key=order.get))


def format_deep(data, cols, query):
    origin = query.frum.nested_path[0]
    if query.format == "cube":
        num_rows = len(data)
        header = _deep_header(cols, origin)
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
        header = _deep_header(cols, origin)
        if header == (".",):
            temp_data = [(from_data(d),) for d in data]
        else:
            locs = tuple(literal_field(h) for h in header)
            temp_data = [tuple(d[l] for l in locs) for d in data]

        return Data(meta={"format": "table"}, header=header, data=temp_data,)
    else:
        return Data(meta={"format": "list"}, data=data)
