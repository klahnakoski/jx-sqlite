# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from mo_dots import dict_to_data
from mo_logs.strings import expand_template

from jx_base.utils import delist
from mo_json import value2json


def list2cube(rows, column_names=None):
    if column_names:
        keys = column_names
    else:
        columns = set()
        for r in rows:
            columns |= set(r.keys())
        keys = list(columns)

    data = {k: [] for k in keys}
    output = dict_to_data({
        "meta": {"format": "cube"},
        "edges": [{"name": "rownum", "domain": {"type": "rownum", "min": 0, "max": len(rows), "interval": 1},}],
        "data": data,
    })

    for r in rows:
        for k in keys:
            data[k].append(delist(r[k]))

    return output


def list2table(rows, column_names=None):
    if column_names:
        keys = list(set(column_names))
    else:
        columns = set()
        for r in rows:
            columns |= set(r.keys())
        keys = list(columns)

    output = [[delist(r.get(k)) for k in keys] for r in rows]

    return dict_to_data({"meta": {"format": "table"}, "header": keys, "data": output})


def table2csv(table_data):
    """
    :param table_data: expecting a list of tuples
    :return: text in nice formatted csv
    """
    text_data = [tuple(value2json(vals, pretty=True) for vals in rows) for rows in table_data]

    col_widths = [max(len(t) for t in cols) for cols in zip(*text_data)]
    template = ", ".join(f"{{{{{i}|left_align({w})}}}}" for i, w in enumerate(col_widths))
    output = "\n".join(expand_template(template, d) for d in text_data)
    return output
