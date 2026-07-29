# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from copy import copy
from math import isnan

from jx_base import DataClass
from jx_base import Snowflake
from jx_base.expressions import NULL, SqlScript
from mo_dots import (
    Data,
    concat_field,
    is_data,
    is_list,
    join_field,
    split_field,
    startswith_field,
    is_sequence,
    is_missing,
)
from mo_future import is_text
from mo_json import BOOLEAN, ARRAY, NUMBER, OBJECT, STRING, json2value, JX_BOOLEAN, get_if_type
from mo_json.typed_encoder import untype_path
from mo_logs import Log
from mo_math import randoms
from mo_sql.utils import SQL_KEYS, SQL_ARRAY_KEY, SQL_KEY_PREFIX, SQL_NUMBER_KEY, UID, GUID, ORDER, PARENT, COLUMN
from mo_sqlite import ConcatSQL, SQL_EQ, SQL_LEFT_JOIN, SQL_ON, sql_alias
from mo_sqlite.utils import quote_column
from mo_times import Date


def unique_name():
    return randoms.string(20)


def column_key(k, v):
    if v == None:
        return None
    elif isinstance(v, bool):
        return k, "boolean"
    elif is_text(v):
        return k, "string"
    elif is_list(v):
        return k, None
    elif is_data(v):
        return k, "object"
    elif isinstance(v, Date):
        return k, "number"
    else:
        return k, "number"


POS_INF = float("+inf")


def value_to_jx_type(v):
    if v == None:
        return None
    elif isinstance(v, bool):
        return BOOLEAN
    elif is_text(v):
        return STRING
    elif is_data(v):
        return OBJECT
    elif isinstance(v, float):
        if isnan(v) or abs(v) == POS_INF:
            return None
        return NUMBER
    elif isinstance(v, (int, Date)):
        return NUMBER
    elif is_sequence(v):
        return ARRAY
    return None


def table_alias(i):
    """
    :param i:
    :return:
    """
    return "__t" + str(i) + "__"


def sql_join_chain(snowflake, origin_path, required_tables=()):
    """
    THE P2 JOIN-CHAIN (docs/INTERSECTION_SURVEY.md): LEFT JOIN, SPANNING TREE FROM THE FACT,
    ON child.__parent__ = parent.__id__.  COVERS THE ANCESTORS OF THE ORIGIN PLUS ANY
    required_tables (AND THEIR ANCESTORS) THE QUERY REACHES INTO - NO MORE, BECAUSE EVERY
    EXTRA CHILD JOIN FANS OUT THE PARENT ROWS.
    EACH TABLE IS ALIASED AS ITSELF (setop PRECEDENT): NAME ALGEBRA (Names/leaves) KEEPS
    WORKING AND NO SCHEMA RENAME IS NEEDED - ONE SCOPE FOR EVERY COMPILED REFERENCE.
    :param snowflake: PROVIDES query_paths
    :param origin_path: THE QUERY'S PERSPECTIVE (schema.nested_path[0])
    :param required_tables: TABLES HOLDING COLUMNS THE QUERY MENTIONS
    :return: (nest_to_alias, from_sql) - ALIAS FOR EVERY QUERY PATH, FROM-CLAUSE FRAGMENTS
    """
    nest_to_alias = {sub_table: sub_table for sub_table in snowflake.query_paths}
    targets = {origin_path, *required_tables}
    chain = sorted(
        (t for t in snowflake.query_paths if any(startswith_field(target, t) for target in targets)),
        key=len,
    )
    fact = chain[0]
    from_sql = [sql_alias(quote_column(fact), fact)]
    for nest in chain[1:]:
        parent = max(
            (t for t in chain if t != nest and startswith_field(nest, t)),
            key=len,
        )
        from_sql.append(ConcatSQL(
            SQL_LEFT_JOIN,
            sql_alias(quote_column(nest), nest),
            SQL_ON,
            quote_column(nest, PARENT),
            SQL_EQ,
            quote_column(parent, UID),
        ))
    return nest_to_alias, from_sql


def get_document_value(document, column):
    """
    RETURN DOCUMENT VALUE IF MATCHES THE column (name, type)

    :param document: THE DOCUMENT
    :param column: A (name, type) PAIR
    :return: VALUE, IF IT IS THE SAME NAME AND TYPE
    """
    v = document.get(split_field(column.name)[0], None)
    return get_if_type(v, column.jx_type)


def _make_column_name(number):
    return COLUMN + str(number)


def sql_text_array_to_set(column):
    def _convert(row):
        text = row[column]
        if text == None:
            return set()
        else:
            value = json2value(row[column])
            return set(value) - {None}

    return _convert


def get_column(column, json_type=None, default=NULL):
    """
    :param column: The column you want extracted
    :return: a function that can pull the given column out of sql resultset
    """

    to_type = json_type_to_python_type.get(json_type)

    if to_type is None:

        def _get(row):
            value = row[column]
            if is_missing(value):
                return default.value
            return value

        return _get

    def _get_type(row):
        value = row[column]
        if is_missing(value):
            return default.value
        return to_type(value)

    return _get_type


json_type_to_python_type = {JX_BOOLEAN: bool}


def set_column(row, col, child, value):
    """
    EXECUTE `row[col][child]=value` KNOWING THAT row[col] MIGHT BE None
    :param row:
    :param col:
    :param child:
    :param value:
    :return:
    """
    if child == ".":
        row[col] = value
    else:
        column = row[col]

        if column is None:
            column = row[col] = {}
        Data(column)[child] = value


def copy_cols(cols, nest_to_alias):
    """
    MAKE ALIAS FOR EACH COLUMN
    :param cols:
    :param nest_to_alias:  map from nesting level to subquery alias
    :return:
    """
    output = set()
    for c in cols:
        c = copy(c)
        c.es_index = nest_to_alias[c.nested_path[0]]
        output.add(c)
    return output


ColumnMapping = DataClass(
    "ColumnMapping",
    [
        {  # EDGES ARE AUTOMATICALLY INCLUDED IN THE OUTPUT, USE THIS TO INDICATE EDGES SO WE DO NOT DOUBLE-PRINT
            "name": "is_edge",
            "default": False,
        },
        {"name": "num_push_columns", "nulls": True,},  # TRACK NUMBER OF TABLE COLUMNS THIS column REPRESENTS
        {"name": "push_list_name", "nulls": True,},  # NAME OF THE PROPERTY (USED BY LIST FORMAT ONLY)
        {  # PATH INTO COLUMN WHERE VALUE IS STORED ("." MEANS COLUMN HOLDS PRIMITIVE VALUE)
            "name": "push_column_child",
            "nulls": True,
        },
        {"name": "push_column_index", "nulls": True},  # THE COLUMN NUMBER
        {  # ABSOLUTE (FACT-ROOTED) PATH OF THE DOCUMENT THIS COLUMN LANDS IN - ITS SLOT'S ADDRESS
            # (setop.py).  THE TABLE'S OWN PATH FOR PLAIN ASSEMBLY, THE TERM'S WHEN A SELECT NAMES
            # THE BRANCH; TABLE/CUBE READ THE TOP-LEVEL KEY OFF IT
            "name": "slot_path",
            "nulls": True,
        },
        {  # THE COLUMN NAME FOR TABLES AND CUBES (WITH NO ESCAPING DOTS, NOT IN LEAF FORM)
            "name": "push_column_name",
            "nulls": True,
        },
        {"name": "pull", "nulls": True},  # A FUNCTION THAT WILL RETURN A VALUE
        {"name": "sql",},  # A LIST OF MULTI-SQL REQUIRED TO GET THE VALUE FROM THE DATABASE
        "type",  # THE NAME OF THE JSON DATA TYPE EXPECTED
        {"name": "nested_path", "type": list, "default": ["."],},  # A LIST OF PATHS EACH INDICATING AN ARRAY
        "column_alias",
    ],
    constraint={"and": [
        {"in": {"type": ["0", "boolean", "number", "string", "object"]}},
        {"gte": [{"length": "nested_path"}, 1]},
    ]},
)


def fan_out_tuple(terms, make_slot, *, push_list_name, push_column_name, push_column_index, is_edge=False):
    """
    Fan a TupleOp value into one output column per slot, sharing the reshape
    contract: every slot's ColumnMapping carries num_push_columns=len(terms) and
    push_column_child=i, which is what tells format.py to reassemble the columns
    into a positional list (rather than a named object).  make_slot(i, term)
    supplies the per-slot pieces the two call sites (edge domain, select aggregate)
    compute differently: (select_sql, pull, type, column_alias, mapping_sql).
    Yields (select_sql, ColumnMapping) per slot, in tuple order.
    """
    n = len(terms)
    for i, term in enumerate(terms):
        select_sql, pull, type, column_alias, mapping_sql = make_slot(i, term)
        yield select_sql, ColumnMapping(
            is_edge=is_edge,
            push_list_name=push_list_name,
            push_column_name=push_column_name,
            push_column_index=push_column_index,
            num_push_columns=n,
            push_column_child=i,
            pull=pull,
            type=type,
            sql=mapping_sql,
            column_alias=column_alias,
        )


class StrictSnowflake(Snowflake):
    def __init__(self, query_paths, columns):
        self._query_paths = query_paths
        self._columns = columns

    @property
    def query_paths(self):
        return self._query_paths

    @property
    def columns(self):
        return self._columns

    @property
    def column(self):
        return ColumnLocator(self._columns)


class ColumnLocator:
    def __init__(self, columns):
        self.columns = columns

    def __getitem__(self, column_name):
        return [c for c in self.columns if untype_path(c.name) == column_name]
