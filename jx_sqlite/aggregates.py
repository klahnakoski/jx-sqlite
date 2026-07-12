# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import (
    NULL,
    CountOp,
    PercentileOp,
    PercentilesOp,
    StatsOp,
    CardinalityOp,
    OrOp,
    AndOp,
    UnionOp,
    ZERO,
)
from jx_base.language import is_op
from jx_sqlite.expressions.variable import Variable
from jx_sqlite.utils import (
    ColumnMapping,
    STATS,
    _make_column_name,
    get_column,
    sql_text_array_to_set,
)
from mo_dots import unliteral_field
from mo_json import NUMBER, JX_BOOLEAN, BOOLEAN, jx_type_to_json_type
from mo_sql.utils import sql_type_key_to_json_type, sql_aggs, untyped_column, UID
from mo_sql import *
from mo_sqlite import *

# AGGREGATE APPLICATION (P8, docs\INTERSECTION_SURVEY.md): ONE RULE PER AGGREGATE SHAPE.
# A RULE COMPILES ONE select TERM, YIELDING (aliased select sql, ColumnMapping) PAIRS;
# column_number IS THE POSITION THE FIRST YIELDED COLUMN WILL TAKE IN THE SELECT LIST


def aggregates(facts, index_to_column, offset, outer_selects, query, schema):
    for si, s in enumerate(query.select.terms, start=offset):
        rule = aggregate_rule(s, query)
        for sql, mapping in rule(facts, s, si, len(outer_selects), schema):
            index_to_column[len(outer_selects)] = mapping
            outer_selects.append(sql)


def aggregate_rule(s, query):
    if is_op(s.value, Variable) and s.value.var in ["row", "."] and is_op(s.aggregate, CountOp):
        return _count_records
    if is_op(s.aggregate, CountOp) and (not query.edges and not query.groupby):
        return _count_columns
    if is_op(s.aggregate, PercentileOp) or is_op(s.aggregate, PercentilesOp):
        return _percentile
    if is_op(s.aggregate, CardinalityOp):
        return _cardinality
    if is_op(s.aggregate, OrOp):
        return _or_aggregate
    if is_op(s.aggregate, AndOp):
        return _and_aggregate
    if is_op(s.aggregate, UnionOp):
        return _union_aggregate
    if is_op(s.aggregate, StatsOp):
        return _stats_aggregate
    return _standard_aggregate


def _count_records(facts, s, si, column_number, schema):
    # COUNT RECORDS, NOT ANY ONE VALUE: COUNT THE ORIGIN TABLE'S UID
    # (NON-NULL PER ORIGIN ROW; NULL WHERE A LEFT JOIN FOUND NOTHING)
    sql = sql_alias(sql_count(quote_column(schema.nested_path[0], UID)), s.name)
    yield sql, ColumnMapping(
        push_list_name=s.name,
        push_column_name=unliteral_field(s.name),
        push_column_index=si,
        push_column_child=".",
        pull=get_column(column_number, None, ZERO),
        sql=sql,
        column_alias=s.name,
        type=NUMBER,
    )


def _count_columns(facts, s, si, column_number, schema):
    value = s.value.var
    columns = [c.es_column for c in facts.snowflake.columns if untyped_column(c.es_column)[0] == value]
    sql = SQL_PLUS.join(sql_count(quote_column(col)) for col in columns)
    yield sql_alias(sql, _make_column_name(column_number)), ColumnMapping(
        push_list_name=s.name,
        push_column_name=unliteral_field(s.name),
        push_column_index=si,
        push_column_child=".",
        pull=get_column(column_number, None, s.default),
        sql=sql,
        column_alias=_make_column_name(column_number),
        type=NUMBER,
    )


def _percentile(facts, s, si, column_number, schema):
    raise NotImplementedError()


def _cardinality(facts, s, si, column_number, schema):
    sql = s.value.partial_eval(SQLang).to_sql(schema)
    count_sql = sql_alias(sql_count("DISTINCT" + sql_iso(sql)), _make_column_name(column_number),)
    yield count_sql, ColumnMapping(
        push_list_name=s.name,
        push_column_name=unliteral_field(s.name),
        push_column_index=si,
        push_column_child=".",
        pull=get_column(column_number, None, 0),
        sql=count_sql,
        column_alias=_make_column_name(column_number),
        type=NUMBER,
    )


def _or_aggregate(facts, s, si, column_number, schema):
    sql = s.value.partial_eval(SQLang).to_sql(schema)
    yield sql_alias(
        ConcatSQL(SQL_NOT, SQL_NOT, sql_call("SUM", sql_iso(sql))), _make_column_name(column_number),
    ), ColumnMapping(
        push_list_name=s.name,
        push_column_name=unliteral_field(s.name),
        push_column_index=si,
        push_column_child=".",
        pull=get_column(column_number, JX_BOOLEAN, s.default),
        sql=sql,
        column_alias=_make_column_name(column_number),
        type=BOOLEAN,
    )


def _and_aggregate(facts, s, si, column_number, schema):
    sql = s.value.partial_eval(SQLang).to_sql(schema)
    yield sql_alias(
        ConcatSQL(SQL_NOT, sql_call("SUM", sql_iso(ConcatSQL(SQL_NOT, sql_iso(sql))))),
        _make_column_name(column_number),
    ), ColumnMapping(
        push_list_name=s.name,
        push_column_name=unliteral_field(s.name),
        push_column_index=si,
        push_column_child=".",
        pull=get_column(column_number, JX_BOOLEAN, s.default),
        sql=sql,
        column_alias=_make_column_name(column_number),
        type=BOOLEAN,
    )


def _union_aggregate(facts, s, si, column_number, schema):
    for details in s.value.partial_eval(SQLang).to_sql(schema):
        for sql_type, sql in details.sql.items():
            yield sql_alias(
                "JSON_GROUP_ARRAY(DISTINCT" + sql_iso(sql) + ")", _make_column_name(column_number),
            ), ColumnMapping(
                push_list_name=s.name,
                push_column_name=unliteral_field(s.name),
                push_column_index=si,
                push_column_child=".",
                pull=sql_text_array_to_set(column_number),
                sql=sql,
                column_alias=_make_column_name(column_number),
                type=sql_type_key_to_json_type[sql_type],
            )
            column_number += 1


def _stats_aggregate(facts, s, si, column_number, schema):
    # THE STATS OBJECT
    sql = s.value.to_sql(schema)
    for name, code in STATS.items():
        full_sql = code.replace("{{value}}", sql)
        yield sql_alias(full_sql, _make_column_name(column_number)), ColumnMapping(
            push_list_name=s.name,
            push_column_name=unliteral_field(s.name),
            push_column_index=si,
            push_column_child=name,
            pull=get_column(column_number, None, s.default),
            sql=full_sql,
            column_alias=_make_column_name(column_number),
            type="number",
        )
        column_number += 1


def _standard_aggregate(facts, s, si, column_number, schema):
    sql = s.value.partial_eval(SQLang).to_sql(schema)
    sql = sql_call(sql_aggs[s.aggregate.op], sql)
    json_type = jx_type_to_json_type(s.aggregate.jx_type)

    default_value = s.default
    if default_value is NULL and is_op(s.aggregate, CountOp):
        # COUNT OF NOTHING IS 0, NEVER NULL (DECISIVE COUNT)
        default_value = ZERO
    yield sql_alias(sql, _make_column_name(column_number)), ColumnMapping(
        push_list_name=s.name,
        push_column_name=unliteral_field(s.name),
        push_column_index=si,
        push_column_child=".",
        pull=get_column(column_number, json_type, default_value),
        sql=sql,
        column_alias=_make_column_name(column_number),
        type=json_type,
    )
