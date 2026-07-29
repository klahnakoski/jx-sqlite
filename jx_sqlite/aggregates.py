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
    TupleOp,
    ZERO,
)
from jx_base.language import is_op
from jx_sqlite.expressions.variable import Variable
from jx_sqlite.utils import (
    ColumnMapping,
    _make_column_name,
    fan_out_tuple,
    frames_one_document,
    gather_column,
    get_column,
    sql_text_array_to_set,
)
from mo_json import NUMBER, JX_BOOLEAN, BOOLEAN, jx_type_to_json_type
from mo_logs import Log
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


def per_document_aggregates(
    facts, index_to_column, offset, outer_selects, inner_selects, query, schema, doc_alias, frame
):
    """
    THE DOCUMENT IS AN IMPLICIT EDGE (edges.py): THE INNER QUERY ANSWERS ONE ROW PER
    (COORDINATE, DOCUMENT) AND THE OUTER COLLAPSES THAT AXIS.  A TERM THAT FRAMES ONE DOCUMENT
    IS COMPUTED INSIDE AND *GATHERED* OUTSIDE - IT COMES BACK AS A MULTIVALUE, ONE VALUE PER
    DOCUMENT.  A TERM THAT CAN COLLAPSE THE WHOLE GROUP IS SELECTED RAW INSIDE AND AGGREGATED
    OUTSIDE, WHICH IS SAFE ONLY BECAUSE ITS VALUE LIVES AT (OR ABOVE) frame - THE TABLE WHOSE
    ROWS THE INNER GROUPING KEEPS DISTINCT - SO IT IS ONE VALUE PER DOCUMENT ALREADY
    :param frame: THE TABLE THE INNER QUERY GROUPS BY (utils.aggregate_frame)
    """
    for si, s in enumerate(query.select.terms, start=offset):
        column_number = len(outer_selects)
        alias = _make_column_name(column_number)
        rule = NULL if s.aggregate is NULL else aggregate_rule(s, query)
        if rule not in (NULL, _standard_aggregate, _count_records):
            # ONLY THE AGGREGATES WHOSE SHAPE IS AGG(<value>) HAVE BEEN SPLIT OVER TWO LEVELS
            Log.error("{{agg}} beside a term that frames one document is not supported", agg=s.aggregate.op)
        if frames_one_document(s, schema):
            # COMPUTED INSIDE, GATHERED OUTSIDE: A MULTIVALUE, ONE VALUE PER DOCUMENT
            value = s.value.partial_eval(SQLang).to_sql(schema)
            if rule is NULL:
                inner_sql = value
                json_type = jx_type_to_json_type(s.value.jx_type)
            else:
                inner_sql = sql_call(sql_aggs[s.aggregate.op], value)
                json_type = jx_type_to_json_type(s.aggregate.jx_type)
            outer_sql = sql_call("JSON_GROUP_ARRAY", quote_column(doc_alias, alias))
            pull = gather_column(column_number, json_type, s.default)
        elif rule is _count_records:
            # COUNT DOCUMENTS: THE UID IS THE INNER GROUP KEY, SO EACH INNER ROW IS ONE
            inner_sql = quote_column(frame, UID)
            outer_sql = sql_count(quote_column(doc_alias, alias))
            json_type = NUMBER
            pull = get_column(column_number, json_type, ZERO)
        else:
            inner_sql = s.value.partial_eval(SQLang).to_sql(schema)
            outer_sql = sql_call(sql_aggs[s.aggregate.op], quote_column(doc_alias, alias))
            json_type = jx_type_to_json_type(s.aggregate.jx_type)
            pull = get_column(column_number, json_type, s.default)
        inner_selects.append(sql_alias(inner_sql, alias))
        outer_selects.append(sql_alias(outer_sql, alias))
        index_to_column[column_number] = ColumnMapping(
            push_list_name=s.name,
            push_column_name=s.name,
            push_column_index=si,
            push_column_child=".",
            pull=pull,
            sql=outer_sql,
            column_alias=alias,
            type=json_type,
        )


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
    if is_op(s.value, TupleOp):
        return _tuple_aggregate
    return _standard_aggregate


def _count_records(facts, s, si, column_number, schema):
    # COUNT RECORDS, NOT ANY ONE VALUE: COUNT THE ORIGIN TABLE'S UID
    # (NON-NULL PER ORIGIN ROW; NULL WHERE A LEFT JOIN FOUND NOTHING)
    sql = sql_alias(sql_count(quote_column(schema.nested_path[0], UID)), s.name)
    yield sql, ColumnMapping(
        push_list_name=s.name,
        push_column_name=s.name,
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
        push_column_name=s.name,
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
        push_column_name=s.name,
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
        push_column_name=s.name,
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
        push_column_name=s.name,
        push_column_index=si,
        push_column_child=".",
        pull=get_column(column_number, JX_BOOLEAN, s.default),
        sql=sql,
        column_alias=_make_column_name(column_number),
        type=BOOLEAN,
    )


def _union_aggregate(facts, s, si, column_number, schema):
    sql = s.value.partial_eval(SQLang).to_sql(schema)
    array_sql = sql_alias(
        ConcatSQL(SQL("JSON_GROUP_ARRAY(DISTINCT "), sql_iso(sql), SQL_CP),
        _make_column_name(column_number),
    )
    yield array_sql, ColumnMapping(
        push_list_name=s.name,
        push_column_name=s.name,
        push_column_index=si,
        push_column_child=".",
        pull=sql_text_array_to_set(column_number),
        sql=sql,
        column_alias=_make_column_name(column_number),
        type=jx_type_to_json_type(s.value.jx_type),
    )


def _stats_aggregate(facts, s, si, column_number, schema):
    # THE STATS OBJECT (median lives in PercentilesOp, not here).
    # SQLite has no VARIANCE aggregate, so variance is the population form
    # SUM(x*x)/N - (SUM(x)/N)**2, and std is its SQRT.
    value = sql_iso(s.value.partial_eval(SQLang).to_sql(schema))
    count = sql_call("COUNT", value)
    total = sql_call("SUM", value)
    sos = sql_call("SUM", ConcatSQL(value, SQL_STAR, value))
    avg = sql_iso(ConcatSQL(total, SQL_STAR, SQL("1.0"), SQL_DIV, count))
    var = ConcatSQL(
        sql_iso(ConcatSQL(sos, SQL_STAR, SQL("1.0"), SQL_DIV, count)),
        SQL(" - "),
        avg,
        SQL_STAR,
        avg,
    )
    stats = {
        "count": count,
        "std": sql_call("SQRT", sql_iso(var)),
        "min": sql_call("MIN", value),
        "max": sql_call("MAX", value),
        "sum": total,
        "sos": sos,
        "var": var,
        "avg": avg,
    }
    for name, code in stats.items():
        full_sql = sql_alias(code, _make_column_name(column_number))
        yield full_sql, ColumnMapping(
            push_list_name=s.name,
            push_column_name=s.name,
            push_column_index=si,
            push_column_child=name,
            pull=get_column(column_number, None, s.default),
            sql=code,
            column_alias=_make_column_name(column_number),
            type="number",
        )
        column_number += 1


def _tuple_aggregate(facts, s, si, column_number, schema):
    # A TUPLE VALUE FANS OUT: THE AGGREGATE DISTRIBUTES OVER EACH SLOT
    # (max([a, b]) == [max(a), max(b)]), ONE OUTPUT COLUMN PER SLOT, REASSEMBLED
    # INTO A POSITIONAL LIST BY num_push_columns (SEE fan_out_tuple).
    agg = sql_aggs[s.aggregate.op]

    def make_slot(i, term):
        col = column_number + i
        script = term.partial_eval(SQLang).to_sql(schema)
        code = sql_call(agg, script)
        alias = _make_column_name(col)
        json_type = jx_type_to_json_type(script.jx_type)
        return sql_alias(code, alias), get_column(col, json_type, s.default), json_type, alias, code

    yield from fan_out_tuple(
        s.value.terms, make_slot,
        push_list_name=s.name,
        push_column_name=s.name,
        push_column_index=si,
    )


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
        push_column_name=s.name,
        push_column_index=si,
        push_column_child=".",
        pull=get_column(column_number, json_type, default_value),
        sql=sql,
        column_alias=_make_column_name(column_number),
        type=json_type,
    )
