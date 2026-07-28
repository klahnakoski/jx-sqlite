# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from typing import List, Dict, Tuple

from jx_base import Column, is_op, FALSE
from jx_base.expressions import NULL, SqlScript, SelectOp, Variable
from jx_base.expressions.variable import is_variable
from jx_base.expressions.sql_is_null_op import SqlIsNullOp
from jx_base.expressions.sql_order_by_op import OneOrder
from jx_base.utils import GUID
from jx_sqlite.builder import DocumentDetails, BranchBuilder
from jx_sqlite.expressions.leaves_op import LeavesOp
from jx_sqlite.expressions.to_boolean_op import ToBooleanOp
from jx_sqlite.format import format_deep
from jx_sqlite.utils import (
    COLUMN,
    ColumnMapping,
    ORDER,
    _make_column_name,
    get_column,
    UID,
    PARENT,
)
from mo_dots import (
    Data,
    startswith_field,
    unwraplist,
    relative_field,
    is_missing,
    listwrap,
    Null,
    tail_field,
    unliteral_field,
)
from mo_future import extend, first
from mo_json.types import OBJECT, jx_type_to_json_type, JX_ANY, STRING, INTEGER, JX_TEXT, JX_INTEGER
from mo_logs import Log
from mo_sql import SQL_DESC, SQL_ASC, NO_SQL, SQL_TRUE, SQL_INNER_JOIN
from mo_sql.utils import untype_field
from mo_sqlite import Facts
from mo_sqlite import (
    SQL_FROM,
    SQL_LEFT_JOIN,
    SQL_ON,
    SQL_SELECT,
    SQL_UNION_ALL,
    SQL_WHERE,
    sql_list,
    ConcatSQL,
)
from mo_sqlite import SQLang
from mo_sqlite import quote_column, sql_alias
from mo_sqlite.expressions import SqlVariable, SqlOrderByOp, SqlEqOp, SqlAliasOp, SqlLimitOp
from mo_sqlite.expressions.sql_script import SqlScript
from mo_times import Date


@extend(Facts)
def _set_op(self, query):
    index_to_column, command, primary_doc_details = to_sql(self, query)
    result = self.container.db.query(command)

    def _accumulate_nested(
        state,  # [current_row] one-element list; state[0] is None when the stream is exhausted
        rows,  # row generator (advance with next(rows, None))
        nested_doc_details: DocumentDetails,  # describes how the rows get mapped to nested docs
        parent_id: int,  # the id of the parent doc (child rows carry it at parent_id_coord)
        parent_id_coord: int,  # the column of the parent_id; None at the origin (no above grouping)
    ) -> List[Data]:
        # STREAMING HIERARCHICAL GROUPER (NO INLINE FIRST-ROW).  Rows arrive in hierarchical
        # ORDER BY (parent uid, then child uid, ...), so each element's own row (child uids NULL)
        # precedes its child rows.  We build the element from its own row, then dispatch the
        # following rows of the same element to whichever child's uid is present.  A LEFT-JOIN
        # NULL element at the origin is a legitimate (empty) doc (missing child == implicit [{}]).
        output = []
        id_coord = nested_doc_details.id_coord
        curr_nested_path, _ = untype_field(nested_doc_details.nested_path[0])
        is_origin = parent_id_coord is None
        index_to_column = tuple((c.push_list_name, c.pull) for _, c in nested_doc_details.index_to_column.items())
        children = nested_doc_details.children

        while state[0] is not None:
            row = state[0]
            if not is_origin and row[parent_id_coord] != parent_id:
                break  # belongs to a different parent
            my_id = row[id_coord]
            if not is_origin and is_missing(my_id):
                break  # a sibling branch's row (this level's uid absent); let the caller dispatch

            doc = Null
            for rel_field, pull in index_to_column:
                value = pull(row)
                if is_missing(value):
                    continue
                if rel_field == ".":
                    # WHOLE-VALUE SELECT; doc["."] = value WOULD WRAP A SCALAR IN Data
                    doc = value
                else:
                    doc = doc or Data()
                    doc[rel_field] = value

            state[0] = next(rows, None)  # CONSUME THIS ELEMENT'S OWN ROW
            consumed = set()  # id(child) that had a surviving (dispatched) row
            if not is_missing(my_id):
                # DISPATCH THE ROWS OF THIS ELEMENT (SAME my_id) TO THEIR CHILD BRANCHES.  ORDER BY
                # KEEPS EACH CHILD'S ROWS CONTIGUOUS, SO ONE PASS PER CHILD RUN SUFFICES.
                while state[0] is not None and state[0][id_coord] == my_id:
                    crow = state[0]
                    for child_details in children:
                        if id(child_details) in consumed or is_missing(crow[child_details.id_coord]):
                            continue
                        consumed.add(id(child_details))
                        nested_value = _accumulate_nested(state, rows, child_details, my_id, id_coord)
                        if nested_value:
                            doc = doc or Data()
                            # THE CHILD'S ASSEMBLED VALUE LANDS AT ITS OWN PUSH NAME (AN EXPLICIT
                            # DEEP-LEAF SELECT ACCUMULATES BARE VALUES AT THE TERM PATH); DEFAULT IS
                            # THE CHILD TABLE'S RELATIVE PATH
                            rel_field = child_details.push_list_name or relative_field(
                                untype_field(child_details.nested_path[0])[0], curr_nested_path
                            )
                            doc[rel_field] = unwraplist(nested_value)
                        break
                    else:
                        # NO UNCONSUMED CHILD OWNS crow (guards against an unexpected duplicate row)
                        break

            # A WHERE FILTERED A REQUIRED CHILD TO NOTHING => DROP THIS PARENT.  KEY ON WHETHER A
            # CHILD ROW SURVIVED (WAS DISPATCHED), NOT ON DOC CONTENT: A where LIKE `exists a.b`
            # SELECTS NO COLUMNS FROM THE CHILD, SO EXISTENCE IS THE ONLY SIGNAL.
            dropped = any(id(c) not in consumed for c in children if c.required)

            if not dropped and (doc or is_origin):
                output.append(doc)

        return output

    cols = tuple(i for i in index_to_column.values() if i.push_list_name != None)

    if result.data:
        state = [None]
        rows = iter(result.data)
        state[0] = next(rows, None)
        # REASSEMBLY ROOTS AT THE ORIGIN (primary_doc_details IS THE ORIGIN NODE): ABOVE-ORIGIN
        # ANCESTORS ARE JOINS ONLY, NOT REASSEMBLY LEVELS.  parent_id_coord=None => EVERY ORIGIN
        # ELEMENT IS A TOP-LEVEL DOC (NO ABOVE-ORIGIN GROUPING), SO NO POST-PROC FLATTEN IS NEEDED.
        data = _accumulate_nested(state, rows, primary_doc_details, 0, None)
    else:
        data = result.data

    return format_deep(data, cols, query)


@extend(Facts)
def to_sql(self, query) -> Tuple[Dict[int, ColumnMapping], SqlScript, DocumentDetails]:
    # EACH SELECT VALUE BELONGS AT QUERY DEPTH, FIND LEAST DEEP FOR EACH (AND THE VARIABLES REQUIRED)

    schema = query.frum.schema
    known_vars = schema.keys()

    # PARTITION SELECT TERMS INTO BRANCHES.  A SUBQUERY VALUE (SelectOp OVER A NESTED FromOp)
    # AND A DEEP-LEAF PATH ("a._a.v", CROSSING AN ARRAY BOUNDARY) ARE THE SAME THING: A DEEP
    # GROUP - ITS OWN BRANCH SELECTING BRANCH-RELATIVE COLUMNS.  KYLE'S RULE: A DEEP-LEAF PATH
    # `a._a.v` IS SUGAR FOR `{"from":"a._a","select":"v","name":"a._a.v"}` (SPLIT AT THE ARRAY
    # BOUNDARY).  A BRANCH OF ONE LEAF COLLAPSES TO A BARE MULTIVALUE; SEVERAL LEAVES ASSEMBLE
    # AS SUB-OBJECTS.  READ THE PRE-partial_eval TERMS: partial_eval FLATTENS BOTH BACK TO
    # PATHS.  docs/INTERSECTION_SURVEY.md §7 STEPS 1-2
    origin = query.frum.nested_path[0]
    plain_terms = []
    subqueries = {}  # branch table path -> list of (outer name, [(inner name, inner value), ...])
    deep_candidates = {}  # table path -> [(term, select_path), ...] deep-leaf plain terms
    for term in query.select.terms:
        if is_op(term.value, SelectOp):
            rel = first(term.value.frum.vars())
            table_path = first(c.nested_path[0] for _, c in schema.leaves(rel))
            subqueries.setdefault(table_path, []).append((term.name, list(term.value)))
            continue
        if is_variable(term.value):
            split = _deep_split(term.value.var, schema, origin)
            if split:
                table_path, select_path = split
                deep_candidates.setdefault(table_path, []).append((term, select_path))
                continue
        plain_terms.append(term)

    # ONE DEEP LEAF PER TABLE -> REWRITE TO A ONE-LEAF SUBQUERY (ROUTES THROUGH THE STEP-1 PATH,
    # COLLAPSING TO A BARE MULTIVALUE).  SEVERAL DEEP LEAVES AT ONE TABLE STAY PLAIN (THE OLD
    # RE-ROOTED SUB-DOC PATH) UNTIL STEP 3 MAKES EACH ITS OWN BRANCH.
    for table_path, cands in deep_candidates.items():
        if len(cands) == 1 and table_path not in subqueries:
            term, select_path = cands[0]
            subqueries.setdefault(table_path, []).append((term.name, [(select_path, Variable(select_path))]))
        else:
            plain_terms.extend(term for term, _ in cands)
    plain_select = SelectOp(query.select.frum, *plain_terms)

    # GET LIST OF SELECTED COLUMNS
    select_vars = set(
        rest if first == "row" else v
        for s in plain_terms
        for v in s.value.vars()
        for first, rest in [tail_field(v)]
    )
    active_paths = {schema.nested_path[0]: {
        Column(
            name=GUID,
            json_type=STRING,
            es_column=GUID,
            es_index=self.name,
            es_type=str,
            nested_path=[self.name],
            multi=1,
            cardinality=0,
            last_updated=Date.now(),
        ),
        Column(
            name=UID,
            json_type=INTEGER,
            es_column=UID,
            es_index=self.name,
            es_type=str,
            nested_path=[self.name],
            multi=1,
            cardinality=0,
            last_updated=Date.now(),
        ),
    }}
    for v in select_vars:
        for _, c in schema.leaves(v):
            active_paths.setdefault(c.nested_path[0], set()).add(c)

    # ANY VARS MENTIONED WITH NO COLUMNS?
    for v in select_vars:
        if not any(startswith_field(cname, v) for cname in known_vars):
            active_paths[schema.path].add(Column(
                name=v,
                json_type=OBJECT,
                es_column=".",
                es_index=schema.path,
                es_type="NULL",
                nested_path=[schema.path],
                multi=1,
                cardinality=0,
                last_updated=Date.now(),
            ))
    # EACH SUBQUERY'S ORIGIN TABLE IS AN ACTIVE BRANCH (ITS INNER SELECT SUPPLIES THE COLUMNS)
    for table_path in subqueries:
        active_paths.setdefault(table_path, set())
    # EVERY COLUMN, AND THE COLUMN INDEX IT OCCUPIES
    builder = BranchBuilder()
    # ALIASES: SAME OBJECTS THE BUILDER OWNS (IN-PLACE MUTATION IS SHARED); THE REST OF to_sql
    # AND _make_sql_for_one_nest_in_set_op READ THESE DIRECTLY
    index_to_column: Dict[int, ColumnMapping] = builder.index_to_column
    index_to_uid = builder.index_to_uid
    sql_selects = builder.sql_selects

    selects = plain_select.partial_eval(SQLang)

    # BRANCHES ALSO NEED TABLES THE where/sort REFERENCE (JOINED FOR FILTERING/ORDERING, NOT
    # SELECTED AS VALUES).  RESOLVE THOSE VARS TO THEIR TABLES THE SAME WAY select_vars ARE.
    referenced_paths = set(active_paths)
    for v in set(
        rest if first == "row" else v
        for source in (query.where.vars(), *(s.value.vars() for s in listwrap(query.sort)))
        for v in source
        for first, rest in [tail_field(v)]
    ):
        for _, c in schema.leaves(v):
            referenced_paths.add(c.nested_path[0])

    # THE BRANCHES OF THE RESULT HIERARCHY.  QUERY-DRIVEN: KEEP ONLY THE BRANCHES ON THE PATH TO
    # SOMETHING THE QUERY REFERENCES - EACH REFERENCED PATH AND ITS ANCESTORS (FOR THE JOIN
    # CHAIN).  UNRELATED/SIBLING TABLES CONTRIBUTE NOTHING, SO DROPPING THEM ONLY REMOVES EMPTY
    # UNION-ALL BRANCHES.  THE SORT KEYS AND THE UNION-ALL SQL ARE BOTH DRIVEN FROM THIS LIST.
    # docs/INTERSECTION_SURVEY.md §6
    branches = [
        t
        for t in self.snowflake.query_paths
        if any(startswith_field(a, t) for a in referenced_paths)
    ]

    # TABLES THE where REFERENCES.  A where TABLE STRICTLY BELOW THE ORIGIN CANNOT BE EVALUATED ON
    # THE ORIGIN ARM (THE CHILD IS NOT JOINED THERE, NO INLINE FIRST-ROW); INSTEAD ITS OWN ARM
    # FILTERS AND THE BRANCH IS MARKED required SO A PARENT WITH NO SURVIVING CHILD IS DROPPED AT
    # ASSEMBLY.  docs/INTERSECTION_SURVEY.md §7 (3-pre WHERE)
    where_tables = set(
        c.nested_path[0]
        for v in set(
            rest if first == "row" else v
            for v in query.where.vars()
            for first, rest in [tail_field(v)]
        )
        for _, c in schema.leaves(v)
    )

    # EVERY SELECT STATEMENT THAT WILL BE REQUIRED, NO MATTER THE DEPTH
    # WE WILL CREATE THEM ACCORDING TO THE DEPTH REQUIRED
    origin_doc_details = None  # REASSEMBLY ROOTS HERE (THE ORIGIN), NOT AT THE FACT
    for table_number, sub_table in enumerate(branches):
        nested_doc_details = builder.add_branch(sub_table, table_number)
        if sub_table == origin:
            origin_doc_details = nested_doc_details
        if (
            sub_table != origin
            and startswith_field(sub_table, origin)
            and any(startswith_field(wt, sub_table) for wt in where_tables)
        ):
            # ON THE CHAIN FROM JUST-BELOW-ORIGIN DOWN TO A where TABLE: A PARENT WITH NO SURVIVING
            # ROW HERE IS DROPPED, AND THE DROP BUBBLES UP TO THE ORIGIN.
            nested_doc_details.required = True
        nested_path = nested_doc_details.nested_path
        sub_schema = self.snowflake.get_schema(list(reversed([
            t for t in self.snowflake.query_paths if startswith_field(sub_table, t)
        ])))

        # WE DO NOT NEED DATA FROM TABLES WE REQUEST NOTHING FROM
        if sub_table not in active_paths:
            continue

        sub_selects = selects.partial_eval(SQLang).to_sql(sub_schema).expr

        # A BRANCH BELOW THE ORIGIN NORMALLY RE-ROOTS ITS COLUMNS TO BRANCH-RELATIVE NAMES;
        # select_op KEEPS THE ORIGIN-ROOTED NAME (docs/NAMES.md push_name=push_child=".")
        # ONLY FOR AN EXPLICIT DEEP LEAF - A SINGLE VALUE SELECTED FROM ABOVE.  READ THAT
        # DECISION BACK OFF THE RESOLVED TERM NAME: A DEEP LEAF STILL STARTS WITH THE
        # BRANCH'S ORIGIN-RELATIVE PATH.
        origin = query.frum.nested_path[0]
        branch_rel = (
            untype_field(relative_field(sub_table, origin))[0]
            if startswith_field(sub_table, origin) and sub_table != origin
            else None
        )
        deep_leaves = []  # COLUMN NUMBERS OF EXPLICIT DEEP LEAVES IN THIS BRANCH

        for i, term in enumerate(sub_selects.terms):
            name, value = term.name, term.value
            if is_op(value, LeavesOp):
                Log.error("expecting SelectOp to subsume the LeavesOp")

            push_column_name, push_column_child = tail_field(name)
            push_column_name = unliteral_field(push_column_name)
            column_number = builder.add_column(
                nested_doc_details,
                value,
                # LIST FORMAT SPLATS THE "." CONTAINER INTO THE DOC ROOT
                push_list_name=push_column_child if push_column_name == "." else name,
                push_column_name=push_column_name,
                push_column_child=push_column_child,
                push_column_index=i,
                nested_path=nested_path,
            )
            if branch_rel and startswith_field(unliteral_field(name), branch_rel) and unliteral_field(name) != branch_rel:
                deep_leaves.append(column_number)

        # ONE DEEP LEAF COLLAPSES TO BARE VALUES (push_list_name=".") ACCUMULATED AS A
        # MULTIVALUE AT ITS TERM PATH ON THE ORIGIN DOC; BARE VALUES CANNOT SHARE A DOC,
        # SO SEVERAL LEAVES KEEP THEIR (RE-ROOTED) SUB-DOC INSTEAD
        if len(deep_leaves) == 1:
            leaf = index_to_column[deep_leaves[0]]
            nested_doc_details.push_list_name = leaf.push_list_name
            leaf.push_list_name = "."

        # A SUBQUERY (OR REWRITTEN DEEP-LEAF) WHOSE ORIGIN IS THIS BRANCH CONTRIBUTES ITS INNER
        # SELECT COMPILED BRANCH-RELATIVE (NAMES LIKE "v","s", NOT "a._a.v").  SEVERAL LEAVES
        # ASSEMBLE AS SUB-OBJECTS UNDER THIS NODE; A LONE LEAF COLLAPSES TO A BARE MULTIVALUE
        # (push_list_name=".") LANDING AT THE OUTER TERM PATH.  docs/INTERSECTION_SURVEY.md §7 STEPS 1-2
        for outer_name, inner_select in subqueries.get(sub_table, []):
            added = []
            for i, (name, value) in enumerate(inner_select):
                sql = value.partial_eval(SQLang).to_sql(sub_schema)
                push_column_name, push_column_child = tail_field(name)
                push_column_name = unliteral_field(push_column_name)
                added.append(builder.add_column(
                    nested_doc_details,
                    sql,
                    push_list_name=push_column_child if push_column_name == "." else name,
                    push_column_name=push_column_name,
                    push_column_child=push_column_child,
                    push_column_index=i,
                    nested_path=nested_path,
                ))
            if len(added) == 1:
                nested_doc_details.push_list_name = outer_name
                index_to_column[added[0]].push_list_name = "."
    where_clause = ToBooleanOp(query.where).partial_eval(SQLang).to_sql(schema)
    # ORDERING
    sorts = []
    if query.sort:
        for sort in query.sort:
            sql = sort.value.partial_eval(SQLang).to_sql(schema)
            column_number = len(sql_selects)
            # SQL HAS ABS TABLE REFERENCE
            column_alias = _make_column_name(column_number)
            sql_selects.append(sql_alias(sql, column_alias))
            sorts.append(OneOrder(SqlIsNullOp(SqlVariable(None, column_alias)), NO_SQL))
            sorts.append(OneOrder(SqlVariable(None, column_alias), sort_to_sqlite_order[sort.sort]))
    # ONE UID SORT KEY PER NODE (SHALLOW->DEEP PRE-ORDER), NOT PER TABLE: TWO SIBLING NODES ON THE
    # SAME TABLE EACH GET THEIR OWN KEY, KEEPING EACH ARM'S ROWS CONTIGUOUS PER PARENT.
    for node in _preorder(builder.primary_doc_details):
        sorts.append(OneOrder(SqlVariable(None, f"{COLUMN}{node.id_coord}", jx_type=JX_TEXT), NO_SQL))

    # THE ORIGIN ARM (AND ALL DESCENDANT ARMS) MUST TREAT ABOVE-ORIGIN ANCESTOR COLUMNS AS REAL;
    # SEED THE NULL-PAD OWNERSHIP WITH THEM (ABOVE-ORIGIN NODES ARE JOIN-ONLY, NOT THEIR OWN ARMS).
    above_owned = set()
    for node in _path_to(builder.primary_doc_details, origin_doc_details)[:-1]:
        above_owned |= _owned_of(node)

    unsorted_sql = _make_sql_for_one_nest_in_set_op(
        self,
        origin_doc_details,
        origin,
        sql_selects,
        where_clause,
        index_to_column,
        above_owned,
        schema,
        where_tables,
    )

    ordered_sql = SqlOrderByOp(unsorted_sql, sorts)
    if query.limit is not NULL:
        ordered_sql = SqlLimitOp(ordered_sql, query.limit.to_sql(schema))
    return index_to_column, ordered_sql, origin_doc_details


@extend(Facts)
def _make_sql_for_one_nest_in_set_op(
    self,
    origin_node,  # THE ORIGIN DocumentDetails NODE; REASSEMBLY AND THE ARM TREE BOTH ROOT HERE
    origin_path,  # ITS TABLE PATH (ITS ARM LEFT-JOINS FOR THE EMPTY ELEMENT; CHILD ARMS INNER-JOIN)
    sql_selects,  # THE ALIGNED SELECT LIST (POSITION = COLUMN INDEX)
    where_clause,
    index_to_column,  # COLUMN INDEX -> ColumnMapping (VALUE/uid/order COLUMNS; SORT COLS ABSENT)
    above_owned,  # COLUMN INDICES OWNED BY ABOVE-ORIGIN ANCESTORS (ALWAYS REAL ON EVERY ARM)
    schema,
    where_tables,  # TABLES THE where REFERENCES (FOR PER-ARM WHERE GATING)
):
    """
    ONE UNION-ALL ARM PER NODE IN THE origin_node SUBTREE (NODE-DRIVEN, NOT TABLE-DRIVEN: TWO
    SIBLING NODES ON ONE TABLE => TWO ARMS).  EACH ARM IS ROOTED AT THE FACT AND JOINS DOWN
    node.nested_path TO ITS OWN LEVEL (SUPPLYING ANCESTOR uids/COLUMNS); IT DOES NOT JOIN ITS
    CHILDREN - THEY ARE THEIR OWN ARMS (NO INLINE FIRST-ROW).  A COLUMN IS REAL ON A NODE'S ARM
    IFF OWNED BY THAT NODE OR AN ANCESTOR NODE; DESCENDANT/SIBLING COLUMNS NULL-PAD.
    docs/INTERSECTION_SURVEY.md §7 (3-pre / step 3)
    """
    fact = self.snowflake.fact_name

    def arm_from(node):
        # FROM fact, THEN JOIN DOWN node.nested_path (fact FIRST).  ORIGIN TABLE + PROPER ANCESTORS
        # LEFT JOIN (JOIN-ONLY / EMPTY-ELEMENT); TABLES BELOW ORIGIN INNER JOIN (REAL ROWS ONLY).
        parts = []
        parent_alias = None
        for table in reversed(node.nested_path):
            alias = table  # BUILDER'S uid/VALUE COLUMNS REFERENCE TABLES BY FULL NAME
            if table == fact:
                parts.append(ConcatSQL(
                    SQL_FROM,
                    SqlAliasOp(SqlVariable(fact, None, jx_type=self.schema.jx_type), alias),
                ))
            else:
                join = SQL_INNER_JOIN if startswith_field(table, origin_path) and table != origin_path else SQL_LEFT_JOIN
                parts.append(ConcatSQL(
                    join,
                    SqlAliasOp(SqlVariable(table, None), alias),
                    SQL_ON,
                    SqlEqOp(SqlVariable(alias, PARENT), SqlVariable(parent_alias, UID)),
                ))
            parent_alias = alias
        return parts

    def arm_select(owned):
        # REAL VALUE IFF THE INDEX IS OWNED BY THIS ARM'S NODE OR AN ANCESTOR; ELSE NULL-PAD.
        # INDICES ABSENT FROM index_to_column (SORT KEYS) PASS THROUGH UNCHANGED ON EVERY ARM.
        out = []
        for select_index, s in enumerate(sql_selects):
            column_mapping = index_to_column.get(select_index)
            if not column_mapping:
                out.append(s)
            elif select_index in owned:
                out.append(SqlAliasOp(column_mapping.sql, column_mapping.column_alias))
            else:
                out.append(SqlAliasOp(NULL.to_sql(schema), column_mapping.column_alias))
        return out

    def build(node, ancestor_owned):
        owned = ancestor_owned | _owned_of(node)
        # THIS ARM MAY APPLY THE where ONLY IF EVERY TABLE IT REFERENCES IS JOINED HERE (AT OR ABOVE
        # THIS NODE'S TABLE); OTHERWISE A DEEPER ARM FILTERS AND ITS BRANCH IS required.
        arm_where = (
            where_clause
            if all(startswith_field(node.nested_path[0], wt) for wt in where_tables)
            else SQL_TRUE
        )
        arm = ConcatSQL(
            SQL_SELECT, sql_list(arm_select(owned)), ConcatSQL(*arm_from(node)), SQL_WHERE, arm_where
        )
        arms = [arm]
        for child in node.children:
            arms.extend(build(child, owned))
        return arms

    return SqlScript(
        jx_type=JX_ANY,
        # TODO: IS THIS THE TYPE FOR THE SET OF COLUMNS?  (INCLUDE NESTING, SO WE MAY UNION TO GET FINAL TYPE)
        expr=SQL_UNION_ALL.join(build(origin_node, above_owned)),
        frum=None,
        miss=FALSE,
        schema=schema,
    )


def _owned_of(node):
    # COLUMN INDICES A NODE OWNS: ITS VALUE COLUMNS PLUS ITS uid/order PLUMBING.
    return set(node.index_to_column.keys()) | set(node.uid_coords)


def _preorder(node):
    # NODES SHALLOW->DEEP, PARENTS BEFORE CHILDREN (SORT-KEY ORDER: PARENT uid BEFORE CHILD uid).
    yield node
    for child in node.children:
        yield from _preorder(child)


def _path_to(node, target):
    # THE CHAIN OF NODES FROM node DOWN TO target INCLUSIVE (target IS UNIQUE IN THE TREE).
    if node is target:
        return [node]
    for child in node.children:
        below = _path_to(child, target)
        if below:
            return [node, *below]
    return None


def _deep_split(var, schema, origin):
    # A DEEP-LEAF PATH IS SUGAR FOR A ONE-LEAF SUBQUERY: SPLIT AT THE ARRAY BOUNDARY.  RETURN
    # (branch table path, select path relative to that table) WHEN var'S LEAVES ALL LIVE IN ONE
    # TABLE BELOW THE ORIGIN; None OTHERWISE (SHALLOW, OR SPLIT ACROSS TABLES/UNION TYPES).
    tables = set(c.nested_path[0] for _, c in schema.leaves(var))
    if len(tables) != 1:
        return None
    table_path = first(tables)
    if table_path == origin or not startswith_field(table_path, origin):
        return None
    from_path = untype_field(relative_field(table_path, origin))[0]
    return table_path, untype_field(relative_field(var, from_path))[0]


sort_to_sqlite_order = {-1: SQL_DESC, 0: SQL_ASC, 1: SQL_ASC}


def test_dots(cols):
    for c in cols:
        if "\\" in c.push_column_name:
            return True
    return False
