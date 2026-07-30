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
from jx_base.expressions import NULL, SqlScript, SelectOp, Variable, Literal
from jx_base.expressions.select_op import SelectOne
from jx_base.expressions.variable import is_variable
from jx_base.expressions.sql_is_null_op import SqlIsNullOp
from jx_base.expressions.sql_order_by_op import OneOrder
from jx_base.utils import GUID
from jx_sqlite.builder import DocumentDetails, BranchBuilder
from jx_sqlite.expressions.coalesce_op import CoalesceOp
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
    concat_field,
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
from mo_sqlite import sql_alias
from mo_sqlite.expressions import SqlVariable, SqlOrderByOp, SqlEqOp, SqlAliasOp
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
    ) -> Dict[object, List[Data]]:
        # STREAMING HIERARCHICAL GROUPER (NO INLINE FIRST-ROW).  Rows arrive in hierarchical
        # ORDER BY (parent uid, then child uid, ...), so each element's own row (child uids NULL)
        # precedes its child rows.  We build the element from its own row, then dispatch the
        # following rows of the same element to whichever child's uid is present.  A LEFT-JOIN
        # NULL element at the origin is a legitimate (empty) doc (missing child == implicit [{}]).
        # ONE DOC PER (ELEMENT, SLOT): THE SAME ROWS ANSWER SEVERAL SELECT TERMS, EACH ASSEMBLING
        # ITS OWN DOCUMENT FROM ITS OWN COLUMNS AND LANDING AT ITS OWN PATH.
        output = {}  # slot -> its docs
        id_coord = nested_doc_details.id_coord
        slot_path = nested_doc_details.slot_path
        is_origin = parent_id_coord is None
        slot_columns = tuple(
            (slot, tuple((c.push_list_name, c.pull) for c in cols.values()))
            for slot, cols in nested_doc_details.slot_columns.items()
        )
        children = nested_doc_details.children

        while state[0] is not None:
            row = state[0]
            if not is_origin and row[parent_id_coord] != parent_id:
                break  # belongs to a different parent
            my_id = row[id_coord]
            if not is_origin and is_missing(my_id):
                break  # a sibling branch's row (this level's uid absent); let the caller dispatch

            docs = {slot: Null for slot in slot_path}  # A SLOT WITHOUT COLUMNS STILL CARRIES CHILDREN
            for slot, cols in slot_columns:
                for rel_field, pull in cols:
                    value = pull(row)
                    if is_missing(value):
                        continue
                    docs[slot] = _land(docs[slot], rel_field, value)

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
                        for slot, nested_value in _accumulate_nested(
                            state, rows, child_details, my_id, id_coord
                        ).items():
                            if not nested_value:
                                continue
                            # A SLOT LANDS IN THE PARENT'S DOC FOR THE SAME SLOT; A TERM THE PARENT
                            # DOES NOT ANSWER (ITS OWN PART WAS SHALLOW) LANDS IN THE DOCUMENT.
                            # BOTH PATHS ARE ABSOLUTE, SO IT LANDS AT THE DIFFERENCE - INCLUDING
                            # "." (THE CHILD *IS* THE DOC)
                            target = slot if slot in slot_path else None
                            rel_field = relative_field(child_details.slot_path[slot], slot_path[target])
                            docs[target] = _land(docs[target], rel_field, nested_value)
                        break
                    else:
                        # NO UNCONSUMED CHILD OWNS crow (guards against an unexpected duplicate row)
                        break

            # A WHERE FILTERED A REQUIRED CHILD TO NOTHING => DROP THIS PARENT.  KEY ON WHETHER A
            # CHILD ROW SURVIVED (WAS DISPATCHED), NOT ON DOC CONTENT: A where LIKE `exists a.b`
            # SELECTS NO COLUMNS FROM THE CHILD, SO EXISTENCE IS THE ONLY SIGNAL.
            dropped = any(id(c) not in consumed for c in children if c.required)

            # KEEP ANY NON-MISSING doc, INCLUDING A FALSY SCALAR (A COLLAPSED MULTIVALUE OF False/0):
            # `doc or is_origin` WOULD DROP s=False.  doc STARTS AS Null (is_missing) UNTIL A VALUE LANDS.
            if not dropped:
                for slot, doc in docs.items():
                    if not is_missing(doc) or is_origin:
                        output.setdefault(slot, []).append(doc)

        return output

    cols = tuple(i for i in index_to_column.values() if i.push_list_name != None)

    if result.data:
        state = [None]
        rows = iter(result.data)
        state[0] = next(rows, None)
        # REASSEMBLY ROOTS AT THE ORIGIN (primary_doc_details IS THE ORIGIN NODE): ABOVE-ORIGIN
        # ANCESTORS ARE JOINS ONLY, NOT REASSEMBLY LEVELS.  parent_id_coord=None => EVERY ORIGIN
        # ELEMENT IS A TOP-LEVEL DOC (NO ABOVE-ORIGIN GROUPING), SO NO POST-PROC FLATTEN IS NEEDED.
        data = _accumulate_nested(state, rows, primary_doc_details, 0, None).get(None, [])
    else:
        data = result.data

    # LIMIT COUNTS DOCUMENTS, NOT UNION ROWS.  ASSEMBLY EMITS DOCS IN SORT ORDER, SO SLICING THE
    # FIRST N IS THE DOCUMENT-LEVEL LIMIT (A SQL ROW-LIMIT WOULD TRUNCATE MID-DOCUMENT - N ARMS EACH).
    if is_op(query.limit, Literal) and isinstance(query.limit.value, (int, float)):
        data = data[: int(query.limit.value)]

    return format_deep(data, cols, query)


@extend(Facts)
def to_sql(self, query) -> Tuple[Dict[int, ColumnMapping], SqlScript, DocumentDetails]:
    # EACH SELECT VALUE BELONGS AT QUERY DEPTH, FIND LEAST DEEP FOR EACH (AND THE VARIABLES REQUIRED)

    schema = query.frum.schema
    known_vars = schema.keys()

    # PARTITION SELECT TERMS INTO BRANCHES.  A SUBQUERY VALUE (SelectOp OVER A NESTED FromOp)
    # AND A NAME REACHING BELOW THE ORIGIN ("a._a.v", "_a", ".") ARE THE SAME THING: A DEEP
    # GROUP - ITS OWN BRANCH SELECTING BRANCH-RELATIVE COLUMNS.  KYLE'S RULE: A DEEP-LEAF PATH
    # `a._a.v` IS SUGAR FOR `{"from":"a._a","select":"v","name":"a._a.v"}` (SPLIT AT THE ARRAY
    # BOUNDARY); _branch_split GENERALIZES THAT SPLIT TO A NAME THAT REACHES SEVERAL TABLES,
    # ASKING Names WHERE EACH BINDING LANDS.  READ THE PRE-partial_eval TERMS: partial_eval
    # FLATTENS BOTH BACK TO PATHS.  docs/INTERSECTION_SURVEY.md §7 STEPS 1-2
    origin = query.frum.nested_path[0]
    plain_terms = []
    subqueries = {}  # branch table path -> list of (outer name, [(inner name, inner value), ...])
    # A TERM CARRYING AN aggregate DECLARATION IS READ AS ITS OPERATOR FORM (SelectOne.expr):
    # ROUTED HERE, IT IS A COLLECTION AGGREGATE OF ONE DOCUMENT (ToListOp DOES ITS OWN FROM), SO
    # IT NEEDS NO BRANCH - `.value` WOULD HAND US THE BARE NESTED NAME AND BUILD ONE.  THE TWO
    # ARE THE SAME OBJECT FOR EVERY OTHER TERM.  query.py::_is_per_document
    for term in query.select.terms:
        if is_op(term.expr, SelectOp):
            rel = first(term.expr.frum.vars())
            table_path = first(c.nested_path[0] for _, c in schema.leaves(rel))
            inner = []
            for inner_term in term.expr.terms:
                if inner_term.aggregate is not NULL and is_variable(inner_term.value):
                    # AN AGGREGATE INSIDE THE SUBQUERY COLLAPSES ITS ROWS, SO IT IS A SCALAR OF
                    # THE OUTER DOCUMENT, NOT A BRANCH: SAME OPERATOR, ORIGIN-ROOTED NAME.  A
                    # ONE-TERM SUBQUERY *IS* ITS TERM, SO THE OUTER NAME IS THE WHOLE NAME
                    plain_terms.append(SelectOne(
                        term.name if len(term.expr.terms) == 1 else concat_field(term.name, inner_term.name),
                        Variable(concat_field(rel, inner_term.value.var)),
                        aggregate=inner_term.aggregate,
                    ))
                else:
                    inner.append((inner_term.name, inner_term.expr))
            if inner:
                subqueries.setdefault(table_path, []).append((term.name, rel, inner))
            continue
        if is_variable(term.expr):
            # EACH TABLE THE NAME REACHES -> ITS OWN SUBQUERY ENTRY = ITS OWN SLOT ON THAT TABLE,
            # SO a._a.v -> list AND a._a.s -> scalar COLLAPSE INDEPENDENTLY.  A NAME WITH BINDINGS
            # AT OR ABOVE THE ORIGIN TOO (`.` OVER A DOC WITH A NESTED ARRAY) KEEPS ITS PLAIN TERM
            # FOR THOSE.  docs/INTERSECTION_SURVEY.md §7 STEP 3
            deep, shallow = _branch_split(term, schema, origin)
            for table_path, entry in deep.items():
                subqueries.setdefault(table_path, []).append(entry)
            if deep and not shallow:
                continue
        plain_terms.append(term)

    plain_select = SelectOp(query.select.frum, *plain_terms)

    # GET LIST OF SELECTED COLUMNS.  join_vars, NOT vars: A COLLECTION AGGREGATE READS ITS NESTED
    # COLUMN THROUGH ITS OWN FROM (ToListOp), SO IT NEEDS NO BRANCH - AND MUST NOT HAVE ONE, OR THE
    # ASSEMBLER WOULD TRY TO HANG THE CHILD DOCS OFF THE SCALAR THE AGGREGATE ALREADY PRODUCED.
    select_vars = set(
        rest if first == "row" else v
        for s in plain_terms
        for v in s.expr.join_vars()
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
    # A NAME REACHES DOWN THROUGH ITS OWN ARM (_branch_split ABOVE), NEVER THROUGH A BRANCH
    # RE-COMPILATION OF THE SAME SELECT: BELOW THE ORIGIN THE PLAIN SELECT CARRIES ONLY THE REST
    # (`*`, EXPRESSIONS EVALUATED PER CHILD ROW).
    deep_selects = SelectOp(
        query.select.frum, *(t for t in plain_terms if not is_variable(t.expr))
    ).partial_eval(SQLang)

    # BRANCHES ALSO NEED TABLES THE where/sort REFERENCE (JOINED FOR FILTERING/ORDERING, NOT
    # SELECTED AS VALUES).  RESOLVE THOSE VARS TO THEIR TABLES THE SAME WAY select_vars ARE.
    # join_vars, NOT vars: A COLLECTION AGGREGATE READS A NESTED COLUMN THROUGH ITS OWN FROM
    # (ToListOp), SO IT NEEDS NO BRANCH HERE.
    referenced_paths = set(active_paths)
    for v in set(
        rest if first == "row" else v
        for source in (query.where.join_vars(), *(s.value.vars() for s in listwrap(query.sort)))
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

    # TABLES THE where MUST HAVE JOINED.  SUCH A TABLE STRICTLY BELOW THE ORIGIN CANNOT BE
    # EVALUATED ON THE ORIGIN ARM (THE CHILD IS NOT JOINED THERE, NO INLINE FIRST-ROW); INSTEAD ITS
    # OWN ARM FILTERS AND THE BRANCH IS MARKED required SO A PARENT WITH NO SURVIVING CHILD IS
    # DROPPED AT ASSEMBLY.  docs/INTERSECTION_SURVEY.md §7 (3-pre WHERE)
    # A COLLECTION AGGREGATE IS ABSENT HERE (join_vars): IT IS A PROPERTY OF THE *PARENT*, SO IT
    # BELONGS ON THE ORIGIN ARM - PUSHING IT TO THE CHILD ARM WOULD DROP A DOCUMENT WITH NO CHILD
    # ROW BEFORE THE PREDICATE COULD SEE IT (count == 0).
    where_tables = set(
        c.nested_path[0]
        for v in set(
            rest if first == "row" else v
            for v in query.where.join_vars()
            for first, rest in [tail_field(v)]
        )
        for _, c in schema.leaves(v)
    )

    # THE SELECT CLAUSE'S OWN NAMES, LONGEST FIRST (SEE _push_name)
    term_names = sorted((t.name for t in plain_terms), key=len, reverse=True)

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

        below_origin = sub_table != origin and startswith_field(sub_table, origin)
        sub_selects = (deep_selects if below_origin else selects).partial_eval(SQLang).to_sql(sub_schema).expr

        for i, term in enumerate(sub_selects.terms):
            name, value = term.name, term.value
            if is_op(value, LeavesOp):
                Log.error("expecting SelectOp to subsume the LeavesOp")

            container, container_path, push_column_child = _push_name(name, term_names)
            # NAMES ARRIVE ESCAPED FIELD BY FIELD (join_field), SO THE ROOT "." READS AS "\b"
            is_root = container == "."
            builder.add_column(
                nested_doc_details,
                value,
                # LIST FORMAT SPLATS THE "." CONTAINER INTO THE DOC ROOT
                push_list_name=push_column_child if is_root else name,
                # THE table/cube HEADER IS WHAT THE SELECT CLAUSE CALLED THIS - A NAME, NOT A PATH
                push_column_name=container,
                push_column_path=container_path,
                push_column_child=push_column_child,
                push_column_index=i,
                nested_path=nested_path,
            )

    # EACH SUBQUERY ENTRY (INCL. A REWRITTEN DEEP NAME) IS A SLOT ON ITS TABLE'S NODE, COMPILED
    # BRANCH-RELATIVE (NAMES LIKE "v","s", NOT "a._a.v").  TWO DEEP LEAVES a._a.v, a._a.s ARE TWO
    # SLOTS ON THE SAME ROWS, EACH ASSEMBLING ITS OWN DOC, SO EACH COLLAPSES TO ITS OWN MULTIVALUE.
    # THE SLOT'S DOC LANDS AT ITS TERM NAME; ITS COLUMNS ARE NAMED INSIDE THE ELEMENT, AND A COLUMN
    # NAMED "." IS THE ELEMENT ITSELF (A BARE MULTIVALUE).  docs/INTERSECTION_SURVEY.md §7 STEP 3
    origin_rel = untype_field(origin)[0]
    for table_path, entries in subqueries.items():
        sub_schema = self.snowflake.get_schema(list(reversed([
            t for t in self.snowflake.query_paths if startswith_field(table_path, t)
        ])))
        for slot, source, inner_select in entries:
            node = builder.nodes[table_path]
            # THE SLOT'S ADDRESS AT EVERY LEVEL FROM BELOW THE ORIGIN DOWN TO THIS TABLE: THE TERM
            # NAME PLUS THE PART OF THAT LEVEL INSIDE THE NAMED VALUE ("." WHILE THE NAME IS STILL
            # AT OR INSIDE THE LEVEL).  NAMING EVERY LEVEL IS WHAT CARRIES A RENAME ACROSS AN
            # INTERMEDIATE ARRAY - THE LEVELS BELOW LAND RELATIVE TO THE RENAMED ONE, NOT THE TABLE.
            for level in self.snowflake.query_paths:
                if level == origin or not startswith_field(level, origin) or not startswith_field(table_path, level):
                    continue
                level_rel = untype_field(relative_field(level, origin))[0]
                inside = "." if startswith_field(source, level_rel) else relative_field(level_rel, source)
                builder.nodes[level].slot_path.setdefault(
                    slot, concat_field(origin_rel, concat_field(slot, inside))
                )
            for i, (name, value) in enumerate(inner_select):
                sql = value.partial_eval(SQLang).to_sql(sub_schema)
                container, push_column_child = tail_field(name)
                builder.add_column(
                    node,
                    sql,
                    slot=slot,
                    push_list_name=push_column_child if unliteral_field(container) == "." else name,
                    # THE HEADER IS THE TERM THAT CLAIMED THIS SLOT, NOT THE BRANCH-RELATIVE NAME
                    push_column_name=slot,
                    push_column_path=slot,
                    push_column_child=push_column_child,
                    push_column_index=i,
                    nested_path=node.nested_path,
                )
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

    # NO SQL ROW-LIMIT: THE UNION HAS N ARMS PER DOCUMENT, SO A ROW-LIMIT TRUNCATES MID-DOCUMENT.
    # _set_op LIMITS THE ASSEMBLED DOCUMENTS INSTEAD (SqlOrderByOp IS A FIRST-CLASS TOP COMMAND).
    return index_to_column, SqlOrderByOp(unsorted_sql, sorts), origin_doc_details


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


def _land(doc, rel_field, value):
    # LAND ONE VALUE IN THE DOC UNDER ITS PUSH NAME.  ONE RULE FOR BOTH KINDS - THIS ELEMENT'S OWN
    # COLUMNS AND A CHILD BRANCH'S ASSEMBLED LIST: A ONE-ELEMENT MULTIVALUE IS THE VALUE
    # (unwraplist, A NO-OP ON A COLUMN - A SQL CELL IS NEVER A LIST), AND A PUSH NAME OF "." MEANS
    # THE VALUE *IS* THE DOC (WHOLE-VALUE SELECT), WHERE doc["."] = value WOULD WRAP IT IN Data.
    value = unwraplist(value)
    if rel_field == ".":
        return value
    doc = doc or Data()
    doc[rel_field] = value
    return doc


def _owned_of(node):
    # COLUMN INDICES A NODE OWNS: EVERY SLOT'S VALUE COLUMNS PLUS ITS uid/order PLUMBING.
    return set(i for cols in node.slot_columns.values() for i in cols) | set(node.uid_coords)


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


def _branch_split(term, schema, origin):
    """
    A SELECT TERM NAMING A BRANCH IS ONE SUBQUERY PER TABLE THE NAME REACHES.  Names ANSWERS
    WHERE EACH BINDING LANDS (docs/NAMES.md): push_child IS ITS PATH INSIDE THE ELEMENT OF THE
    ARRAY HOLDING IT, SO THAT IS THE COLUMN'S NAME - INCLUDING "." WHEN THE QUERIED NAME *IS*
    THE VALUE (`a._a.v` -> A BARE MULTIVALUE OF v).  THAT SPLIT IS THE WHOLE RULE: A ONE-LEAF
    ARRAY OF OBJECTS (`a` OVER [{"b":1}]) KEEPS ITS PROPERTY NAME BECAUSE push_child IS "b",
    NOT BECAUSE OF ANY COUNT.  (WHERE THE *ELEMENT* LANDS IS THE SLOT'S ADDRESS, DERIVED FROM
    THE TERM NAME PER LEVEL BY THE CALLER - push_name IS THAT ANSWER FOR ONE LEVEL ONLY.)

    RETURN ({table path: (term name, queried name, [(inner name, inner value), ...])}, WHETHER
    ANY BINDING LIVES AT OR ABOVE THE ORIGIN - THOSE STAY WITH THE PLAIN TERM).
    """
    var = term.expr.var
    if startswith_field(var, "row"):
        _, var = tail_field(var)
    groups = {}  # table path -> {push_child: [column]}
    shallow = False
    for resolved in schema.leaves(var):
        _, col = resolved
        table_path = col.nested_path[0]
        if table_path == origin or not startswith_field(table_path, origin):
            shallow = True
            continue
        groups.setdefault(table_path, {}).setdefault(resolved.push_child, []).append(col)
    return (
        {
            t: (term.name, var, [(push_child, _binding(cols)) for push_child, cols in entries.items()])
            for t, entries in groups.items()
        },
        shallow,
    )


def _binding(cols):
    """
    THE VALUE OF ONE push_child: ITS COLUMNS.  UNION TYPES BIND SEVERAL TO ONE NAME, SO THEY ARE
    COALESCED - BUT BY es_column, NOT BY A RE-DERIVED NAME.  A NAME THAT IS *BOTH* A BARE VALUE AND
    AN OBJECT (`a` OVER ["b", {"b": 1}] AFTER MERGING) RESOLVES TO THE WHOLE ELEMENT WHEN ASKED BY
    NAME, WHICH IS NOT WHAT push_child "." MEANS - IT MEANS THIS ELEMENT'S OWN VALUE
    """
    terms = [Variable(c.es_column, c.json_type) for c in cols]
    if len(terms) == 1:
        return terms[0]
    return CoalesceOp(*terms)


sort_to_sqlite_order = {-1: SQL_DESC, 0: SQL_ASC, 1: SQL_ASC}


def _push_name(full_name, term_names):
    """
    SPLIT A COMPILED PUSH NAME INTO (HEADER, PATH TO IT IN THE DOCUMENT, PATH INSIDE IT).

    THE BOUNDARY IS NOT RECOVERABLE FROM THE STRING - `a.b` IS EITHER THE TERM `a.b` OR THE TERM
    `a` EXPANDED TO ITS LEAF `b` - SO ASK THE SELECT CLAUSE: THE LONGEST TERM NAME THAT PREFIXES
    THE PUSH NAME OWNS THE COLUMN, AND ITS NAME IS THE HEADER *AS WRITTEN*, WHICH IS ALSO THE PATH
    (`a..html` SELECTS THE PROPERTY NAMED `a.html` AND SAYS SO; `a.b` IS TWO STEPS AND NESTS).
    A NAME NO TERM CLAIMS WAS INVENTED BY EXPANSION - `*` FLATTENS EVERY LEAF INTO ONE ESCAPED KEY
    - SO THERE THE HEADER IS THE UNESCAPED NAME WHILE THE DOCUMENT IS STILL KEYED BY THE ESCAPED
    ONE, AND THE TWO DIFFER.
    """
    for term_name in term_names:
        if term_name == full_name:
            return term_name, term_name, "."
        if term_name != "." and startswith_field(full_name, term_name):
            return term_name, term_name, relative_field(full_name, term_name)
    container, child = tail_field(full_name)
    return unliteral_field(container), container, child


def test_dots(cols):
    for c in cols:
        if "\\" in c.push_column_name:
            return True
    return False
