# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
# THE SHARED-BRANCH BUILDER (docs/INTERSECTION_SURVEY.md §6).  A hierarchical set-op query is
# assembled by having operators CONTRIBUTE branches and columns into one shared set of
# structures - the aligned select list and the pull plan - rather than each returning a
# detached SELECT.  The column index is the shared key binding each SQL alias to its pull.
from dataclasses import dataclass
from typing import List, Dict

from jx_sqlite.utils import ColumnMapping, ORDER, _make_column_name, get_column, UID
from mo_dots import startswith_field
from mo_sql.utils import untype_field
from mo_json.types import jx_type_to_json_type, JX_TEXT, JX_INTEGER
from mo_sqlite import sql_alias
from mo_sqlite.expressions import SqlVariable, SqlAliasOp


@dataclass
class DocumentDetails:
    sub_table: str
    alias: str
    id_coord: int
    nested_path: List[str]
    children: List["DocumentDetails"]
    # ONE TABLE'S ROWS ANSWER SEVERAL SELECT TERMS, EACH ASSEMBLING ITS OWN DOCUMENT AND LANDING
    # SOMEWHERE ELSE.  A *SLOT* IS ONE SUCH ANSWER, KEYED BY THE TERM'S NAME (None = THE DOCUMENT
    # ITSELF, PLAIN ASSEMBLY).  TWO SIDE TABLES, NOT A NODE PER TERM:
    slot_path: Dict[object, str]  # SLOT -> WHERE ITS DOC LANDS, AS AN ABSOLUTE (FACT-ROOTED) PATH;
    # ASSEMBLY LANDS IT AT THAT PATH RELATIVE TO THE PARENT'S PATH FOR THE SAME SLOT.  THE DEFAULT
    # SLOT IS THE TABLE'S OWN PATH - PLAIN DOCUMENT ASSEMBLY IS THE SLOT THE SNOWFLAKE DICTATES.
    slot_columns: Dict[object, Dict[int, ColumnMapping]]  # SLOT -> ITS VALUE COLUMNS
    required: bool  # A WHERE FILTERS THIS BRANCH: A PARENT WITH NO SURVIVING ROW HERE IS DROPPED AT ASSEMBLY
    uid_coords: List[int]  # COLUMN INDICES OF THIS NODE'S PLUMBING (uid, AND order FOR A CHILD); OWNED BY THIS NODE

    def __init__(self, sub_table: str):
        self.sub_table = sub_table
        self.alias = ""
        self.id_coord = -1
        self.nested_path = [sub_table]
        self.children = []
        self.slot_path = {None: untype_field(sub_table)[0]}
        self.slot_columns = {None: {}}
        self.required = False
        self.uid_coords = []


def place(node, parent):
    # INSERT node UNDER THE DEEPEST NODE WHOSE TABLE IS A *PROPER* ANCESTOR OF node'S TABLE.
    for c in parent.children:
        if startswith_field(node.nested_path[0], c.nested_path[0]) and node.nested_path[0] != c.nested_path[0]:
            return place(node, c)
    parent.children.append(node)
    node.nested_path = [node.nested_path[0], *parent.nested_path]


class BranchBuilder:
    """
    THE SHARED STRUCTURES OF ONE HIERARCHICAL SET-OP QUERY: THE ALIGNED SELECT LIST AND THE
    PULL PLAN, KEYED BY ONE COLUMN INDEX.  OPERATORS CONTRIBUTE BRANCHES (add_branch) AND
    COLUMNS (add_column) INTO IT RATHER THAN RETURNING DETACHED SQL.  THE COLUMN INDEX IS THE
    SHARED KEY BINDING EACH SQL ALIAS TO ITS PULL.  docs/INTERSECTION_SURVEY.md §6
    """

    def __init__(self):
        self.sql_selects = []           # ALIGNED SELECT LIST (position = column index)
        self.index_to_column = {}       # column index -> ColumnMapping (pull-plan leaves)
        self.index_to_uid = {}          # nested path -> column index of its UID
        self.nodes = {}                 # table path -> its DocumentDetails (ONE NODE PER TABLE)
        self.primary_doc_details = None  # ROOT OF THE DocumentDetails TREE

    def add_column(
        self, node, sql, *, slot=None, push_list_name, push_column_name, push_column_child, push_column_index,
        nested_path,
    ):
        # CONTRIBUTE ONE VALUE COLUMN: APPEND TO THE ALIGNED SELECT LIST AND REGISTER ITS PULL
        # UNDER THE SAME INDEX, KEEPING THE SQL SIDE AND THE PULL PLAN IN LOCKSTEP.
        n = len(self.sql_selects)
        alias = _make_column_name(n)
        self.sql_selects.append(SqlAliasOp(sql, alias))
        self.index_to_column[n] = node.slot_columns.setdefault(slot, {})[n] = ColumnMapping(
            push_list_name=push_list_name,
            push_column_child=push_column_child,
            push_column_name=push_column_name,
            push_column_index=push_column_index,
            slot_path=node.slot_path[slot],
            pull=get_column(n, json_type=sql.jx_type),
            sql=sql,
            type=jx_type_to_json_type(sql.jx_type),
            column_alias=alias,
            nested_path=nested_path,
        )
        return n

    def add_branch(self, sub_table, table_number):
        # CONTRIBUTE A BRANCH (ONE NESTED LEVEL): A DocumentDetails NODE PLACED IN THE TREE,
        # PLUS ITS UID (AND ORDER, FOR A CHILD) PLUMBING COLUMNS.
        node = self.nodes[sub_table] = DocumentDetails(sub_table)
        if table_number == 0:
            self.primary_doc_details = node  # ROOT OF TREE
        else:
            place(node, self.primary_doc_details)  # INSERT INTO TREE
        node.alias = sub_table

        # WE ALWAYS ADD THE UID
        n = self.index_to_uid[sub_table] = node.id_coord = len(self.sql_selects)
        node.uid_coords.append(n)
        uid_sql = SqlVariable(sub_table, UID, jx_type=JX_TEXT)
        self.sql_selects.append(sql_alias(uid_sql, _make_column_name(n)))
        if table_number > 0:
            # UID AND ORDER FOR CHILD TABLE
            self.index_to_column[n] = ColumnMapping(
                sql=uid_sql, type="number", nested_path=node.nested_path, column_alias=_make_column_name(n),
            )
            n = len(self.sql_selects)
            node.uid_coords.append(n)
            order_sql = SqlVariable(sub_table, ORDER, jx_type=JX_INTEGER)
            self.sql_selects.append(sql_alias(order_sql, _make_column_name(n)))
            self.index_to_column[n] = ColumnMapping(
                sql=order_sql, type="number", nested_path=node.nested_path, column_alias=_make_column_name(n),
            )
        return node
