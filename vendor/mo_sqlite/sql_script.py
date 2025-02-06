# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from typing import List, Tuple

from jx_base.expressions import Expression, NULL
from jx_base.expressions.sql_inner_join_op import SqlJoinOne
from mo_future import flatten
from mo_sqlite.expressions.sql_select_op import SqlSelectOp
from mo_sqlite.expressions.sql_inner_join_op import SqlInnerJoinOp
from mo_sqlite.expressions.sql_alias_op import SqlAliasOp
from mo_sqlite.expressions.sql_and_op import SqlAndOp
from mo_sqlite.expressions.sql_eq_op import SqlEqOp
from mo_sqlite.expressions.sql_variable import SqlVariable
from mo_sqlite.utils import (
    JoinSQL,
    ConcatSQL,
    SQL_UNION_ALL,
    SQL_ORDERBY,
    SQL_COMMA,
    SQL,
)


class SqlStep:
    def __init__(self, parent, subquery, selects, uids, order):
        self.parent: SqlStep = parent  # THE PARENT STEP
        self.subquery: Expression = subquery  # ASSUMED TO BE A SUB QUERY, INCLUDING THE id AND order COLUMNS
        self.selects: Tuple[SqlAliasOp] = selects  # THE NAME/VALUE PAIRS TO SELECT FROM THE SUBQUERY
        self.uids: Tuple[Expression] = uids  # USED FOR JOINING TO PARENT
        self.order: Tuple[Expression] = order  # USED TO SORT THE FINAL RESULT

        self.nested_path = None  # THE SEQUENCE OF TABLES JOINED TO GET HERE
        self.id = None  # EACH subquery IN THE QUERY IS GIVEN AN ID
        self.start = None  # WHERE TO PLACE THE COLUMNS IN THE SELECT
        self.end = None  # THE END OF THE COLUMNS IN THE SELECT

    def position(self, done, all_selects):
        """
        REGISTER ALL SELECTS INTO all_selects AND
        RETURN NESTED PATH
        """
        if self in done:
            return self.nested_path

        if self.parent:
            self.nested_path = (self,) + self.parent.position(done, all_selects)
        else:
            self.nested_path = (self,)

        self.id = len(done)
        done.append(self)
        self.start = len(all_selects)
        for oi, _ in enumerate(self.order):
            all_selects.append(f"o{self.id}_{oi}")
        for ii, _ in enumerate(self.uids):
            all_selects.append(f"i{self.id}_{ii}")
        for ci, _ in enumerate(self.selects):
            all_selects.append(f"c{self.id}_{ci}")
        self.end = len(all_selects)
        return self.nested_path

    def node_sql(self, all_selects):
        """
        SQL TO PULL MINIMUM COLUMNS FOR LEFT JOINS
        """
        columns = [
            *(SqlAliasOp( ov,f"o{self.id}_{oi}") for oi, ov in enumerate(self.order)),
            *(SqlAliasOp( iv,f"i{self.id}_{ii}") for ii, iv, in enumerate(self.uids)),
        ]
        parent_end = self.parent.end if self.parent else 0
        start_of_values = self.start + len(self.order) + len(self.uids)
        return (
            [
                *(SqlAliasOp( NULL,s) for s in all_selects[parent_end : self.start]),
                *(SqlVariable( s) for s in all_selects[self.start: start_of_values]),
                *(SqlAliasOp( NULL,s) for s in all_selects[start_of_values : self.end]),
            ],
            SqlAliasOp(SqlSelectOp(self.subquery, *columns), f"t{self.id}"),
        )

    def leaf_sql(self, all_selects):
        """
        SQL TO PULL ALL COLUMNS FOR LEAF
        """
        columns = [
            *(SqlAliasOp( ov,f"o{self.id}_{oi}") for oi, ov in enumerate(self.order)),
            *(SqlAliasOp( iv,f"i{self.id}_{ii}") for ii, iv, in enumerate(self.uids)),
            *(SqlAliasOp(cv.value, f"c{self.id}_{ci}") for ci, cv in enumerate(self.selects)),
        ]
        parent_end = self.parent.end if self.parent else 0
        return (
            [
                *(SqlAliasOp( NULL,s) for s in all_selects[parent_end : self.start]),
                *(SqlVariable( s) for s in all_selects[self.start: self.end]),
                *(SqlAliasOp( NULL,s) for s in all_selects[self.end :]),
            ],
            SqlAliasOp(SqlSelectOp(self.subquery, *columns), f"t{self.id}"),
        )

    def branch_sql(self, done: List, sql_queries: List[SQL], all_selects: List[str]) -> Tuple:
        """
        return tuple of SqlSteps for caller to make into a SqlScript
        insert branch query into sql_queries
        """
        if self in done:
            return (self,)

        if not self.parent:
            done.append(self)
            selects, leaf = self.leaf_sql(all_selects)
            sql_queries.append(SqlSelectOp(leaf, *selects))
            return (self,)

        nested_path = self.parent.branch_sql(done, sql_queries, all_selects)
        done.append(self)

        # LEFT JOINS FROM ROOT TO LEAF
        sql_selects = []
        sql_joins = []

        selects, frum = nested_path[0].node_sql(all_selects)
        sql_selects.extend(selects)
        for step in nested_path[1:]:
            selects, leaf = step.node_sql(all_selects)
            sql_selects.extend(selects)
            sql_joins.append(SqlJoinOne(
                leaf,
                SqlAndOp(
                    *(
                        SqlEqOp(SqlVariable(f"i{step.id}_{i}"), SqlVariable(f"i{step.parent.id}_{i}"))
                        for i, _ in enumerate(step.parent.uids)
                    )
                ),
            ))
        selects, leaf = self.leaf_sql(all_selects)
        sql_selects.extend(selects)
        sql_joins.append(SqlJoinOne(
            leaf,
            SqlAndOp(
                *(
                    SqlEqOp(SqlVariable( f"i{self.id}_{i}"), SqlVariable( f"i{self.parent.id}_{i}"))
                    for i, _ in enumerate(self.parent.uids)
                )
            ),
        ))
        sql_queries.append(SqlSelectOp(SqlInnerJoinOp(frum, *sql_joins), *sql_selects))
        return nested_path + (self,)


class SqlTree:
    def __init__(self, leaves: List[SqlStep]):
        self.leaves = leaves

    def to_sql(self):
        done = []
        all_selects = []
        for leaf in self.leaves:
            leaf.position(done, all_selects)

        done = []
        sql_queries = []
        for leaf in self.leaves:
            leaf.branch_sql(done, sql_queries, all_selects)

        ordering = list(flatten(
            [
                *(SqlVariable(f"o{n.id}_{oi}") for oi, ov in enumerate(n.order)),
                *(SqlVariable(f"i{n.id}_{ii}") for ii, iv in enumerate(n.uids)),
            ]
            for n in done
        ))
        return ConcatSQL(JoinSQL(SQL_UNION_ALL, sql_queries), SQL_ORDERBY, JoinSQL(SQL_COMMA, ordering),)

