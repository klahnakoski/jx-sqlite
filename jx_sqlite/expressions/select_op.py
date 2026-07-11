# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base import FALSE
from jx_base.expressions import SelectOp as _SelectOp, LeavesOp, NULL, SqlScript
from jx_base.expressions.variable import is_variable
from jx_base.language import is_op
from jx_sqlite.expressions._utils import check
from mo_sqlite.expressions.sql_script import SqlScript
from mo_dots import concat_field, literal_field, relative_field, startswith_field, tail_field
from mo_sql.utils import untype_field
from mo_json.types import JX_IS_NULL, to_jx_type
from mo_sqlite.expressions import SqlVariable, SqlSelectOp, SqlAliasOp


class SelectOp(_SelectOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        # setop RE-COMPILES THE SELECT ONCE PER BRANCH (PER TABLE OF THE SNOWFLAKE); EACH
        # SELECT VALUE IS EMITTED IN EXACTLY ONE BRANCH ("EACH SELECT VALUE BELONGS AT QUERY
        # DEPTH"): THE ORIGIN'S BRANCH MAY REACH UP (ONE ANCESTOR VALUE PER ROW) BUT NEVER
        # DOWN (A CHILD HAS MANY ROWS - THAT IS THE CHILD BRANCH'S JOB); A BRANCH BELOW THE
        # ORIGIN OWNS EXACTLY ITS OWN COLUMNS, PUSH NAMES RE-ROOTED TO THE BRANCH (DOCUMENT
        # ASSEMBLY RESTORES THE NESTING).  KLUDGE (Kyle): PER-BRANCH RE-INTERPRETATION OF THE
        # SAME NAMES IS THE WRONG SHAPE - THE SELECT SHOULD RESOLVE ONCE AT THE QUERY ORIGIN
        # AND BRANCHES MERELY PROJECT; SEE docs/NAMES.md
        query_origin = getattr(self.frum, "nested_path", [None])[0]
        at_origin = query_origin is None or schema.nested_path[0] == query_origin
        branch_depth = len(schema.nested_path)
        if at_origin:
            branch_prefix = "."
            keep = lambda col: len(col.nested_path) <= branch_depth  # UP-REACH ONLY
        elif startswith_field(schema.nested_path[0], query_origin):
            # BRANCH BELOW THE ORIGIN: OWNS EXACTLY ITS OWN COLUMNS
            branch_prefix = untype_field(relative_field(schema.nested_path[0], query_origin))[0]
            keep = lambda col: col.nested_path[0] == schema.nested_path[0]
        else:
            # BRANCH ABOVE THE ORIGIN: UID/ORDER PLUMBING ONLY, NO VALUES
            branch_prefix = "."
            keep = lambda col: False

        jx_type = JX_IS_NULL
        sql_terms = []
        for term in self.terms:
            name, expr, agg, default = term.name, term.value, term.aggregate, term.default
            if is_variable(expr):
                var_name = expr.var
                if startswith_field(var_name, "row"):
                    _, var_name = tail_field(var_name)
                cols = list(schema.leaves(var_name))
                if len(cols) == 1:
                    rel_name0, col0 = cols[0]
                    if col0.es_column == var_name:
                        # WHEN WE REQUEST AN ES_COLUMN DIRECTLY, BREAK THE RECURSIVE LOOP
                        full_name = concat_field(name, rel_name0)
                        jx_type |= full_name + to_jx_type(col0.json_type)
                        sql_terms.append(SqlAliasOp(
                            SqlVariable(None, expr.var, jx_type=to_jx_type(col0.json_type)), full_name
                        ))
                        continue

                emitted = False
                for rel_name, col in cols:
                    if not keep(col):
                        continue
                    full_name = relative_field(concat_field(name, rel_name), branch_prefix)
                    jx_type |= full_name + to_jx_type(col.json_type)
                    sql_terms.append(SqlAliasOp(
                        SqlVariable(col.es_index, col.es_column, jx_type=to_jx_type(col.json_type)), full_name
                    ))
                    emitted = True
                if not emitted:
                    sql_terms.append(SqlAliasOp(NULL, name))
            elif is_op(expr, LeavesOp):
                # `a*` CARRIES A PREFIX ("a.") THAT FLATTENS LEAVES INTO LITERAL DOTTED KEYS
                prefix = "" if expr.prefix is NULL else expr.prefix.value
                # PLAIN `*` IS DOCUMENT ASSEMBLY: A NESTED ARRAY IS ITSELF ONE LEAF-VALUE (A
                # SUB-DOCUMENT LIST, ASSEMBLED SEPARATELY), SO DO NOT DESCEND INTO CHILD TABLES.
                # AN EXPLICIT PREFIX (`a.*`) NAMES A PATH AND KEEPS ITS DEEPER LEAVES.
                origin_depth = len(schema.nested_path)
                # `select *` FROM A NESTED ORIGIN INCLUDES THE PARENT'S FIELDS (all_leaves:
                # EVERY SCOPE - ORIGIN SUBTREE + ANCESTOR SCALARS; CONTRAST `select "."`:
                # THE ORIGIN DOC ALONE).  BUT ONLY AT THE QUERY'S OWN ORIGIN: setop RE-COMPILES
                # THE SELECT PER BRANCH, AND A CHILD BRANCH ONLY OWNS ITS OWN SUBTREE.
                query_origin = getattr(self.frum, "nested_path", [None])[0]
                enumerate_leaves = schema.all_leaves if schema.nested_path[0] == query_origin else schema.leaves
                var_names = expr.vars()
                for var_name in var_names:
                    cols = enumerate_leaves(var_name)
                    for rel_name, col in cols:
                        if not prefix and len(col.nested_path) > origin_depth:
                            continue
                        full_name = concat_field(name, literal_field(prefix + rel_name))
                        jx_type |= full_name + to_jx_type(col.json_type)
                        sql_terms.append(SqlAliasOp(
                            SqlVariable(col.es_index, col.es_column, jx_type=to_jx_type(col.json_type)), full_name
                        ))
            else:
                sql_script = expr.to_sql(schema)
                jx_type |= name + to_jx_type(sql_script.jx_type)
                sql_terms.append(SqlAliasOp(sql_script, name))

        return SqlScript(
            jx_type=jx_type,
            expr=SqlSelectOp(self.frum.to_sql(schema), *sql_terms),
            miss=FALSE,
            frum=self,
            schema=schema,
        )
