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
        if at_origin:
            # UP-REACH ONLY, AND UP MEANS *THIS LINE*: A COLUMN AT THIS TABLE OR AN ANCESTOR OF
            # IT IS ONE VALUE PER ROW.  A DESCENDANT HAS MANY ROWS (ITS OWN BRANCH'S JOB), AND A
            # TABLE OFF THE LINE - A SIBLING ARRAY - IS A FAN-OUT, NOT A PROPERTY OF THIS
            # DOCUMENT.  A DEPTH TEST (`len(col.nested_path) <= len(schema.nested_path)`) LET A
            # SIBLING THROUGH, BECAUSE IT SITS AT THE SAME DEPTH: THE ARM EMITTED A REFERENCE TO
            # A TABLE IT NEVER JOINS (`no such column: testing.k.$A.z.$N`)
            keep = lambda col: startswith_field(schema.nested_path[0], col.nested_path[0])
        else:
            # BRANCH BELOW THE ORIGIN: OWNS EXACTLY ITS OWN COLUMNS
            keep = lambda col: col.nested_path[0] == schema.nested_path[0]

        jx_type = JX_IS_NULL
        sql_terms = []
        for term in self.terms:
            # expr, NOT value: A TERM CARRYING AN aggregate DECLARATION COMPILES AS ITS OPERATOR
            # FORM (A COLLECTION AGGREGATE OF ONE DOCUMENT); THE TWO ARE ONE OBJECT OTHERWISE
            name, expr, agg, default = term.name, term.expr, term.aggregate, term.default
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
                for resolved in cols:
                    rel_name, col = resolved
                    if not keep(col):
                        continue
                    if at_origin and resolved.push_name == ".":
                        # BINDING IS A VALUE AT THE TERM NAME: COLLAPSE UNDER IT.  literal_field
                        # KEEPS THE BOUNDARY WHEN THE TERM IS NAMED "." (concat_field WOULD ERASE IT)
                        full_name = concat_field(name if name != "." else literal_field("."), resolved.push_child)
                    elif resolved.push_name == "." and resolved.push_child == ".":
                        # EXPLICIT DEEP LEAF SELECTED FROM ABOVE: ONE VALUE PER BRANCH ROW,
                        # ACCUMULATED AS A MULTIVALUE ON THE ORIGIN DOC - KEEP THE TERM-ROOTED
                        # NAME SO ASSEMBLY KNOWS WHERE IT LANDS
                        full_name = name
                    else:
                        # rel_name IS ALREADY RELATIVE TO THIS BRANCH'S ORIGIN, SO THE BRANCH-ROOTED
                        # PUSH NAME IS JUST name+rel_name.  (SUBTRACTING branch_prefix HERE ASSUMED
                        # rel_name WAS ORIGIN-ABSOLUTE; FOR A REAL BELOW-ORIGIN PREFIX LIKE `_a.k` IT
                        # MANUFACTURED AN UP-REF `...b` THAT JxType REJECTS.  A no-op WHEN prefix==".")
                        full_name = concat_field(name, rel_name)
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
