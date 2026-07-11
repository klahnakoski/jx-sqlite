# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
# CONTRACT TESTS FOR THE RESURRECTED RENAME-MAP NAMESPACE (jx_base.models.names)
# AND ITS SNOWFLAKE BUILDER (mo_sqlite.models.names).  THE SIX Schema.leaves CASES
# MIRROR tests/test_leaves.py; THE REST COVER SHADOWING, PERSPECTIVE, AND FREE VARS.
from mo_dots import Data
from mo_json import NUMBER, STRING
from mo_testing.fuzzytestcase import FuzzyTestCase, add_error_reporting

from jx_base.models.names import AMBIGUOUS
from mo_sqlite.models.names import snowflake_names


def _live_snowflake():
    # SHAPE OBSERVED FROM data=[{"o":3,"_a":[{"b":"x","v":2},...]},...,{"o":4,"c":"x"}]
    return Data(
        query_paths=["testing", "testing._a.$A"],
        columns=[
            dict(name="_id", es_column="_id", es_index="testing", nested_path=["testing"], json_type=STRING),
            dict(name="o", es_column="o.$N", es_index="testing", nested_path=["testing"], json_type=NUMBER),
            dict(name="c", es_column="c.$S", es_index="testing", nested_path=["testing"], json_type=STRING),
            dict(name="__id__", es_column="__id__", es_index="testing._a.$A", nested_path=["testing._a.$A", "testing"], json_type=NUMBER),
            dict(name="__parent__", es_column="__parent__", es_index="testing._a.$A", nested_path=["testing._a.$A", "testing"], json_type=NUMBER),
            dict(name="__order__", es_column="__order__", es_index="testing._a.$A", nested_path=["testing._a.$A", "testing"], json_type=NUMBER),
            dict(name="_a.b", es_column="b.$S", es_index="testing._a.$A", nested_path=["testing._a.$A", "testing"], json_type=STRING),
            dict(name="_a.v", es_column="v.$N", es_index="testing._a.$A", nested_path=["testing._a.$A", "testing"], json_type=NUMBER),
        ],
    )


@add_error_reporting
class TestNamespace(FuzzyTestCase):

    # ------ THE SIX tests/test_leaves.py CONTRACT CASES ------

    def test_exact_es_match(self):
        snowflake = Data(
            query_paths=["test", "test.$A"],
            columns=[
                dict(name="a", es_column="a.$N", es_index="test.$A", nested_path=["test.$A", "test"], json_type=STRING),
                dict(name="a", es_column="a.$S", es_index="test.$A", nested_path=["test.$A", "test"], json_type=STRING),
            ],
        )
        result = snowflake_names(snowflake, "test.$A").leaves("a.$N")
        self.assertEqual(len(result), 1)
        rel, col = result[0]
        self.assertEqual(rel, ".")
        self.assertEqual(col.es_column, "a.$N")

    def test_exact_match_parent(self):
        # A TYPED NAME IS A PHYSICAL ADDRESS: IT DOES NOT TRAVEL THE SCOPE CHAIN
        snowflake = Data(
            query_paths=["test", "test.$A"],
            columns=[
                dict(name="a", es_column="a.$N", es_index="test.$A", nested_path=["test"], json_type=NUMBER),
                dict(name="a", es_column="a.$S", es_index="test.$A", nested_path=["test.$A", "test"], json_type=STRING),
            ],
        )
        result = snowflake_names(snowflake, "test.$A").leaves("a.$N")
        self.assertEqual(result, [])

    def test_exact_match_child(self):
        snowflake = Data(
            query_paths=["test", "test.$A"],
            columns=[
                dict(name="a", es_column="a.$N", es_index="test.$A", nested_path=["test.$A", "test"], json_type=NUMBER),
                dict(name="a", es_column="a.$S", es_index="test.$A", nested_path=["test.$A", "test"], json_type=STRING),
            ],
        )
        result = snowflake_names(snowflake, "test").leaves("a.$N")
        self.assertEqual(result, [])

    def test_match_dot(self):
        snowflake = Data(
            query_paths=["test", "test.$A"],
            columns=[
                dict(name="a", es_column="a.$N", es_index="test.$A", nested_path=["test.$A", "test"], json_type=STRING),
            ],
        )
        result = snowflake_names(snowflake, "test").leaves(".")
        self.assertEqual(len(result), 1)
        rel, col = result[0]
        self.assertEqual(rel, "a")
        self.assertEqual(col.es_column, "a.$N")

    def test_guid_cant_be_found(self):
        snowflake = Data(
            query_paths=["test", "test.a.$A"],
            columns=[
                dict(name="a", es_column="a.$N", es_index="test.$A", nested_path=["test.a.$A", "test"], json_type=STRING),
                dict(name="_id", es_column="_id", es_index="test", nested_path=["test"], json_type=STRING),
            ],
        )
        result = snowflake_names(snowflake, "test.a.$A").leaves("_id")
        self.assertEqual(result, [])

    def test_deep_child_found(self):
        snowflake = Data(
            query_paths=["test", "test.a.$A"],
            columns=[
                dict(name="a.b", es_column="b.$N", es_index="test.a.$A", nested_path=["test.a.$A", "test"], json_type=STRING),
                dict(name="a", es_column="a.$N", es_index="test.$A", nested_path=["test.$A", "test"], json_type=STRING),
                dict(name="_id", es_column="_id", es_index="test", nested_path=["test"], json_type=STRING),
            ],
        )
        result = snowflake_names(snowflake, "test").leaves("a.b")
        self.assertEqual(len(result), 1)
        rel, col = result[0]
        self.assertEqual(rel, ".")
        self.assertEqual(col.es_column, "b.$N")

    # ------ WHOLE-DOCUMENT ENUMERATION (THE CLUSTER-1 HEADLINE BUGS) ------

    def test_dot_from_fact_no_hidden_no_doubling(self):
        # leaves(".") MUST NOT LEAK __id__/__order__/__parent__ NOR DOUBLE NAMES (_a._a.b)
        names = snowflake_names(_live_snowflake(), "testing")
        result = names.leaves(".")
        self.assertEqual(sorted(rel for rel, _ in result), ["_a.b", "_a.v", "c", "o"])

    def test_dot_from_child_is_child_leaves_only(self):
        names = snowflake_names(_live_snowflake(), "testing._a.$A")
        result = names.leaves(".")
        self.assertEqual(sorted(rel for rel, _ in result), ["b", "v"])

    # ------ SHADOWING AND PERSPECTIVE ------

    def test_relative_shadows_absolute(self):
        # FROM THE CHILD ORIGIN: b BINDS IN THE CHILD; o CLIMBS TO FACT; _a.b IS THE
        # FACT-ABSOLUTE NAME, STILL REACHABLE FROM THE DEEP PERSPECTIVE
        names = snowflake_names(_live_snowflake(), "testing._a.$A")

        (rel, col), = names.leaves("b")
        self.assertEqual((rel, col.es_column), (".", "b.$S"))

        (rel, col), = names.leaves("o")
        self.assertEqual((rel, col.es_column), (".", "o.$N"))

        (rel, col), = names.leaves("_a.b")
        self.assertEqual((rel, col.es_column), (".", "b.$S"))

    def test_bare_name_reaches_into_child(self):
        # FROM THE FACT ORIGIN, A DESCENDANT SCOPE SUPPLIES BARE b (LOWEST PRECEDENCE)
        names = snowflake_names(_live_snowflake(), "testing")
        (rel, col), = names.leaves("b")
        self.assertEqual((rel, col.es_column), (".", "b.$S"))

    def test_resolve(self):
        names = snowflake_names(_live_snowflake(), "testing")
        self.assertEqual(resolve_es(names, "o"), ["o.$N"])
        self.assertEqual(resolve_es(names, "_a.v"), ["v.$N"])
        self.assertEqual(names.resolve("no.such.name"), None)

    # ------ RENAME / FREE VARS (THE 4959d0c MACHINERY) ------

    def test_stack_rename(self):
        names = snowflake_names(_live_snowflake(), "testing")
        renamed = names.stack(**{"alias": "_a"})
        self.assertEqual(sorted(rel for rel, _ in renamed.leaves("alias")), ["b", "v"])
        # OLD NAMES STILL VISIBLE UNDERNEATH
        self.assertEqual(resolve_es(renamed, "_a.b"), ["b.$S"])

    def test_stack_shadowing(self):
        names = snowflake_names(_live_snowflake(), "testing")
        shadowed = names.stack(**{"o": "c"})
        # o NOW BINDS TO WHAT c WAS; THE FACT o IS SHADOWED
        self.assertEqual(resolve_es(shadowed, "o"), ["c.$S"])

    def test_union_marks_collisions_ambiguous(self):
        names = snowflake_names(_live_snowflake(), "testing")
        merged = names.union(**{"o": "c"})
        self.assertIs(merged.resolve("o"), AMBIGUOUS)
        # NON-COLLIDING NAMES UNAFFECTED
        self.assertEqual(resolve_es(merged, "c"), ["c.$S"])

    def test_free_var(self):
        # THE variable.py IDIOM AT 4959d0c: ITERATE A NESTED TABLE BY REBINDING "." AND "row"
        names = snowflake_names(_live_snowflake(), "testing")
        var, names = names.add_free_var("testing._a.$A")
        names = names.stack(**{"row": var, ".": var})
        self.assertEqual(names.resolve("row"), ("testing._a.$A",))
        self.assertEqual(names.resolve("."), ("testing._a.$A",))


def resolve_es(names, name):
    return [c.es_column for c in names.resolve(name)]
