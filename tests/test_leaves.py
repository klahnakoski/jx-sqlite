# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_sqlite import Schema
from mo_dots import Data
from mo_json import STRING, NUMBER
from mo_testing.fuzzytestcase import FuzzyTestCase, add_error_reporting


@add_error_reporting
class TestLeaves(FuzzyTestCase):

    def test_exact_es_match(self):
        schema = Data(
            nested_path=["test.$A", "test"],
            snowflake={"fact_name":"test",  "query_paths": ["test", "test.$A"]},
            columns=[
                dict(name="a", es_column="a.$N", es_index="test.$A", nested_path=["test.$A", "test"], json_type=STRING),
                dict(name="a", es_column="a.$S", es_index="test.$A", nested_path=["test.$A", "test"], json_type=STRING),
            ]
        )

        result = Schema.leaves(schema, "a.$N")
        self.assertEqual(result, [(".", schema.columns[0])])

    def test_exact_match_parent(self):
        schema = Data(
            nested_path=["test.$A", "test"],
            snowflake={"fact_name": "test", "query_paths": ["test", "test.$A"]},
            columns=[
                dict(name="a", es_column="a.$N", es_index="test.$A", nested_path=["test"], json_type=NUMBER),
                dict(name="a", es_column="a.$S", es_index="test.$A", nested_path=["test.$A", "test"], json_type=STRING),
            ]
        )

        result = Schema.leaves(schema, "a.$N")
        self.assertEqual(result, [])

    def test_exact_match_child(self):
        schema = Data(
            nested_path=["test"],
            snowflake={"fact_name": "test", "query_paths": ["test", "test.$A"]},
            columns=[
                dict(name="a", es_column="a.$N", es_index="test.$A", nested_path=["test.$A", "test"], json_type=NUMBER),
                dict(name="a", es_column="a.$S", es_index="test.$A", nested_path=["test.$A", "test"], json_type=STRING),
            ]
        )

        result = Schema.leaves(schema, "a.$N")
        self.assertEqual(result, [])

    def test_match_dot(self):
        schema = Data(
            nested_path=["test"],
            snowflake={"fact_name":"test", "query_paths": ["test", "test.$A"]},
            columns=[
                dict(name="a", es_column="a.$N", es_index="test.$A", nested_path=["test.$A", "test"], json_type=STRING),
            ]
        )

        result = Schema.leaves(schema, ".")
        self.assertEqual(result, [("a", schema.columns[0])])

    def test_guid_cant_be_found(self):
        schema = Data(
            nested_path=["test.a.$A", "test"],
            snowflake={"fact_name":"test", "query_paths": ["test", "test.a.$A"]},
            columns=[
                dict(name="a", es_column="a.$N", es_index="test.$A", nested_path=["test.a.$A", "test"], json_type=STRING),
                dict(name="_id", es_column="_id", es_index="test", nested_path=["test"], json_type=STRING),
            ]
        )

        result = Schema.leaves(schema, "_id")
        self.assertEqual(result, [])

    def test_deep_child_found(self):
        schema = Data(
            nested_path=["test"],
            snowflake={"fact_name": "test", "query_paths": ["test", "test.a.$A"]},
            columns=[
                dict(name="a.b", es_column="b.$N", es_index="test.a.$A", nested_path=["test.a.$A", "test"], json_type=STRING),
                dict(name="a", es_column="a.$N", es_index="test.$A", nested_path=["test.$A", "test"], json_type=STRING),
                dict(name="_id", es_column="_id", es_index="test", nested_path=["test"], json_type=STRING),
            ]
        )

        result = Schema.leaves(schema, "a.b")
        self.assertEqual(result, [(".", schema.columns[0])])



