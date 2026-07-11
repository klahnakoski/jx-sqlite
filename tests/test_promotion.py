# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
# CONTRACT TESTS FOR SCALAR->ARRAY PROMOTION (snowflake._nest_column + insert.flatten_many).
# HISTORY (2026-07-12): PROMOTION SILENTLY CORRUPTED THE STORE - MOVED COLUMNS GOT
# TABLE-QUALIFIED PHYSICAL NAMES (testing.a.$A.b.$N) WHILE FRESH INSERTS CREATE
# TABLE-RELATIVE (b.$N); NUMBERS WERE MANGLED TO TEXT BY THE WRONG COLUMN TYPE; AND THE
# PROMOTING DOC'S VALUES WERE DROPPED (CROSS-CALL: GARBAGE GRANDCHILD ROWS; SAME-CALL:
# COLLECTED ROWS KEYED BY DROPPED COLUMNS).  THESE TESTS PIN THE PHYSICAL TABLES, THE
# COLUMN METADATA, AND A QUERY ROUND-TRIP.
from mo_files import File
from mo_testing.fuzzytestcase import FuzzyTestCase, add_error_reporting

import jx_sqlite  # ATTACH query() AND FRIENDS TO Facts (@extend)
from mo_sqlite import Container, Facts
from tests import test_jx

NEW_DB_EACH_RUN = True


@add_error_reporting
class TestPromotion(FuzzyTestCase):
    def setUp(self):
        if test_jx.global_settings.db.filename:
            File(test_jx.global_settings.db.filename).delete()
        self.container = Container(db=test_jx.global_settings.db)
        self.facts = Facts(name="testing", container=self.container)

    def tearDown(self):
        self.container.db.stop()

    def _child_rows(self):
        result = self.container.db.query('SELECT * FROM "testing.a.$A" ORDER BY __parent__, __order__')
        return list(result.header), result.data

    def test_cross_call_promotion(self):
        # INNER OBJECT FIRST (SCALAR COLUMN AT FACT), ARRAY IN A SEPARATE INSERT
        self.facts.insert([{"a": {"b": 1}}])
        self.facts.insert([{"a": [{"b": 2}, {"b": 3}]}])

        header, rows = self._child_rows()
        # PHYSICAL COLUMN IS TABLE-RELATIVE, MATCHING FRESH INSERTS
        self.assertIn("b.$N", header)
        self.assertNotIn("testing.a.$A.b.$N", header)
        b = header.index("b.$N")
        order = header.index("__order__")
        # DOC1'S VALUE SURVIVED THE MOVE AS AN ORDER-0 ROW; DOC2'S ARRAY IS COMPLETE AND
        # NUMERIC (NOT TEXT-MANGLED); NO GARBAGE EXTRA ROWS
        self.assertEqual(len(rows), 3)
        self.assertEqual(sorted((r[order], r[b]) for r in rows), [(0, 1.0), (0, 2.0), (1, 3.0)])

    def test_same_call_promotion(self):
        # INNER OBJECT AND ARRAY IN ONE INSERT CALL: THE ALREADY-COLLECTED ROW MUST MOVE TOO
        self.facts.insert([
            {"o": 1, "a": {"b": 1}},
            {"o": 2, "a": [{"b": 2}, {"b": 3}]},
        ])

        header, rows = self._child_rows()
        self.assertIn("b.$N", header)
        b = header.index("b.$N")
        order = header.index("__order__")
        self.assertEqual(len(rows), 3)
        self.assertEqual(sorted((r[order], r[b]) for r in rows), [(0, 1.0), (0, 2.0), (1, 3.0)])

    def test_promoted_metadata_is_canonical(self):
        # THE MOVED COLUMN'S METADATA MUST MATCH A FRESH INSERT'S: TABLE-RELATIVE es_column,
        # es_index = THE CHILD TABLE, nested_path LEAF FIRST
        self.facts.insert([{"a": {"b": 1}}])
        self.facts.insert([{"a": [{"b": 2}]}])

        col = next(
            c
            for c in self.facts.schema.columns
            if c.es_column.endswith("b.$N")
        )
        self.assertEqual(col.es_column, "b.$N")
        self.assertEqual(col.es_index, "testing.a.$A")
        self.assertEqual(col.nested_path[0], "testing.a.$A")

    def test_promotion_query_round_trip(self):
        # THE PROMOTED DATA IS QUERYABLE: select * SEES ALL VALUES, INNER OBJECT UNCHANGED
        self.facts.insert([
            {"o": 1, "a": {"b": 1}},
            {"o": 2, "a": [{"b": 2}, {"b": 3}]},
        ])
        result = self.facts.query({"from": "testing", "select": "*", "sort": "o", "format": "list"})
        self.assertEqual(
            result.data,
            [
                {"o": 1, "a": {"b": 1}},
                {"o": 2, "a": [{"b": 2}, {"b": 3}]},
            ],
        )
