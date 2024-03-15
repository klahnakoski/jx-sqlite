# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from dataclasses import dataclass
from typing import Optional

from jx_sqlite import Container
from mo_files import File
from mo_logs import logger
from mo_math import randoms
from mo_sql.utils import GUID, UID
from mo_sqlite import Sqlite
from mo_testing.fuzzytestcase import add_error_reporting, FuzzyTestCase, StructuredLogger_usingList
from mo_threads import Thread, join_all_threads


@add_error_reporting
class TestBasic(FuzzyTestCase):
    @classmethod
    def setUpClass(cls):
        for file in File("sql").children:
            try:
                file.delete()
            except Exception:
                pass

    def _new_file(self):
        return File(f"sql/test{randoms.hex(4)}.sqlite")

    def test_save_and_load(self):
        file = self._new_file()
        file.delete()

        db = Sqlite(file)
        table = Container(db).get_or_create_facts("my_table")
        table.insert([{"os": "linux", "value": 42}])
        table.insert([{"os": "win", "value": 41}])

        db.stop()

        db = Sqlite(file)
        result = Container(db).get_or_create_facts("my_table").query({"select": "os", "where": {"gt": {"value": 0}}})
        self.assertEqual(result, {"meta": {"format": "list"}, "data": [{"os": "linux"}, {"os": "win"}]})

    def test_open_db(self):
        file = self._new_file()
        file.delete()

    def test_reuse_db(self):
        db = Sqlite()
        table1 = Container(db).get_or_create_facts("my_table")
        table2 = Container(db).get_or_create_facts("my_table")

        table1.insert([{"a": "b"}])

        result = table2.query({"select": "."})
        self.assertEqual(result, {"meta": {"format": "list"}, "data": [{"a": "b"}]})

    def test_delete_table(self):
        container = Container(Sqlite())
        table = container.create_or_replace_facts("my_table")
        container.drop(table)
        self.assertNotIn("my_table", container.db.get_tables().name)

    def test_add_nothing(self):
        container = Container(Sqlite())
        table = container.create_or_replace_facts("my_table")
        table.insert([])
        self.assertEqual(table.query({}), {"meta": {"format": "list"}, "data": []})

    def test_no_add(self):
        container = Container(Sqlite())
        table = container.create_or_replace_facts("my_table")
        with self.assertRaises(Exception):
            table.add({})

    def test_simplest_query(self):
        table = Container(Sqlite()).get_or_create_facts("my_table")
        table.insert([{"os": "linux", "value": 42}, {"os": "win", "value": 41}])

        result = table.query({})
        self.assertEqual(
            result, {"meta": {"format": "list"}, "data": [{"os": "linux", "value": 42}, {"os": "win", "value": 41}]}
        )

    def test_insert_with_uuid(self):
        table = Container(Sqlite()).get_or_create_facts("my_table")
        table.insert([{GUID: 42, "value": "test"}])
        table.insert([{GUID: 42, "value": "test2"}])

        result = table.query({"select": ["_id", "value"]})
        self.assertEqual(result, {"meta": {"format": "list"}, "data": [{"_id": 42, "value": "test2"}]})

    def test_insert_with_uid(self):
        table = Container(Sqlite()).get_or_create_facts("my_table")
        with self.assertRaises(Exception):
            table.insert([{UID: 42, "value": "test"}])

    def test_many_container(self):
        sink = StructuredLogger_usingList()
        logger.main_log, old_log = sink, logger.main_log
        try:
            db = Sqlite(debug=True)
            container1 = Container(db)
            container2 = Container(db)

            table_requests = [line for line in sink.lines if "sqlite_master" in line]
            self.assertEqual(len(table_requests), 1)  # ONLY ONE METADATA SCAN
        finally:
            logger.main_log = old_log

    def test_create_and_drop_facts(self):
        db = Sqlite()
        container = Container(db)
        table = container.get_or_create_facts("temp")
        container.drop(table)
        self.assertNotIn("temp", db.get_tables().name)

    def test_insert_dataclass(self):

        @dataclass
        class Temp:
            name: str
            value: int
            amount: Optional[float]

        data = [
            Temp("a", 1, 1.1),
            Temp("b", 2, 2.1),
        ]
        db = Sqlite()
        table = Container(db).get_or_create_facts("temp")
        table.insert(data)
        with db.transaction() as t:
            result = t.query("select * from temp", as_dataclass=Temp)

        self.assertEqual(result, data)

    def test_many_container(self):

        def make_one(db, please_stop):
            return Container(db)

        db = Sqlite()
        threads = [Thread.run(str(i), make_one, db) for i in range(100)]
        join_all_threads(threads)

    def test_create_copy_of_table(self):
        db = Sqlite()
        container = Container(db)
        table = container.get_or_create_facts("temp")
        table.insert([
            {"name":"1", "value":1, "amount":1.1},
            {"name":"2", "value":2, "amount":2.2}
        ])

        with db.transaction() as t:
            t.execute("create table temp2 as select * from temp limit 0")

        table2 = container.get_or_create_facts("temp2")
        result = table2.query({"format":"table"})
        self.assertEqual(result, {"meta": {"format": "table"}, "header":{"name", "value", "amount"}, "data": []})


