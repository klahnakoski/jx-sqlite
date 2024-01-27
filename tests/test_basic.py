# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from unittest import TestCase

from mo_files import File

from jx_sqlite import Container
from mo_math import randoms
from mo_sqlite import Sqlite
from mo_testing.fuzzytestcase import add_error_reporting


@add_error_reporting
class TestBasic(TestCase):
    def test_save_and_load(self):
        file = File(f"sql/test{randoms.hex(4)}.sqlite")
        file.delete()

        db = Sqlite(file)
        table = Container(db).get_or_create_facts("my_table")
        table.add({"os": "linux", "value": 42})
        table.add({"os": "win", "value": 41})

        db.stop()

        db = Sqlite(file)
        result = Container(db).get_or_create_facts("my_table").query({"select": "os", "where": {"gt": {"value": 0}}})
        self.assertEqual(result, {"meta": {"format": "list"}, "data": [{"os": "linux"}, {"os": "win"}]})
