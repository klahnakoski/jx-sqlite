# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#



from unittest import skipIf, skip

from jx_base.expressions import NULL
from mo_dots import list_to_data, concat_field
from mo_sql.utils import SQL_STRING_KEY
from mo_testing.fuzzytestcase import add_error_reporting
from tests.test_jx import BaseTestCase, TEST_TABLE, global_settings

lots_of_data = list_to_data([{"a": i} for i in range(30)])


@add_error_reporting
class TestFilters(BaseTestCase):
    @skipIf(global_settings.use == "sqlite", "broken")
    def test_where_expression(self):
        test = {
            "data": [  # PROPERTIES STARTING WITH _ ARE NESTED AUTOMATICALLY
                {"a": {"b": 0, "c": 0}},
                {"a": {"b": 0, "c": 1}},
                {"a": {"b": 1, "c": 0}},
                {"a": {"b": 1, "c": 1}},
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "*",
                "where": {"eq": ["a.b", "a.c"]},
                "sort": "a.b"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                {"a.b": 0, "a.c": 0},
                {"a.b": 1, "a.c": 1},
            ]},
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.b", "a.c"],
                "data": [[0, 0], [1, 1]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 2, "interval": 1}
                    }
                ],
                "data": {
                    "a.b": [0, 1],
                    "a.c": [0, 1]
                }
            }
        }
        self.utils.execute_tests(test)

    @skipIf(global_settings.use == "sqlite", "broken")
    def test_add_expression(self):
        test = {
            "data": [  # PROPERTIES STARTING WITH _ ARE NESTED AUTOMATICALLY
                {"a": {"b": 0, "c": 0}},
                {"a": {"b": 0, "c": 1}},
                {"a": {"b": 1, "c": 0}},
                {"a": {"b": 1, "c": 1}},
            ],
            "query": {
                "select": "*",
                "from": TEST_TABLE,
                "where": {"eq": [{"add": ["a.b", 1]}, "a.c"]},
                "sort": "a.b"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [
                    {"a.b": 0, "a.c": 1}
                ]
            },
            "expecting_table": {
                "meta": {"format": "table"},
                "header": ["a.b", "a.c"],
                "data": [[0, 1]]
            },
            "expecting_cube": {
                "meta": {"format": "cube"},
                "edges": [
                    {
                        "name": "rownum",
                        "domain": {"type": "rownum", "min": 0, "max": 1, "interval": 1}
                    }
                ],
                "data": {
                    "a.b": [0],
                    "a.c": [1]
                }
            }
        }
        self.utils.execute_tests(test)

    @skipIf(global_settings.use == "sqlite", "broken")
    def test_regexp_expression(self):
        test = {
            "data": [{"_a": [
                {"a": "abba"},
                {"a": "aaba"},
                {"a": "aaaa"},
                {"a": "aa"},
                {"a": "aba"},
                {"a": "aa"},
                {"a": "ab"},
                {"a": "ba"},
                {"a": "a"},
                {"a": "b"}
            ]}],
            "query": {
                "from": concat_field(TEST_TABLE, "_a"),
                "select": "*",
                "where": {"regex": {"a": ".*b.*"}},
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"a": "abba"},
                    {"a": "aaba"},
                    {"a": "aba"},
                    {"a": "ab"},
                    {"a": "ba"},
                    {"a": "b"}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_empty_or(self):
        test = {
            "data": [{"a": 1}],
            "query": {
                "from": TEST_TABLE,
                "select": "*",
                "where": {"or": []}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": []
            }
        }
        self.utils.execute_tests(test)

    def test_empty_and(self):
        test = {
            "data": [{"a": 1}],
            "query": {
                "from": TEST_TABLE,
                "select": "*",
                "where": {"and": []}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"a": 1}]
            }
        }
        self.utils.execute_tests(test)

    def test_empty_in(self):
        test = {
            "data": [{"a": 1}],
            "query": {
                "select": "a",
                "from": TEST_TABLE,
                "where": {"in": {"a": []}}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": []
            }
        }
        self.utils.execute_tests(test)

    def test_in_w_set(self):
        # ENSURE THE SET IS RECOGNIZED LIKE A LIST
        test = {
            "data": [{"a": 1}, {"a": 2}, {"a": 4}],
            "query": {
                "select": "a",
                "from": TEST_TABLE,
                "where": {"in": {"a": {1, 3}}}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [1]
            }
        }
        self.utils.execute_tests(test)

    def test_in_w_missing_column(self):
        # ENSURE THE SET IS RECOGNIZED LIKE A LIST
        test = {
            "data": [{"a": 1}, {"a": 2}, {"a": 4}],
            "query": {
                "select": "a",
                "from": TEST_TABLE,
                "where": {"in": {"b": [1, 3]}}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": []
            }
        }
        self.utils.execute_tests(test)

    def test_empty_match_all(self):
        test = {
            "data": [{"a": 1}],
            "query": {
                "from": TEST_TABLE,
                "select": "*",
                "where": {"match_all": {}}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"a": 1}]
            }
        }
        self.utils.execute_tests(test)

    def test_empty_prefix(self):
        test = {
            "data": [{"v": "test"}],
            "query": {
                "from": TEST_TABLE,
                "select": "*",
                "where": {"prefix": {"v": ""}}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"v": "test"}]
            }
        }
        self.utils.execute_tests(test)

    def test_null_prefix(self):
        test = {
            "data": [{"v": "test"}],
            "query": {
                "from": TEST_TABLE,
                "select": "*",
                "where": {"prefix": {"v": None}}
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"v": "test"}]
            }
        }
        self.utils.execute_tests(test)

    def test_edges_and_empty_prefix(self):
        test = {
            "data": [{"v": "test"}],
            "query": {
                "from": TEST_TABLE,
                "edges": "v",
                "where": {"prefix": {"v": ""}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"v": "test", "count": 1},
                    {"v": NULL, "count": 0}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_edges_and_null_prefix(self):
        test = {
            "data": [{"v": "test"}],
            "query": {
                "from": TEST_TABLE,
                "edges": "v",
                "where": {"prefix": {"v": None}}
            },
            "expecting_list": {
                "meta": {"format": "list"},
                "data": [
                    {"v": "test", "count": 1},
                    {"v": NULL, "count": 0}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_suffix(self):
        test = {
            "data": [
                {"v": "this-is-a-test"},
                {"v": "this-is-a-vest"},
                {"v": "test"},
                {"v": ""},
                {"v": None}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"suffix": {"v": "test"}}
            },
            "expecting_list": {
                "meta": {
                    "format": "list"},
                "data": [
                    {"v": "this-is-a-test"},
                    {"v": "test"}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_null_suffix(self):
        test = {
            "data": [
                {"v": "this-is-a-test"},
                {"v": "this-is-a-vest"},
                {"v": "test"},
                {"v": ""},
                {"v": None}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"postfix": {"v": None}}
            },
            "expecting_list": {
                "meta": {
                    "format": "list"},
                "data": [
                    {"v": "this-is-a-test"},
                    {"v": "this-is-a-vest"},
                    {"v": "test"},
                    {"v": NULL},
                    {"v": NULL}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_empty_suffix(self):
        test = {
            "data": [
                {"v": "this-is-a-test"},
                {"v": "this-is-a-vest"},
                {"v": "test"},
                {"v": ""},
                {"v": None}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"postfix": {"v": ""}}
            },
            "expecting_list": {
                "meta": {
                    "format": "list"},
                "data": [
                    {"v": "this-is-a-test"},
                    {"v": "this-is-a-vest"},
                    {"v": "test"},
                    {"v": NULL},
                    {"v": NULL}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_eq_with_boolean(self):
        test = {
            "data": [
                {"v": True},
                {"v": True},
                {"v": True},
                {"v": False},
                {"v": False},
                {"v": False},
                {"v": None},
                {"v": None},
                {"v": None}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"eq": {"v": "T"}}
            },
            "expecting_list": {
                "meta": {
                    "format": "list"
                },
                "data": [
                    {"v": True},
                    {"v": True},
                    {"v": True}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_big_integers_in_script(self):
        bigger_than_int32 = 1547 * 1000 * 1000 * 1000
        test = {
            "data": [
                {"v": 42}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"lt": [0, {"mul": ["v", bigger_than_int32]}]}  # SOMETHING COMPLICATED ENOUGH TO FORCE SCRIPTING
            },
            "expecting_list": {
                "meta": {
                    "format": "list"
                },
                "data": [
                    {"v": 42}
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_where_is_array(self):
        test = {
            "data": [
                {"a": 1, "b": 1},
                {"a": 1, "b": 2},
                {"a": 2, "b": 1},
                {"a": 2, "b": 2}
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "*",
                "where": [{"eq": {"a": 1}}, {"eq": {"b": 1}}]
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": [{"a": 1, "b": 1}]
            }
        }
        self.utils.execute_tests(test)

    def test_in_using_tuple_of_literals(self):
        test = {
            "data": [
                {"a": "1"},
                {"a": "2"},
                {"a": "3"},
                {"a": "4"},
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "a",
                "where": {"in": ["a", [{"literal": "4"}, {"literal": "2"}]]},
                "sort": "a"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": ["2", "4"]
            }
        }
        self.utils.execute_tests(test)

    def test_eq_using_tuple_of_literals(self):
        test = {
            "data": [
                {"a": "1"},
                {"a": "2"},
                {"a": "3"},
                {"a": "4"},
            ],
            "query": {
                "from": TEST_TABLE,
                "select": "a",
                "where": {"eq": ["a", [{"literal": "4"}, {"literal": "2"}]]},
                "sort": "a"
            },
            "expecting_list": {
                "meta": {"format": "list"}, "data": ["2", "4"]
            }
        }
        self.utils.execute_tests(test)

    @skipIf(not global_settings.elasticsearch.version, "only for ES")
    def test_find_uses_regex_es(self):
        test = {
            "data": [
                {"v": "this-is-a-test"},
                {"v": "this-is-a-vest"},
                {"v": "test"},
                {"v": ""},
                {"v": None}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"find": {"v": "test"}}
            },
            "expecting_list": {
                "meta": {
                    "format": "list",
                    "es_query": {
                        "from": 0,
                        "query": {"regexp": {"v."+SQL_STRING_KEY: ".*test.*"}},
                        "size": 10
                    },
                },
                "data": [
                    {"v": "this-is-a-test"},
                    {"v": "test"},
                ]
            }
        }
        self.utils.execute_tests(test)

    def test_find(self):
        test = {
            "data": [
                {"v": "this-is-a-test"},
                {"v": "this-is-a-vest"},
                {"v": "test"},
                {"v": ""},
                {"v": None}
            ],
            "query": {
                "from": TEST_TABLE,
                "where": {"find": {"v": "test"}}
            },
            "expecting_list": {
                "data": [
                    {"v": "this-is-a-test"},
                    {"v": "test"},
                ]
            }
        }
        self.utils.execute_tests(test)

