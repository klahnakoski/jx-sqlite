# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

import itertools
import os
import signal
import subprocess

import mo_json_config
from jx_base.expressions import QueryOp
from jx_base.meta_columns import query_metadata
from jx_python import jx
from jx_sqlite import Container
from jx_sqlite.query import Facts
from mo_dots import coalesce, from_data, listwrap, Data, startswith_field, to_data, is_many, is_sequence, Null, is_data, \
    tail_field, concat_field
from mo_files import File
from mo_json import json2value
from mo_kwargs import override
from mo_logs import logger, Except, constants
from mo_logs.exceptions import get_stacktrace
from mo_sqlite import SQLang
from mo_testing.fuzzytestcase import assertAlmostEqual
from tests import test_jx
from tests.test_jx import TEST_TABLE

logger.static_template = False


NEW_DB_EACH_RUN = False


class SQLiteUtils(object):
    @override
    def __init__(self, kwargs=None):
        self.container = None
        self.table = None

    def setUp(self):
        if NEW_DB_EACH_RUN and test_jx.global_settings.db.filename:
            File(test_jx.global_settings.db.filename).delete()
        self.container = Container(db=test_jx.global_settings.db)
        self.table = Facts(name="testing", container=self.container)

    def tearDown(self):
        self.container.db.stop()

    def setUpClass(self):
        pass

    def tearDownClass(self):
        pass

    def not_real_service(self):
        return True

    def execute_tests(self, subtest, tjson=False, places=6):
        subtest = to_data(subtest)
        subtest.name = get_stacktrace()[1]["method"]

        if subtest.disable:
            return

        self.fill_container(subtest, typed=tjson)
        self.send_queries(subtest)

    def fill_container(self, subtest, typed=False):
        """
        RETURN SETTINGS THAT CAN BE USED TO POINT TO THE INDEX THAT'S FILLED
        """
        subtest = to_data(subtest)

        try:
            # INSERT DATA
            self.table.insert(subtest.data)
        except Exception as cause:
            logger.error(
                "can not load {{data}} into container", data=subtest.data, cause=cause
            )

        frum = subtest.query["from"]
        if not frum:
            # ASSUME QUERY IS FOR THE TEST_TABLE
            frum = subtest.query["from"] = self.table.name
        else:
            # REPLACE ANY TEST_TABLE WITH THE REAL TABLE NAME
            def replace(v):
                if is_data(v):
                    return {k: replace(v) for k, v in v.items()}
                elif is_many(v):
                    return [replace(v) for v in v]
                elif isinstance(v, str) and startswith_field(v, TEST_TABLE):
                    return concat_field(self.table.name, tail_field(v)[1])
                else:
                    return v
            frum = subtest.query["from"] = replace(frum)
        return Data(index=frum, alias=self.table.name)

    def send_queries(self, subtest):
        subtest = to_data(subtest)

        try:
            # EXECUTE QUERY
            num_expectations = 0
            for k, v in subtest.items():
                if k.startswith("expecting_"):  # WHAT FORMAT ARE WE REQUESTING
                    format = k[len("expecting_") :]
                elif k == "expecting":  # NO FORMAT REQUESTED (TO TEST DEFAULT FORMATS)
                    format = None
                else:
                    continue

                num_expectations += 1
                expected = v

                subtest.query.format = format
                subtest.query.meta.testing = True  # MARK ALL QUERIES FOR TESTING SO FULL METADATA IS AVAILABLE BEFORE QUERY EXECUTION
                try:
                    result = self.execute_query(subtest.query)
                except Exception as cause:
                    cause = Except.wrap(cause)
                    if format == "error":
                        if expected in cause:
                            return
                        else:
                            logger.error(
                                "Query failed, but for wrong reason; expected"
                                " {{expected}}, got {{reason}}",
                                expected=expected,
                                reason=cause,
                            )
                    else:
                        logger.error("did not expect error", cause=cause)

                compare_to_expected(subtest.query, result, expected)
            if num_expectations == 0:
                logger.error(
                    "Expecting test {{name|quote}} to have property named 'expecting_*'"
                    " for testing the various format clauses",
                    {"name": subtest.name},
                )
        except Exception as cause:
            logger.error("Failed test {{name|quote}}", name=subtest.name, cause=cause)

    def execute_update(self, command):
        return self.table.update(command)

    def execute_query(self, query):
        try:
            if "limit" not in query:
                query["limit"] = 10
            if startswith_field(query["from"], self.table.name):
                return self.table.query(query)
            elif startswith_field(query["from"], "meta"):
                return query_metadata(self.table.container, query)
            else:
                logger.error("Do not know how to handle")
        except Exception as cause:
            logger.warning("Failed query", cause)
            raise

    def try_till_response(self, *args, **kwargs):
        self.execute_query(json2value(kwargs["data"].decode("utf8")))


def compare_to_expected(query, result, expect):
    query = to_data(query)
    expect = to_data(expect)

    if result.meta.format == "table":
        assertAlmostEqual(set(result.header), set(expect.header))

        # MAP FROM expected COLUMN TO result COLUMN
        mapping = list(zip(
            *list(zip(
                *filter(
                    lambda v: v[0][1] == v[1][1],
                    itertools.product(
                        enumerate(expect.header), enumerate(result.header)
                    ),
                )
            ))[1]
        ))[0]
        result.header = [result.header[m] for m in mapping]

        if result.data:
            columns = list(zip(*from_data(result.data)))
            result.data = zip(*[columns[m] for m in mapping])

        if not query.sort:
            sort_table(result)
            sort_table(expect)
    elif result.meta.format == "list":
        if query["from"].startswith("meta."):
            pass
        else:
            query = QueryOp.wrap(query, Null, SQLang)

        if not query.sort:
            try:
                # result.data MAY BE A LIST OF VALUES, NOT OBJECTS
                data_columns = jx.sort(
                    set(jx.get_columns(result.data, leaves=True))
                    | set(jx.get_columns(expect.data, leaves=True)),
                    "name",
                )
            except Exception as _:
                data_columns = [{"name": "."}]

            sort_order = listwrap(coalesce(query.edges, query.groupby)) + data_columns

            if is_sequence(expect.data):
                try:
                    expect.data = jx.sort(expect.data, sort_order.name)
                except Exception:
                    pass

            if is_many(result.data):
                try:
                    result.data = jx.sort(result.data, sort_order.name)
                except Exception as cause:
                    logger.warning("sorting failed", cause=cause)

    elif (
        result.meta.format == "cube"
        and len(result.edges) == 1
        and result.edges[0].name == "rownum"
        and not query.sort
    ):
        result_data, result_header = cube2list(result.data)
        result_data = from_data(jx.sort(result_data, result_header))
        result.data = list2cube(result_data, result_header)

        expect_data, expect_header = cube2list(expect.data)
        expect_data = jx.sort(expect_data, expect_header)
        expect.data = list2cube(expect_data, expect_header)

    # CONFIRM MATCH
    assertAlmostEqual(result, expect, places=6)


def cube2list(cube):
    """
    RETURNS header SO THAT THE ORIGINAL CUBE CAN BE RECREATED
    :param cube: A dict WITH VALUES BEING A MULTIDIMENSIONAL ARRAY OF UNIFORM VALUES
    :return: (rows, header) TUPLE
    """
    header = list(from_data(cube).keys())
    rows = []
    for r in zip(*[[(k, v) for v in a] for k, a in cube.items()]):
        row = Data()
        for k, v in r:
            row[k] = v
        rows.append(from_data(row))
    return rows, header


def list2cube(rows, header):
    output = {h: [] for h in header}
    for r in rows:
        for h in header:
            if h == ".":
                output[h].append(r)
            else:
                r = to_data(r)
                output[h].append(r[h])
    return output


def sort_table(result):
    """
    SORT ROWS IN TABLE, EVEN IF ELEMENTS ARE JSON
    """
    data = to_data([{str(i): v for i, v in enumerate(row)} for row in result.data])
    sort_columns = jx.sort(set(jx.get_columns(data, leaves=True).name))
    data = jx.sort(data, sort_columns)
    result.data = [
        tuple(row[str(i)] for i in range(len(result.header))) for row in data
    ]


def error(response):
    response = response.content.decode("utf8")

    try:
        e = Except.new_instance(json2value(response))
    except Exception:
        e = None

    if e:
        logger.error("Failed request", e)
    else:
        logger.error("Failed request\n {{response}}", {"response": response})


def run_app(please_stop, server_is_ready):
    proc = subprocess.Popen(
        [
            "python",
            "active_data\\app.py",
            "--settings",
            "tests/config/elasticsearch.json",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=-1
        # creationflags=CREATE_NEW_PROCESS_GROUP
    )

    while not please_stop:
        line = proc.stdout.readline()
        if not line:
            continue
        if line.find(" * Running on") >= 0:
            server_is_ready.go()
        logger.note("SERVER: {{line}}", {"line": line.strip()})

    proc.send_signal(signal.CTRL_C_EVENT)


# read_alternate_settings
try:
    default_file = File("tests/config/sqlite.json")
    filename = os.environ.get("TEST_CONFIG")
    config_file = File(filename) if filename else default_file
    logger.alert(
        f"Use TEST_CONFIG environment variable to point to config file.  Using {config_file.abs_path}"
    )
    test_jx.global_settings = mo_json_config.get("file://" + config_file.abs_path)
    constants.set(test_jx.global_settings.constants)

    if not test_jx.global_settings.use:
        logger.error('Must have a {"use": type} set in the config file')

    logger.start(test_jx.global_settings.debug)
    test_jx.utils = SQLiteUtils(test_jx.global_settings)
except Exception as cause:
    logger.warning("problem", cause)
