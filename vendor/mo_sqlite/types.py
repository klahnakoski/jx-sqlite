# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_sql.utils import *

json_type_to_sqlite_type = {
    BOOLEAN: "TINYINT",
    INTEGER: "INTEGER",
    NUMBER: "REAL",
    STRING: "TEXT",
    OBJECT: "TEXT",
    ARRAY: "TEXT",
    JX_BOOLEAN: "TINYINT",
    JX_INTEGER: "INTEGER",
    JX_NUMBER: "REAL",
    JX_TIME: "REAL",
    JX_INTERVAL: "REAL",
    JX_TEXT: "TEXT",
}

sqlite_type_to_json_type = {
    "TEXT": STRING,
    "REAL": NUMBER,
    "INT": INTEGER,
    "INTEGER": INTEGER,
    "TINYINT": BOOLEAN,
}

sqlite_type_to_sql_type_key = {
    "ARRAY": SQL_ARRAY_KEY,
    "TEXT": SQL_STRING_KEY,
    "REAL": SQL_NUMBER_KEY,
    "INTEGER": SQL_INTEGER_KEY,
    "TINYINT": SQL_BOOLEAN_KEY,
    "TRUE": SQL_BOOLEAN_KEY,
    "FALSE": SQL_BOOLEAN_KEY,
}

sql_type_key_jx_type = {
    SQL_ARRAY_KEY: JX_ARRAY,
    SQL_STRING_KEY: JX_TEXT,
    SQL_NUMBER_KEY: JX_NUMBER,
    SQL_INTEGER_KEY: JX_INTEGER,
    SQL_BOOLEAN_KEY: JX_BOOLEAN,
}


sql_type_key_to_sqlite_type = {
    SQL_BOOLEAN_KEY: "TINYINT",
    SQL_NUMBER_KEY: "REAL",
    SQL_INTEGER_KEY: "INTEGER",
    SQL_STRING_KEY: "TEXT",
    SQL_TIME_KEY: "REAL",
}


def python_type_to_sql_type_key(python_type):
    jx_type = python_type_to_jx_type(python_type)
    json_type = jx_type_to_json_type(jx_type)
    return json_type_to_sql_type_key[json_type]
