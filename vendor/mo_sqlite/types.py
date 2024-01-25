# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_json import (
    BOOLEAN,
    INTEGER,
    NUMBER,
    STRING,
    OBJECT,
    ARRAY,
    JX_BOOLEAN,
    JX_INTEGER,
    JX_NUMBER,
    JX_TIME,
    JX_INTERVAL,
    JX_TEXT,
)

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
