# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import UnixOp as _UnixOp, SqlScript
from jx_sqlite.expressions._utils import check
from mo_dots import wrap
from mo_sqlite import sql_iso


class UnixOp(_UnixOp):
    @check
    def to_sql(self, schema) -> SqlScript:
        v = self.value.to_sql(schema)[0].sql
        return wrap([{"name": ".", "sql": {"n": "UNIX_TIMESTAMP" + sql_iso(v.n)}}])
