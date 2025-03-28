# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import ExpOp as _ExpOp, SqlScript
from jx_sqlite.expressions._utils import _binaryop_to_sql


class ExpOp(_ExpOp):
    to_sql = _binaryop_to_sql
