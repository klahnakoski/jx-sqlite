# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from jx_base.expressions import BasicMulOp as BasicMulOp_, SqlScript
from jx_sqlite.expressions._utils import basic_multiop_to_sql


class BasicMulOp(BasicMulOp_):
    to_sql = basic_multiop_to_sql
