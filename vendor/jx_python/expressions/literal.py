# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from jx_base.expressions import Literal as _Literal
from jx_base.expressions.python_script import PythonScript


class Literal(_Literal):
    def to_python(self, loop_depth=0):
        source = self.json
        if source.endswith(".0"):
            source = source[:-2]
        return PythonScript({}, loop_depth, self.jx_type, source, self)
