# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_base.expressions import CardinalityOp as _CardinalityOp
from jx_base.expressions.python_script import PythonScript


class CardinalityOp(_CardinalityOp):
    def to_python(self, loop_depth=0):
        if not self.terms:
            return PythonScript({}, loop_depth, "0")
        else:
            return PythonScript({}, loop_depth, "len(set(" + self.terms.to_python(loop_depth) + "))")
