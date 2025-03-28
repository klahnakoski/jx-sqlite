# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_math import randoms

from jx_base.expressions import Expression
from jx_base.expressions import PythonFunction as _PythonFunction
from jx_base.expressions.python_script import PythonScript
from jx_python.utils import wrap_function
from mo_json import JX_ANY


class PythonFunction(_PythonFunction):
    """
    Box a Python function, source unknown
    """

    def __init__(self, func, jx_type=JX_ANY):
        Expression.__init__(self, None)
        self.func = wrap_function(func)
        self._jx_type = jx_type
        self._name = f"boxed_function{randoms.string(8)}"

    def to_python(self, loop_depth=0):
        return PythonScript({self._name: self.func}, loop_depth, self.jx_type, f"{self._name}(row{loop_depth})", self,)

    def __data__(self):
        return {"python_function": {}}

    def __str__(self):
        return "<python function>"
