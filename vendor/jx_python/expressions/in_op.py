# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_base.expressions import InOp as _InOp
from jx_base.expressions.python_script import PythonScript
from jx_python.utils import merge_locals
from mo_json import JX_BOOLEAN


class InOp(_InOp):
    def to_python(self, loop_depth=0):
        value = self.value.to_python(loop_depth)
        superset = self.superset.to_python(loop_depth)
        return PythonScript(
            merge_locals(value.locals, superset.locals),
            loop_depth,
            JX_BOOLEAN,
            f"{value.source} in {superset.source}",
            self,
        )
