# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_base.expressions.expression import Expression
from mo_future import is_text
from mo_logs import Log


class ScriptOp(Expression):
    """
    ONLY FOR WHEN YOU TRUST THE SCRIPT SOURCE
    """

    def __init__(self, *script, data_type):
        Expression.__init__(self, None)
        if not is_text(script):
            Log.error("expecting text of a script")
        self.simplified = True
        self.script = script
        self._jx_type = data_type

    @classmethod
    def define(cls, expr):
        if ALLOW_SCRIPTING:
            Log.warning(
                "Scripting has been activated:  This has known security holes!!\nscript = {{script|quote}}",
                script=expr.script.term,
            )
            return ScriptOp(expr.script)
        else:
            Log.error("scripting is disabled")

    def vars(self):
        return set()

    def map(self, map_):
        return self

    def __str__(self):
        return str(self.script)
