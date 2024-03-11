# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from jx_base.expressions import FormatOp as FormatOp_, SqlScript


class FormatOp(FormatOp_):
    def partial_eval(self, lang):
        return FormatOp(self.frum.partial_eval(lang), self.format.partial_eval(lang))
