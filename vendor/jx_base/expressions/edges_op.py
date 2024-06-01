# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from jx_base.expressions._utils import symbiotic
from jx_base.expressions.expression import Expression, MissingOp
from mo_json import array_of


class EdgesOp(Expression):
    """
    return a series of {"edges": edges, "part": list_of_rows_for_edges}
    """

    def __init__(self, frum, edges):
        Expression.__init__(self, frum, edges)
        self.frum, self.edges = frum, edges

    def __data__(self):
        return symbiotic(EdgesOp, self.frum, self.edges.__data__())

    def vars(self):
        return self.frum.vars() | self.edges.vars()

    def map(self, map_):
        return EdgesOp(self.frum.map(map_), self.edges.map(map_))

    @property
    def jx_type(self):
        return array_of(self.frum.jx_type)

    def missing(self, lang):
        return MissingOp(self)

    def invert(self, lang):
        return self.missing(lang)
