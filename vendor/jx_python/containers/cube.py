# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


import mo_dots as dot
from mo_collections.matrix import Matrix
from mo_dots import (
    Data,
    FlatList,
    Null,
    is_data,
    is_list,
    leaves_to_data,
    to_data,
    list_to_data,
    dict_to_data,
)
from mo_future import is_text, transpose, first
from mo_imports import export
from mo_logs import Log
from mo_math import MAX, OR

from jx_base.expressions.edges_op import _normalize_edge
from jx_base.models.container import Container
from jx_base.utils import enlist
from jx_python.containers.cubes.aggs import cube_aggs
from jx_python.containers.lists.aggs import is_aggs


class Cube(Container):
    """
    A CUBE IS LIKE A NUMPY ARRAY, ONLY WITH THE DIMENSIONS TYPED AND NAMED.
    CUBES ARE EASIER TO WORK WITH THAN PANDAS BECAUSE THEY DEAL WITH NULLS GRACEFULLY
    """

    def __init__(self, select, edges, data, frum=None):
        """
        data IS EXPECTED TO BE A dict TO MATRICES, BUT OTHER COLLECTIONS ARE
        ALLOWED, USING THE select AND edges TO DESCRIBE THE data
        """

        self.is_value = False if is_list(select) else True
        self.select = select
        self.meta = Data(format="cube")  # PUT EXTRA MARKUP HERE
        self.is_none = False

        if not all(data.values()):
            is_none = True

        # ENSURE frum IS PROPER FORM
        if is_list(select):
            if edges and OR(not isinstance(v, Matrix) for v in data.values()):
                Log.error("Expecting data to be a dict with Matrix values")

        if not edges:
            if not data:
                if is_list(select):
                    Log.error("not expecting a list of records")

                data = {select.name: Matrix.ZERO}
                self.edges = Null
            elif is_data(data):
                # EXPECTING NO MORE THAN ONE rownum EDGE IN THE DATA
                length = MAX([len(v) for v in data.values()])
                if length >= 1:
                    self.edges = list_to_data([{"name": "rownum", "domain": {"type": "rownum"}}])
                else:
                    self.edges = Null
            elif is_list(data):
                if is_list(select):
                    Log.error("not expecting a list of records")

                data = {select.name: Matrix.wrap(data)}
                self.edges = list_to_data([{
                    "name": "rownum",
                    "domain": {"type": "rownum", "min": 0, "max": len(data), "interval": 1},
                }])
            elif isinstance(data, Matrix):
                if is_list(select):
                    Log.error("not expecting a list of records")

                data = {select.name: data}
            else:
                if is_list(select):
                    Log.error("not expecting a list of records")

                data = {select.name: Matrix(value=data)}
                self.edges = Null
        else:
            self.edges = to_data(edges)

        self.data = data

    def __len__(self):
        """
        RETURN DATA VOLUME
        """
        if not self.edges:
            return 1

        for d in self.data.values():
            return len(d)

    def __iter__(self):
        if self.is_value:
            return self.data[self.select.name].__iter__()

        if not self.edges:
            return list.__iter__([])

        if len(self.edges) == 1 and to_data(self.edges[0]).domain.type == "index":
            # ITERATE AS LIST OF RECORDS
            keys = list(self.data.keys())
            output = (dot.zip(keys, r) for r in zip(*self.data.values()))
            return output

        Log.error("This is a multicube")

    def query(self, q):
        frum = self
        if is_aggs(q):
            return cube_aggs(frum, q)

        columns = dot.dict_to_data({s.name: s for s in self.select + self.edges})

        # DEFER TO ListContainer
        from jx_python.containers.list_container import ListContainer

        frum = ListContainer(name="", data=frum.values(), schema=columns)
        return frum.query(q)

    def values(self):
        """
        TRY NOT TO USE THIS, IT IS SLOW
        """
        matrix = first(self.data.values())  # CANONICAL REPRESENTATIVE
        if matrix.num == 0:
            return
        e_names = self.edges.name
        s_names = self.select.name
        parts = [e.domain.partitions.value if e.domain.primitive else e.domain.partitions for e in self.edges]
        for c in matrix._all_combos():
            try:
                output = {n: parts[i][c[i]] for i, n in enumerate(e_names)}
            except Exception as e:
                Log.error("problem", cause=e)
            for s in s_names:
                output[s] = self.data[s][c]
            yield to_data(output)

    @property
    def value(self):
        if self.is_none:
            return Null
        if self.edges:
            Log.error("can not get value of with dimension")
        if is_list(self.select):
            Log.error("can not get value of multi-valued cubes")
        return self.data[self.select.name].cube

    def __lt__(self, other):
        return self.value < other

    def __gt__(self, other):
        return self.value > other

    def __eq__(self, other):
        if other == None:
            if self.edges:
                return False
            if self.is_value and self.value == None:
                return True
            return False
        return self.value == other

    def __ne__(self, other):
        return not Cube.__eq__(self, other)

    def __add__(self, other):
        return self.value + other

    def __radd__(self, other):
        return other + self.value

    def __sub__(self, other):
        return self.value - other

    def __rsub__(self, other):
        return other - self.value

    def __mul__(self, other):
        return self.value * other

    def __rmul__(self, other):
        return other * self.value

    def __div__(self, other):
        return self.value / other

    def __rdiv__(self, other):
        return other / self.value

    def __truediv__(self, other):
        return self.value / other

    def __rtruediv__(self, other):
        return other / self.value

    def __getitem__(self, item):
        # TODO: SOLVE FUNDAMENTAL QUESTION OF IF SELECTING A PART OF AN
        # EDGE REMOVES THAT EDGE FROM THIS RESULT, OR ADDS THE PART
        # AS A select {"name":edge.name, "value":edge.domain.partitions[coord]}
        # PROBABLY NOT, THE value IS IDENTICAL OVER THE REMAINING
        if is_data(item):
            coordinates = [None] * len(self.edges)

            # MAP DICT TO NUMERIC INDICES
            for name, v in item.items():
                ei, parts = first((i, e.domain.partitions) for i, e in enumerate(self.edges) if e.name == name)
                if not parts:
                    Log.error(
                        "Can not find {{name}}=={{value|quote}} in list of edges, maybe"
                        " this feature is not implemented yet",
                        name=name,
                        value=v,
                    )
                part = first(p for p in parts if p.value == v)
                if not part:
                    return Null
                else:
                    coordinates[ei] = part.dataIndex

            edges = [e for e, v in zip(self.edges, coordinates) if v is None]
            if not edges:
                # ZERO DIMENSIONAL VALUE
                return dict_to_data({k: v.__getitem__(coordinates) for k, v in self.data.items()})
            else:
                output = Cube(
                    select=self.select,
                    edges=list_to_data([e for e, v in zip(self.edges, coordinates) if v is None]),
                    data={k: Matrix(values=c.__getitem__(coordinates)) for k, c in self.data.items()},
                )
                return output
        elif is_text(item):
            # RETURN A VALUE CUBE
            if self.is_value:
                if item != self.select.name:
                    Log.error("{name} not found in cube", name=item)
                return self

            if item not in self.select.name:
                Log.error("{name} not found in cube", name=item)

            output = Cube(
                select=first(s for s in self.select if s.name == item), edges=self.edges, data={item: self.data[item]},
            )
            return output
        else:
            Log.error("not implemented yet")

    def __getattr__(self, item):
        return self.data[item]

    def items(self):
        return self.data.items()

    def get_columns(self):
        return self.edges + enlist(self.select)

    def get_leaves(self):
        return self.edges + enlist(self.select)

    def forall(self, method):
        """
        TODO: I AM NOT HAPPY THAT THIS WILL NOT WORK WELL WITH WINDOW FUNCTIONS
        THE parts GIVE NO INDICATION OF NEXT ITEM OR PREVIOUS ITEM LIKE rownum
        DOES.  MAYBE ALGEBRAIC EDGES SHOULD BE LOOPED DIFFERENTLY?  ON THE
        OTHER HAND, MAYBE WINDOW FUNCTIONS ARE RESPONSIBLE FOR THIS COMPLICATION
        MAR 2015: THE ISSUE IS parts, IT SHOULD BE coord INSTEAD

        IT IS EXPECTED THE method ACCEPTS (value, coord, cube), WHERE
        value - VALUE FOUND AT ELEMENT
        parts - THE ONE PART CORRESPONDING TO EACH EDGE
        cube - THE WHOLE CUBE, FOR USE IN WINDOW FUNCTIONS
        """
        if not self.is_value:
            Log.error("Not dealing with this case yet")

        matrix = first(self.data.values())
        parts = [e.domain.partitions for e in self.edges]
        for c in matrix._all_combos():
            method(matrix[c], [parts[i][cc] for i, cc in enumerate(c)], self)

    def _select(self, select):
        selects = enlist(select)
        is_aggregate = OR(s.aggregate != None and s.aggregate != "none" for s in selects)
        if is_aggregate:
            values = {s.name: Matrix(value=self.data[s.value].aggregate(s.aggregate)) for s in selects}
            return Cube(select, [], values)
        else:
            values = {s.name: self.data[s.value] for s in selects}
            return Cube(select, self.edges, values)

    def filter(self, where):
        if len(self.edges) == 1 and first(self.edges).domain.type == "index":
            # USE THE STANDARD LIST FILTER
            from jx_python import jx

            return jx.filter(first(self.data.values()).cube, where)
        else:
            # FILTER DOES NOT ALTER DIMESIONS, JUST WHETHER THERE ARE VALUES IN THE CELLS
            Log.unexpected("Incomplete")

    def _groupby(self, edges):
        """
        RETURNS LIST OF (coord, values) TUPLES, WHERE
            coord IS THE INDEX INTO self CUBE (-1 INDEX FOR COORDINATES NOT GROUPED BY)
            values ALL VALUES THAT BELONG TO THE SLICE

        """
        edges = FlatList([n for e in edges for n in _normalize_edge(e)])

        stacked = [e for e in self.edges if e.name in edges.name]
        remainder = [e for e in self.edges if e.name not in edges.name]
        selector = [1 if e.name in edges.name else 0 for e in self.edges]

        if len(stacked) + len(remainder) != len(self.edges):
            Log.error("can not find some edges to group by")
        # CACHE SOME RESULTS
        keys = edges.name
        getKey = [e.domain.getKey for e in self.edges]
        lookup = [
            [getKey[i](p) for p in e.domain.partitions + ([None] if e.allowNulls else [])]
            for i, e in enumerate(self.edges)
        ]

        def coord2term(coord):
            output = leaves_to_data({keys[i]: lookup[i][c] for i, c in enumerate(coord)})
            return output

        if is_list(self.select):
            selects = enlist(self.select)
            index, v = transpose(*self.data[selects[0].name].groupby(selector))

            coord = list_to_data([coord2term(c) for c in index])

            values = [v]
            for s in selects[1::]:
                i, v = transpose(*self.data[s.name].group_by(selector))
                values.append(v)

            output = transpose(
                coord,
                [Cube(self.select, remainder, {s.name: v[i] for i, s in enumerate(selects)},) for v in zip(*values)],
            )
        elif not remainder:
            # v IS A VALUE, NO NEED TO WRAP IT IN A Cube
            output = ((coord2term(coord), v) for coord, v in self.data[self.select.name].groupby(selector))
        else:
            output = (
                (coord2term(coord), Cube(self.select, remainder, v))
                for coord, v in self.data[self.select.name].groupby(selector)
            )

        return output

    def groupby(self, edges):
        """
        SLICE THIS CUBE IN TO ONES WITH LESS DIMENSIONALITY
        simple==True WILL HAVE GROUPS BASED ON PARTITION VALUE, NOT PARTITION OBJECTS
        """
        edges = FlatList([n for e in edges for n in _normalize_edge(e)])

        stacked = [e for e in self.edges if e.name in edges.name]
        remainder = [e for e in self.edges if e.name not in edges.name]
        selector = [1 if e.name in edges.name else 0 for e in self.edges]

        if len(stacked) + len(remainder) != len(self.edges):
            Log.error("can not find some edges to group by")
        # CACHE SOME RESULTS
        keys = edges.name
        getKey = [e.domain.getKey for e in self.edges]
        lookup = [
            [getKey[i](p) for p in e.domain.partitions + ([None] if e.allowNulls else [])]
            for i, e in enumerate(self.edges)
        ]

        def coord2term(coord):
            output = leaves_to_data({keys[i]: lookup[i][c] for i, c in enumerate(coord)})
            return output

        if is_list(self.select):
            selects = enlist(self.select)
            index, v = transpose(*self.data[selects[0].name].groupby(selector))

            coord = list_to_data([coord2term(c) for c in index])

            values = [v]
            for s in selects[1::]:
                i, v = zip(*self.data[s.name].group_by(selector))
                values.append(v)

            output = transpose(
                coord,
                [Cube(self.select, remainder, {s.name: v[i] for i, s in enumerate(selects)},) for v in zip(*values)],
            )
        elif not remainder:
            # v IS A VALUE, NO NEED TO WRAP IT IN A Cube
            output = ((coord2term(coord), v) for coord, v in self.data[self.select.name].groupby(selector))
        else:
            output = (
                (coord2term(coord), Cube(self.select, remainder, v))
                for coord, v in self.data[self.select.name].groupby(selector)
            )

        return output

    def window(self, window):
        if window.edges or window.sort:
            raise NotImplementedError()

        from jx_python import jx

        # SET OP
        canonical = first(self.data.values())
        accessor = jx.get(window.value)
        cnames = self.data.keys()

        # ANNOTATE EXISTING CUBE WITH NEW COLUMN
        m = self.data[window.name] = Matrix(dims=canonical.dims)
        for coord in canonical._all_combos():
            row = Data()  # IT IS SAD WE MUST HAVE A Data(), THERE ARE {"script": expression} USING THE DOT NOTATION
            for k in cnames:
                row[k] = self.data[k][coord]
            for c, e in zip(coord, self.edges):
                row[e.name] = e.domain.partitions[c]
            m[coord] = accessor(row, Null, Null)  # DUMMY Null VALUES BECAUSE I DO NOT KNOW WHAT TO DO

        self.select.append(window)
        return self

    def format(self, format):
        if format == None or format == "cube":
            return self
        else:
            Log.error("Do not know how to handle")

    def __str__(self):
        if self.is_value:
            return str(self.data)
        else:
            return str(self.data)

    def __int__(self):
        if self.is_value:
            return int(self.value)
        else:
            return int(self.data)

    def __float__(self):
        if self.is_value:
            v = self.value
            if v == None:
                return v
            else:
                return float(v)
        else:
            return float(self.data)

    def __data__(self):
        return Data(
            select=self.select, edges=self.edges, data={k: v.cube for k, v in self.data.items()}, meta=self.meta,
        )


export("jx_base.models.container", Cube)
