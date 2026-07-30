# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from mo_dots import Data, Null, coalesce, get_module, is_sequence
from mo_future import text, transpose, xrange
from mo_kwargs import override
from mo_logs import Log


class Tensor:
    """
    SIMPLE n-DIMENSIONAL ARRAY OF OBJECTS
    """

    ZERO = None

    @override
    def __init__(self, dims=[], list=None, value=None, zeros=None, kwargs=None):
        if list != None:
            self.num = 1
            self.dims = (len(list),)
            self.cube = list
            return

        if value != None:
            self.num = 0
            self.dims = tuple()
            self.cube = value
            return

        if any(d < 0 for d in dims):
            Log.error("Expecting non-negative dimensions, not {dims}", dims=dims)

        self.num = len(dims)
        self.dims = tuple(dims)
        if zeros != None:
            if self.num == 0 or any(d == 0 for d in dims):  # NO DIMS, OR HAS A ZERO DIM, THEN IT IS A NULL CUBE
                if hasattr(zeros, "__call__"):
                    self.cube = zeros()
                else:
                    self.cube = zeros
            else:
                self.cube = _zeros(dims, zero=zeros)
        else:
            if self.num == 0 or any(d == 0 for d in dims):  # NO DIMS, OR HAS A ZERO DIM, THEN IT IS A NULL CUBE
                self.cube = Null
            else:
                self.cube = _zeros(dims, zero=Null)

    @staticmethod
    def wrap(array):
        return Tensor(list=array)

    @staticmethod
    def _sub_tensor(dims, cube):
        """
        FAST PATH FOR THE SUB-TENSORS MADE BY __getitem__; ALL PROPERTIES ARE KNOWN
        """
        output = Tensor.__new__(Tensor)
        output.num = len(dims)
        output.dims = dims
        output.cube = cube
        return output

    def __getitem__(self, index):
        if not is_sequence(index):
            if isinstance(index, slice):
                if self.num == 0:
                    Log.error("can not slice a tensor with no dimensions")
                # THE SLICE APPLIES TO THE FIRST DIMENSION, THE REST ARE UNTOUCHED
                sub = self.cube[index]
                return Tensor._sub_tensor((len(sub),) + self.dims[1:], sub)
            elif self.num == 1:
                return self.cube[index]  # SIMPLE VALUE
            else:
                index = (index,)

        if len(index) > self.num:
            Log.error(
                "Expecting coordinates to have no more than {num} dimensions, not {index}", num=self.num, index=index,
            )

        dims, cube = _getitem(self.cube, index)
        # THE DIMENSIONS NO COORDINATE REACHED ARE UNTOUCHED
        dims = dims + self.dims[len(index) :]

        if len(dims) == 0:
            return cube  # SIMPLE VALUE

        return Tensor._sub_tensor(dims, cube)

    def __setitem__(self, key, value):
        if isinstance(key, int):
            key = (key,)
        if not is_sequence(key) or len(key) != self.num:
            Log.error(
                "Expecting coordinates to match the {num} dimensions, not {key}", num=self.num, key=key,
            )

        if self.num == 0:
            self.cube = value
            return

        last = self.num - 1
        try:
            m = self.cube
            for k in key[0:last:]:
                m = m[k]
            m[key[last]] = value
        except Exception as cause:
            Log.error("can not set item at {key}", key=key, cause=cause)

    def __bool__(self):
        return self.cube != None

    def __nonzero__(self):
        return self.cube != None

    def __len__(self):
        # NUMBER OF VALUES IN THIS CUBE; NO DIMENSIONS IS STILL ONE VALUE
        return _product(self.dims)

    @property
    def value(self):
        if self.num:
            Log.error("can not get value of with dimension")
        return self.cube

    def __lt__(self, other):
        return self.value < other

    def __gt__(self, other):
        return self.value > other

    def __eq__(self, other):
        if other == None:
            return not self.num and self.cube == None
        if isinstance(other, Tensor):
            return self.dims == other.dims and self.cube == other.cube
        # COMPARE THE WHOLE CUBE; A DIMENSIONAL TENSOR MATCHES ITS NESTED LISTS
        return self.cube == other

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

    def __iter__(self):
        return self.items()

    def __float__(self):
        return float(self.value)

    def groupby(self, io_select):
        """
        SLICE THIS TENSOR INTO ONES WITH LESS DIMENSIONALITY
        io_select - 1 IF GROUPING BY THIS DIMENSION, 0 IF FLATTENING
        return -
        """

        if len(io_select) != self.num:
            Log.error(
                "Expecting io_select to have {num} dimensions, not {io_select}", num=self.num, io_select=io_select,
            )

        # offsets WILL SERVE TO MASK DIMS WE ARE NOT GROUPING BY, AND SERVE AS RELATIVE INDEX FOR EACH COORDINATE
        # ONLY THE GROUPED DIMS CONTRIBUTE TO THE OFFSET, SO THE OUTPUT HAS ONE ENTRY PER GROUP
        offsets = []
        new_dim = []
        acc = 1
        for i, d in reversed(list(enumerate(self.dims))):
            if io_select[i]:
                offsets.insert(0, acc)
                acc *= d
            else:
                new_dim.insert(0, d)
                offsets.insert(0, 0)

        if not new_dim:
            # WHEN groupby ALL DIMENSIONS, ONLY THE VALUES REMAIN
            # RETURN AN ITERATOR OF PAIRS (c, v), WHERE
            # c - COORDINATES INTO THE CUBE
            # v - VALUE AT GIVEN COORDINATES
            return ((c, self[c]) for c in self._all_combos())
        else:
            output = [[None, Tensor(dims=new_dim)] for i in range(acc)]
            _groupby(self.cube, 0, offsets, 0, output, tuple(), [])

        return output

    def aggregate(self, type):
        func = aggregates[type]
        if not func:
            Log.error("Aggregate of type {type} is not supported yet", type=type)

        return func(self.num, self.cube)

    def forall(self, method):
        """
        IT IS EXPECTED THE method ACCEPTS (value, coord, cube), WHERE
        value - VALUE FOUND AT ELEMENT
        coord - THE COORDINATES OF THE ELEMENT (PLEASE, READ ONLY)
        cube - THE WHOLE CUBE, FOR USE IN WINDOW FUNCTIONS
        """
        for c in self._all_combos():
            method(self[c], c, self.cube)

    def items(self):
        """
        ITERATE THROUGH ALL coord, value PAIRS
        """
        for c in self._all_combos():
            _, value = _getitem(self.cube, c)
            yield c, value

    def _all_combos(self):
        """
        RETURN AN ITERATOR OF ALL COORDINATES
        """
        combos = _product(self.dims)
        if not combos:
            return

        calc = [(coalesce(_product(self.dims[i + 1 :]), 1), mm) for i, mm in enumerate(self.dims)]

        for c in xrange(combos):
            yield tuple(int(c / dd) % mm for dd, mm in calc)

    def __str__(self):
        return "Tensor " + get_module("mo_json").value2json(self.dims) + ": " + str(self.cube)

    def __data__(self):
        return self.cube


Tensor.ZERO = Tensor(value=None)


def _max(depth, cube):
    if depth == 0:
        return cube
    elif depth == 1:
        return _MAX(cube)
    else:
        return _MAX(_max(depth - 1, c) for c in cube)


def _min(depth, cube):
    if depth == 0:
        return cube
    elif depth == 1:
        return _MIN(cube)
    else:
        return _MIN(_min(depth - 1, c) for c in cube)


aggregates = Data(max=_max, maximum=_max, min=_min, minimum=_min)


def _iter(cube, depth):
    if depth == 1:
        return cube.__iter__()
    else:

        def iterator():
            for c in cube:
                for b in _iter(c, depth - 1):
                    yield b

        return iterator()


def _zeros(dims, zero):
    d0 = dims[0]
    if d0 == 0:
        Log.error("Zero dimensions not allowed")
    if len(dims) == 1:
        if hasattr(zero, "__call__"):
            return [zero() for _ in range(d0)]
        else:
            return [zero] * d0
    else:
        return [_zeros(dims[1::], zero) for _ in range(d0)]


def _groupby(cube, depth, intervals, offset, output, group, new_coord):
    if depth == len(intervals):
        output[offset][0] = group
        output[offset][1][new_coord] = cube
        return

    interval = intervals[depth]

    if interval:
        for i, c in enumerate(cube):
            _groupby(c, depth + 1, intervals, offset + i * interval, output, group + (i,), new_coord)
    else:
        for i, c in enumerate(cube):
            _groupby(c, depth + 1, intervals, offset, output, group + (-1,), new_coord + [i])


def _getitem(c, i):
    if len(i) == 0:
        # ALL COORDINATES CONSUMED, THE REST OF THE CUBE IS THE VALUE
        return (), c

    select = i[0]
    if select == None:
        return _sub_cube(c, i[1::])
    elif isinstance(select, slice):
        return _sub_cube(c[select], i[1::])
    else:
        return _getitem(c[select], i[1::])


def _sub_cube(sub, i):
    """
    APPLY THE REMAINING COORDINATES TO EACH MEMBER OF sub
    """
    if not sub:
        return (0,), []
    dims, cube = transpose(*[_getitem(cc, i) for cc in sub])
    return (len(cube),) + dims[0], list(cube)


def _zero_dim(value):
    return tuple()


def index_to_coordinate(dims):
    """
    RETURN A FUNCTION THAT WILL TAKE AN INDEX, AND MAP IT TO A coordinate IN dims

    :param dims: TUPLE WITH NUMBER OF POINTS IN EACH DIMENSION
    :return: FUNCTION
    """
    _ = divmod  # SO WE KEEP THE IMPORT

    num_dims = len(dims)
    if num_dims == 0:
        return _zero_dim

    prod = [1] * num_dims
    acc = 1
    domain = range(0, num_dims)
    for i in reversed(domain):
        prod[i] = acc
        acc *= dims[i]

    commands = []
    coords = []
    for i in domain:
        if i == num_dims - 1:
            commands.append("\tc" + str(i) + " = index")
        else:
            commands.append("\tc" + str(i) + ", index = divmod(index, " + str(prod[i]) + ")")
        coords.append("c" + str(i))
    output = None
    if num_dims == 1:
        code = "def output(index):\n" + "\n".join(commands) + "\n" + "\treturn " + coords[0] + ","
    else:
        code = "def output(index):\n" + "\n".join(commands) + "\n" + "\treturn " + ", ".join(coords)

    fake_locals = {}
    exec(code, globals(), fake_locals)
    return fake_locals["output"]


def _product(values):
    output = 1
    for v in values:
        output *= v
    return output


def _MIN(values):
    output = None
    for v in values:
        if v == None:
            continue
        elif output == None or v < output:
            output = v
        else:
            pass
    return output


def _MAX(values):
    output = None
    for v in values:
        if v == None:
            continue
        elif output == None or v > output:
            output = v
        else:
            pass
    return output
