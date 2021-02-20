#
# ALL_TYPES = {IS_NULL: IS_NULL, BOOLEAN: BOOLEAN, INTEGER: INTEGER, NUMBER: NUMBER, TIME:TIME, INTERVAL:INTERVAL, STRING: STRING, OBJECT: OBJECT, NESTED: NESTED, EXISTS: EXISTS}
# JSON_TYPES = (BOOLEAN, INTEGER, NUMBER, STRING, OBJECT)
# NUMBER_TYPES = (INTEGER, NUMBER, TIME, INTERVAL)
# PRIMITIVE = (EXISTS, BOOLEAN, INTEGER, NUMBER, TIME, INTERVAL, STRING)
# INTERNAL = (EXISTS, OBJECT, NESTED)
# STRUCT = (OBJECT, NESTED)
from datetime import datetime, date
from decimal import Decimal

from mo_dots import split_field, NullType, is_many, is_data, concat_field
from mo_future import text, none_type, PY2, long, items, first
from mo_logs import Log
from mo_times import Date


def ToJsonType(value):
    if isinstance(value, JsonType):
        return value
    try:
        return _type_to_json_type[value]
    except Exception:
        return T_UNKNOWN


def FromJsonType(value):
    value = base_type(value)
    for simple_type, complex_type in _type_to_json_type.items():
        if simple_type is value or complex_type is value or complex_type == value:
            return simple_type
    return OBJECT


class JsonType(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            if isinstance(v, JsonType):
                setattr(self, k, v)
            else:
                Log.error("Not allowed")

    def __or__(self, other):
        other = ToJsonType(other)
        sd = self.__dict__.copy()
        od = other.__dict__

        dirty = False
        for k, ov in od.items():
            sv = sd.get(k)
            if sv is ov:
                continue
            if sv is None:
                if k in T_NUMBER_TYPES.__dict__ and sd.get(_N):
                    continue
                elif k is _N and any(sd.get(kk) for kk in T_NUMBER_TYPES.__dict__.keys()):
                    for kk in T_NUMBER_TYPES.__dict__.keys():
                        del sd[kk]
                    sd[k] = T_NUMBER.__dict__[k]
                    dirty = True
                    continue
                sd[k] = ov
                dirty = True
                continue
            if isinstance(sv, JsonType) and isinstance(ov, JsonType):
                new_value = sv | ov
                if new_value is sv:
                    continue
                sd[k] = new_value
                dirty = True
                continue

            Log.error("Not expected")

        if not dirty:
            return self

        output = _new(JsonType)
        output.__dict__ = sd
        return output

    def __hash__(self):
        return hash(self.tuple())

    def leaves(self):
        if self in T_PRIMITIVE:
            yield ".", self
        else:
            for k, v in self.__dict__.items():
                for p, t in v.leaves():
                    yield concat_field(k, p), t

    def tuple(self):
        return tuple(
            (k, v.tuple() if isinstance(v, JsonType) else v)
            for k, v in sorted(self.__dict__.items(), key=lambda p: p[0])
        )

    def __contains__(self, item):
        if not isinstance(item, JsonType):
            return False
        sd = self.__dict__
        od = item.__dict__
        for k, ov in od.items():
            sv = sd.get(k)
            if sv != ov:
                return False
        return True

    def __eq__(self, other):
        if not isinstance(other, JsonType):
            return False

        if self is T_INTEGER or self is T_NUMBER:
            if other is T_INTEGER or other is T_NUMBER:
                return True

        # DETECT DIFFERENCE BY ONLY NAME DEPTH
        sd = base_type(self).__dict__
        od = base_type(other).__dict__

        if len(sd) != len(od):
            return False

        try:
            for k, sv in sd.items():
                ov = od.get(k)
                if sv != ov:
                    return False
            return True
        except Exception as cause:
            sd = self.__dict__
            od = other.__dict__

            # DETECT DIFFERENCE BY ONLY NAME DEPTH
            sd = base_type(sd)
            od = base_type(od)

            Log.error("not expected", cause)

    def __radd__(self, path):
        """
        RETURN self AT THE END OF path
        :param path
        """

        acc = self
        for step in reversed(split_field(path)):
            acc = JsonType(**{step: acc})
        return acc

    def __data__(self):
        return {
            k: v.__data__() if isinstance(v, JsonType) else str(v)
            for k, v in self.__dict__.items()
        }

    def __str__(self):
        return str(self.__data__())


def base_type(type_):
    """
    TYPES OFTEN COME WITH SIMPLE NAMES THAT GET IN THE WAY OF THE "BASE TYPE"
    THIS WILL STRIP EXTRANOIUS NAMES, RETURNING THE MOST BASIC TYPE
    EITHER A PRIMITIVE, OR A STRUCTURE

    USE THIS WHEN MANIPULATING FUNCTIONS THAT ACT ON VALUES, NOT STRUCTURES
    EXAMPLE: {"a": {"~n~": number}} REPRESENTS BOTH A STRUCTURE {"a": 1} AND A NUMBER
    """
    d = type_.__dict__
    ld = len(d)
    while ld == 1:
        n, t = first(d.items())
        if n in _primitive_type_codes:
            return type_
        if n in (_A, _U):
            return type_
        type_ = t
        d = t.__dict__
        ld = len(d)
    return type_


def union_type(*types):
    output = T_IS_NULL

    for t in types:
        output |= t
    return output


def array_type(item_type):
    return _primitive(_A, item_type)


_new = object.__new__


def _primitive(name, value):
    output = _new(JsonType)
    setattr(output, name, value)
    return output


IS_NULL = "0"
BOOLEAN = "boolean"
INTEGER = "integer"
NUMBER = "number"
TIME = "time"
INTERVAL = "interval"
STRING = "string"
OBJECT = "object"
ARRAY = "nested"
EXISTS = "exists"

ALL_TYPES = {
    IS_NULL: IS_NULL,
    BOOLEAN: BOOLEAN,
    INTEGER: INTEGER,
    NUMBER: NUMBER,
    TIME: TIME,
    INTERVAL: INTERVAL,
    STRING: STRING,
    OBJECT: OBJECT,
    ARRAY: ARRAY,
    EXISTS: EXISTS,
}
JSON_TYPES = (BOOLEAN, INTEGER, NUMBER, STRING, OBJECT)
NUMBER_TYPES = (INTEGER, NUMBER, TIME, INTERVAL)
PRIMITIVE = (EXISTS, BOOLEAN, INTEGER, NUMBER, TIME, INTERVAL, STRING)
INTERNAL = (EXISTS, OBJECT, ARRAY)
STRUCT = (OBJECT, ARRAY)

_B = "~b~"
_I = "~i~"
_N = "~n~"
_T = "~t~"
_D = "~d~"
_S = "~s~"
_U = "~u~"
_A = "~a~"
_primitive_type_codes = (_B, _I, _N, _T, _D, _S)

T_IS_NULL = _new(JsonType)
T_BOOLEAN = _primitive(_B, BOOLEAN)
T_INTEGER = _primitive(_I, INTEGER)
T_NUMBER = _primitive(_N, NUMBER)
T_TIME = _primitive(_T, TIME)
T_INTERVAL = _primitive(_D, INTERVAL)  # d FOR DELTA
T_STRING = _primitive(_S, STRING)
T_ARRAY = _primitive(_A, ARRAY)
T_UNKNOWN = _primitive(_U, "unknown")

T_PRIMITIVE = _new(JsonType)
T_PRIMITIVE.__dict__ = [
    (x, x.update(d))[0]
    for x in [{}]
    for d in [
        T_BOOLEAN.__dict__,
        T_INTEGER.__dict__,
        T_NUMBER.__dict__,
        T_TIME.__dict__,
        T_INTERVAL.__dict__,
        T_STRING.__dict__,
    ]
][0]
T_NUMBER_TYPES = _new(JsonType)
T_NUMBER_TYPES.__dict__ = [
    (x, x.update(d))[0]
    for x in [{}]
    for d in [
        T_INTEGER.__dict__,
        T_NUMBER.__dict__,
        T_TIME.__dict__,
        T_INTERVAL.__dict__,
    ]
][0]

_type_to_json_type = {
    IS_NULL: T_IS_NULL,
    BOOLEAN: T_BOOLEAN,
    INTEGER: T_INTERVAL,
    NUMBER: T_NUMBER,
    TIME: T_TIME,
    INTERVAL: T_INTERVAL,
    STRING: T_STRING,
    ARRAY: T_ARRAY,
}


def value_to_json_type(value):
    if is_many(value):
        return _primitive(_A, union_type(*(value_to_json_type(v) for v in value)))
    elif is_data(value):
        return {k: value_to_json_type(v) for k, v in value.items()}
    else:
        return _python_type_to_json_type[value.__class__]


def python_type_to_json_type(type):
    return _python_type_to_json_type[type]


_python_type_to_json_type = {
    int: T_INTEGER,
    text: T_STRING,
    float: T_NUMBER,
    Decimal: T_NUMBER,
    bool: T_BOOLEAN,
    NullType: T_IS_NULL,
    none_type: T_IS_NULL,
    Date: T_TIME,
    datetime: T_TIME,
    date: T_TIME,
}

if PY2:
    _python_type_to_json_type[str] = T_STRING
    _python_type_to_json_type[long] = T_INTEGER


for k, v in items(_python_type_to_json_type):
    _python_type_to_json_type[k.__name__] = v
