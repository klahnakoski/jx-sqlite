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
from mo_future import text, none_type, PY2, long, items, is_text
from mo_logs import Log
from mo_times import Date


def ToJsonType(value):
    if isinstance(value, JsonType):
        return value
    return _type_to_json_type[value]


def FromJsonType(value):
    for k, v in _type_to_json_type.items():
        if k is value or v is value:
            return k
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

    def __eq__(self, other):
        if not isinstance(other, JsonType):
            return False

        if self is T_INTEGER or self is T_NUMBER:
            if other is T_INTEGER or other is T_NUMBER:
                return True

        sd = self.__dict__
        od = other.__dict__
        for k, sv in sd.items():
            ov = od.get(k)
            if sv != ov:
                return False

        return len(sd) == len(od)

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
        return {k: v.__data__() if isinstance(v, JsonType) else str(v) for k, v in self.__dict__.items()}

    def __str__(self):
        return str(self.__data__())


def union_type(*types):
    output = T_IS_NULL

    for t in types:
        output |= t
    return output


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
NESTED = "nested"
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
    NESTED: NESTED,
    EXISTS: EXISTS,
}
JSON_TYPES = (BOOLEAN, INTEGER, NUMBER, STRING, OBJECT)
NUMBER_TYPES = (INTEGER, NUMBER, TIME, INTERVAL)
PRIMITIVE = (EXISTS, BOOLEAN, INTEGER, NUMBER, TIME, INTERVAL, STRING)
INTERNAL = (EXISTS, OBJECT, NESTED)
STRUCT = (OBJECT, NESTED)

NESTED_KEY = "~a~"  # "a" FOR ARRAY

T_IS_NULL = _new(JsonType)
T_BOOLEAN = _primitive("~b~", BOOLEAN)
T_INTEGER = _primitive("~i~", INTEGER)
T_NUMBER = _primitive("~n~", NUMBER)
T_TIME = _primitive("~t~", TIME)
T_INTERVAL = _primitive("~d~", INTERVAL)
T_STRING = _primitive("~s~", STRING)
T_NESTED = _primitive(NESTED_KEY, NESTED)

T_PRIMITIVE = (T_BOOLEAN, T_INTEGER, T_NUMBER, T_TIME, T_INTERVAL, T_STRING)
T_NUMBER_TYPES = (T_INTEGER, T_NUMBER, T_TIME, T_INTERVAL)

_type_to_json_type = {
    IS_NULL: T_IS_NULL,
    BOOLEAN: T_BOOLEAN,
    INTEGER: T_INTERVAL,
    NUMBER: T_NUMBER,
    TIME: T_TIME,
    INTERVAL: T_INTERVAL,
    STRING: T_STRING,
    NESTED: T_NESTED
}


def value_to_json_type(value):
    if is_many(value):
        return _primitive(NESTED_KEY, union_type(*value))
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

