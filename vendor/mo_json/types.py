#
# ALL_TYPES = {IS_NULL: IS_NULL, BOOLEAN: BOOLEAN, INTEGER: INTEGER, NUMBER: NUMBER, TIME:TIME, INTERVAL:INTERVAL, STRING: STRING, OBJECT: OBJECT, NESTED: NESTED, EXISTS: EXISTS}
# JSON_TYPES = (BOOLEAN, INTEGER, NUMBER, STRING, OBJECT)
# NUMBER_TYPES = (INTEGER, NUMBER, TIME, INTERVAL)
# PRIMITIVE = (EXISTS, BOOLEAN, INTEGER, NUMBER, TIME, INTERVAL, STRING)
# INTERNAL = (EXISTS, OBJECT, NESTED)
# STRUCT = (OBJECT, NESTED)
from mo_dots import split_field
from mo_json import STRING, INTERVAL, TIME, NUMBER, INTEGER, BOOLEAN, NESTED, IS_NULL
from mo_logs import Log


def ToJsonType(value):
    if isinstance(value, JsonType):
        return value
    return _type_to_json_type[value]


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

    def tuple(self):
        return tuple(
            (k, v.tuple() if isinstance(v, JsonType) else v)
            for k, v in sorted(self.__dict__.items(), key=lambda p: p[0])
        )

    def __eq__(self, other):
        if not isinstance(other, JsonType):
            return False
        sd = self.__dict__
        od = other.__dict__
        for k, v in sd.items():
            if v != od[k]:
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


_new = object.__new__


def _primitive(name, value):
    output = _new(JsonType)
    setattr(output, name, value)
    return output


T_IS_NULL = _new(JsonType)
T_BOOLEAN = _primitive("b", BOOLEAN)
T_INTEGER = _primitive("i", INTEGER)
T_NUMBER = _primitive("n", NUMBER)
T_TIME = _primitive("t", TIME)
T_INTERVAL = _primitive("d", INTERVAL)
T_STRING = _primitive("s", STRING)
T_NESTED = _primitive("a", NESTED)

_type_to_json_type ={
    IS_NULL: T_IS_NULL,
    BOOLEAN: T_BOOLEAN,
    INTEGER: T_INTERVAL,
    NUMBER: T_NUMBER,
    TIME: T_TIME,
    INTERVAL: T_INTERVAL,
    STRING: T_STRING,
    NESTED: T_NESTED
}
