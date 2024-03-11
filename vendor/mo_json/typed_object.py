# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from collections import OrderedDict

from mo_dots import is_many, is_data, exists, is_missing
from mo_dots.datas import register_data
from mo_logs import logger

from mo_json import python_type_to_jx_type_key, IS_PRIMITIVE_KEY, ARRAY_KEY, EXISTS_KEY, NUMBER_KEY


def entype(value):
    """
    MAKE SURE VALUE IS TYPED
    """
    if isinstance(value, TypedObject):
        return value
    else:
        return TypedObject(value)


def entype_array(values):
    return {ARRAY_KEY: values}


class TypedObject(OrderedDict):
    """
    LAZY BOX FOR TYPED OBJECTS
    """

    __slots__ = ["_attachments", "_boxed_value"]

    def __init__(self, value, **attachments):
        self._attachments = attachments
        if isinstance(value, TypedObject):
            logger.error("expecting plain object")
        self._boxed_value = value

    def __iter__(self):
        if is_many(self._boxed_value):
            yield from self._boxed_value
        else:
            yield self._boxed_value

    def __getitem__(self, item):
        if item in self._attachments:
            return self._attachments[item]
        if item == NUMBER_KEY:
            if isinstance(self._boxed_value, (int, float)):
                return self._boxed_value
            return None
        if item == ARRAY_KEY:
            if is_many(self._boxed_value):
                return [entype(v) for v in self._boxed_value]
            return None
        try:
            IS_PRIMITIVE_KEY.match(item)
        except Exception:
            logger.error("expecting primitive key not {item}", item=item.__class__.__name__)
        if IS_PRIMITIVE_KEY.match(item):
            expected = python_type_to_jx_type_key.get(type(self._boxed_value))
            if item == expected:
                return self._boxed_value
            return None
        if item == EXISTS_KEY:
            if is_many(self._boxed_value):
                return len(self._boxed_value)
            elif exists(self._boxed_value):
                return 1
            return 0
        if is_missing(self._boxed_value):
            return None
        else:
            try:
                return entype(self._boxed_value[item])
            except Exception:
                pass
            try:
                return entype(getattr(self._boxed_value, item))
            except Exception:
                pass
            return None

    def __setitem__(self, key, value):
        if isinstance(value, TypedObject):
            value = value._boxed_value
        self._attachments[key] = value

    def keys(self):
        if is_missing(self._boxed_value):
            return self._attachments.keys()
        if is_data(self._boxed_value):
            return self._boxed_value.keys() | self._attachments.keys()
        type_key = python_type_to_jx_type_key.get(type(self._boxed_value))
        return {type_key} | self._attachments.keys()

    def items(self):
        value = self._boxed_value
        if is_missing(value):
            return self._attachments.items()
        if is_data(value):
            return [
                *((k, TypedObject(v)) for k, v in value.items()),
                *((k, TypedObject(v)) for k, v in self._attachments.items()),
            ]
        if is_many(value):
            return [
                (ARRAY_KEY, [TypedObject(v) for v in value]),
                *((k, TypedObject(v)) for k, v in self._attachments.items()),
            ]
        type_key = python_type_to_jx_type_key.get(type(value))
        return [(type_key, value), *((k, TypedObject(v)) for k, v in self._attachments.items())]

    def __str__(self):
        return f"{self._boxed_value} ({self._attachments})"

    def __repr__(self):
        return f"{self._boxed_value} ({self._attachments})"


register_data(TypedObject)
