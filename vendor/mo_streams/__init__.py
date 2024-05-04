# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from mo_dots import Data, is_many
from mo_dots.utils import is_finite
from mo_files import File
from mo_future import first
from mo_imports import export

from mo_json import JxType, JX_TEXT
from mo_streams._utils import Stream, Reader
from mo_streams.byte_stream import ByteStream
from mo_streams.empty_stream import EmptyStream
from mo_streams.files import content, File_usingStream
from mo_streams.function_factory import it
from mo_streams.object_stream import ObjectStream, ERROR, WARNING, NONE
from mo_streams.string_stream import StringStream
from mo_streams.type_utils import Typer, CallableTyper, StreamTyper, LazyTyper


def stream(value):
    if isinstance(value, (dict, Data)):
        kv = first(value.items())
        if not kv:
            return EmptyStream()
        _, example = kv
        return ObjectStream(((v, {"key": k}) for k, v in value.items()), Typer(example=example), JxType(key=JX_TEXT),)
    elif isinstance(value, bytes):
        return ByteStream(Reader(iter([value])))
    elif isinstance(value, str):
        return StringStream(iter([value]))
    elif value == None:
        return EmptyStream
    elif isinstance(value, Stream):
        return value
    elif isinstance(value, type(range(1))):
        return ObjectStream(((v, {}) for v in value), Typer(example=value.stop), JxType())
    elif is_finite(value):
        example = first(value)

        def read_from_list():
            for v in value:
                yield v, {}

        return ObjectStream(read_from_list(), Typer(example=example), JxType())
    elif is_many(value):
        example = first(value)

        def read():
            yield example, {}
            for v in value:
                yield v, {}

        return ObjectStream(read(), Typer(example=example), JxType())
    else:
        return ObjectStream(iter([(value, {})]), Typer(example=value), JxType())


def from_s3(bucket, key):
    import boto3
    return S3Object(boto3.resource('s3').Object(bucket, key))


class S3Object:
    def __init__(self, obj):
        self.obj = obj

    def content(self) -> ByteStream:
        return ByteStream(self.obj.get()['Body'])


STR_CALL = CallableTyper(return_type=str)
BOOL_CALL = CallableTyper(return_type=bool)
INT_CALL = CallableTyper(return_type=int)
BYTES_CALL = CallableTyper(return_type=bytes)
STR = Typer(python_type=str)


ANNOTATIONS = {
    # THE str METHODS DO NOT APPEAR TO BE IN THE str CLASS ANNOTATIONS
    (str, "encode"): BYTES_CALL,
    (str, "startswith"): BOOL_CALL,
    (str, "__repr__"): STR_CALL,
    (str, "__str__"): STR_CALL,
    (str, "__lt__"): BOOL_CALL,
    (str, "__le__"): BOOL_CALL,
    (str, "__eq__"): BOOL_CALL,
    (str, "__ne__"): BOOL_CALL,
    (str, "__gt__"): BOOL_CALL,
    (str, "__ge__"): BOOL_CALL,
    (str, "__len__"): INT_CALL,
    (str, "__add__"): STR_CALL,
    (str, "__contains__"): BOOL_CALL,
    (str, "encode"): BYTES_CALL,
    (str, "replace"): STR_CALL,
    (str, "split"): CallableTyper(return_type=StreamTyper(member_type=STR, _schema=JX_TEXT)),
    (str, "rsplit"): CallableTyper(return_type=StreamTyper(member_type=STR, _schema=JX_TEXT)),
    (str, "join"): STR_CALL,
    (str, "capitalize"): STR_CALL,
    (str, "casefold"): STR_CALL,
    (str, "title"): STR_CALL,
    (str, "center"): STR_CALL,
    (str, "count"): INT_CALL,
    (str, "expandtabs"): STR_CALL,
    (str, "find"): INT_CALL,
    (str, "partition"): BOOL_CALL,
    (str, "index"): INT_CALL,
    (str, "ljust"): STR_CALL,
    (str, "lower"): STR_CALL,
    (str, "lstrip"): STR_CALL,
    (str, "rfind"): INT_CALL,
    (str, "rindex"): INT_CALL,
    (str, "rjust"): BOOL_CALL,
    (str, "rstrip"): BOOL_CALL,
    (str, "rpartition"): BOOL_CALL,
    (str, "splitlines"): BOOL_CALL,
    (str, "strip"): STR_CALL,
    (str, "swapcase"): STR_CALL,
    (str, "translate"): STR_CALL,
    (str, "upper"): STR_CALL,
    (str, "startswith"): BOOL_CALL,
    (str, "endswith"): BOOL_CALL,
    (str, "removeprefix"): STR_CALL,
    (str, "removesuffix"): STR_CALL,
    (str, "isascii"): BOOL_CALL,
    (str, "islower"): BOOL_CALL,
    (str, "isupper"): BOOL_CALL,
    (str, "istitle"): BOOL_CALL,
    (str, "isspace"): BOOL_CALL,
    (str, "isdecimal"): BOOL_CALL,
    (str, "isdigit"): BOOL_CALL,
    (str, "isnumeric"): BOOL_CALL,
    (str, "isalpha"): BOOL_CALL,
    (str, "isalnum"): BOOL_CALL,
    (str, "isidentifier"): BOOL_CALL,
    (str, "isprintable"): BOOL_CALL,
    (File_usingStream, "content"): CallableTyper(return_type=ByteStream),
    (File, "content"): CallableTyper(return_type=ByteStream),
    (ByteStream, "utf8"): CallableTyper(return_type=StringStream),
    (StringStream, "lines"): CallableTyper(return_type=StreamTyper(member_type=STR, _schema=JxType())),
    (ByteStream, "lines"): CallableTyper(return_type=StreamTyper(member_type=STR, _schema=JxType())),
    (ObjectStream, "map"): CallableTyper(return_type=StreamTyper(member_type=LazyTyper(), _schema=JxType())),
}

export("mo_streams.object_stream", stream)
export("mo_streams.type_utils", ANNOTATIONS)
export("mo_streams.function_factory", ANNOTATIONS)
