# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from mo_dots import Null, to_data, leaves_to_data, is_list
from mo_imports import delay_import
from mo_logs import Except
from mo_logs.strings import toString, FORMATTERS
from mo_times import Timer

from mo_json.scrubber import Scrubber, _keep_whitespace, trim_whitespace
from mo_json.typed_encoder import detype
from mo_json.typed_object import entype
from mo_json.types import *
from mo_json.utils import *

__all__ = [
    "ARRAY",
    "ARRAY_KEY",
    "BOOLEAN",
    "EXISTS",
    "INTERVAL",
    "IS_NULL",
    "JX_ANY",
    "JX_ARRAY",
    "JX_BOOLEAN",
    "JX_INTEGER",
    "JX_INTERVAL",
    "JX_IS_NULL",
    "JX_NUMBER",
    "JX_TEXT",
    "JX_TIME",
    "NUMBER",
    "OBJECT",
    "STRING",
    "TIME",
    "INTEGER",
    "detype",
    "entype",
    "jx_type_to_json_type",
    "json2value",
    "python_type_to_jx_type",
    "python_type_to_json_type",
    "quote",
    "to_jx_type",
    "value2json",
    "value_to_jx_type",
]

logger = delay_import("mo_logs.logger")
hjson2value = delay_import("hjson.loads")

FIND_LOOPS = True  # FIND LOOPS IN DATA STRUCTURES
CAN_NOT_DECODE_JSON = "Can not decode JSON"

true, false, null = True, False, None

_get = object.__getattribute__


def scrub(value, keep_whitespace=True):
    return Scrubber(scrub_text=_keep_whitespace if keep_whitespace else trim_whitespace).scrub(value)


def value2json(obj, pretty=False, sort_keys=False, keep_whitespace=True):
    """
    :param obj:  THE VALUE TO TURN INTO JSON
    :param pretty: True TO MAKE A MULTI-LINE PRETTY VERSION
    :param sort_keys: True TO SORT KEYS
    :param keep_whitespace: False TO strip() THE WHITESPACE IN THE VALUES
    :return:
    """
    with Timer("scrub", too_long=0.1):
        obj = Scrubber(scrub_text=_keep_whitespace if keep_whitespace else trim_whitespace).scrub(obj)
    try:
        json = json_encoder(obj, pretty=pretty)
        if json == None:
            logger.note(
                str(type(obj)) + " is not valid{{type}}JSON", type=" (pretty) " if pretty else " ",
            )
            logger.error("Not valid JSON: " + str(obj) + " of type " + str(type(obj)))
        return json
    except Exception as e:
        e = Except.wrap(e)
        try:
            json = pypy_json_encode(obj)
            return json
        except Exception:
            pass
        logger.error("Can not encode into JSON: {value}", value=str(repr(obj)), cause=e)


def remove_line_comment(line):
    mode = 0  # 0=code, 1=inside_string, 2=escaping
    for i, c in enumerate(line):
        if c == '"':
            if mode == 0:
                mode = 1
            elif mode == 1:
                mode = 0
            else:
                mode = 1
        elif c == "\\":
            if mode == 0:
                mode = 0
            elif mode == 1:
                mode = 2
            else:
                mode = 1
        elif mode == 2:
            mode = 1
        elif c == "#" and mode == 0:
            return line[0:i]
        elif c == "/" and mode == 0 and line[i + 1] == "/":
            return line[0:i]
    return line


def check_depth(json, limit=30):
    """
    THROW ERROR IF JSON IS TOO DEEP
    :param json:  THE JSON STRING TO CHECK
    :param limit:  EXIST EARLY IF TOO DEEP
    """
    l = len(json)
    expecting = ["{"] * limit
    e = -1
    i = 0
    while i < l:
        c = json[i]
        if c == '"':
            i += 1
            while True:
                c = json[i]
                if c == "\\" and json[i + 1] == '"':
                    i += 2
                    continue
                i += 1
                if c == '"':
                    break
        elif c == "{":
            e += 1
            expecting[e] = "}"
            i += 1
        elif c == "[":
            e += 1
            expecting[e] = "]"
            i += 1
        elif c in "]}":
            if expecting[e] == c:
                e -= 1
            else:
                logger.error("invalid JSON")
            i += 1
        else:
            i += 1


def json2value(json_string, params=Null, flexible=False, leaves=False):
    """
    :param json_string: THE JSON
    :param params: STANDARD JSON PARAMS
    :param flexible: REMOVE COMMENTS (uses hjson)
    :param leaves: ASSUME JSON KEYS ARE DOT-DELIMITED
    :return: Python value
    """
    if not isinstance(json_string, str) and json_string.__class__.__name__ != "FileString":
        logger.error("only unicode json accepted")

    try:
        if len(params):
            # LOOKUP REFERENCES
            json_string = _simple_expand(json_string, (params,))

        if flexible:
            value = to_data(hjson2value(json_string))
        else:
            value = to_data(json_decoder(str(json_string)))

        if leaves:
            value = leaves_to_data(value)

        return value

    except Exception as e:
        e = Except.wrap(e)

        if not json_string.strip():
            logger.error("JSON string is only whitespace")

        c = e
        while c.cause and "Expecting '" in c.cause and "' delimiter: line" in c.cause:
            c = c.cause

        if "Expecting '" in c and "' delimiter: line" in c:
            line_index = int(strings.between(c.message, " line ", " column ")) - 1
            column = int(strings.between(c.message, " column ", " ")) - 1
            line = json_string.split("\n")[line_index].replace("\t", " ")
            if column > 20:
                sample = "..." + line[column - 20:]
                pointer = "   " + (" " * 20) + "^"
            else:
                sample = line
                pointer = (" " * column) + "^"

            if len(sample) > 43:
                sample = sample[:43] + "..."

            logger.error(
                CAN_NOT_DECODE_JSON + " at:\n\t{{sample}}\n\t{{pointer}}\n", sample=sample, pointer=pointer,
            )

        base_str = strings.limit(json_string, 1000).encode("utf8")
        hexx_str = bytes2hex(base_str, " ")
        try:
            char_str = " " + "  ".join((chr(c) if c >= 32 else ".") for c in base_str)
        except Exception as cause:
            char_str = " "
        logger.error(
            CAN_NOT_DECODE_JSON + ":\n{{char_str}}\n{{hexx_str}}\n", char_str=char_str, hexx_str=hexx_str, cause=e,
        )


def bytes2hex(value, separator=" "):
    return separator.join(f"{x:02X}" for x in value)


_variable_pattern = re.compile(r"\{\{([\w_\.]+(\|[^\}^\|]+)*)\}\}")


def _simple_expand(template, seq):
    """
    seq IS TUPLE OF OBJECTS IN PATH ORDER INTO THE DATA TREE
    seq[-1] IS THE CURRENT CONTEXT
    """

    def replacer(found):
        ops = found.group(1).split("|")

        path = ops[0]
        var = path.lstrip(".")
        depth = min(len(seq), max(1, len(path) - len(var)))
        try:
            val = seq[-depth]
            if var:
                if is_sequence(val) and float(var) == round(float(var), 0):
                    val = val[int(var)]
                else:
                    val = val[var]
            for func_name in ops[1:]:
                parts = func_name.split("(")
                if len(parts) > 1:
                    val = eval(parts[0] + "(val, " + "(".join(parts[1::]))
                else:
                    val = FORMATTERS[func_name](val)
            val = toString(val)
            return val
        except Exception as cause:
            from mo_logs import Except

            cause = Except.wrap(cause)
            try:
                if cause.message.find("is not JSON serializable"):
                    # WORK HARDER
                    val = toString(val)
                    return val
            except Exception as f:
                logger.warning(
                    "Can not expand " + "|".join(ops) + " in template: {{template_|json}}",
                    template_=template,
                    cause=cause,
                )
            return "[template expansion error: (" + str(cause.message) + ")]"

    return _variable_pattern.sub(replacer, template)


def get_if_type(value, json_type):
    """
    RETURN value IF IT IS THE CORRECT TYPE, OTHERWISE None
    """
    if is_json_type(value, json_type):
        if json_type == "object":
            return "."
        if isinstance(value, Date):
            return value.unix
        return value
    return None


def is_json_type(value, json_type):
    """
    RETURN IF  value IF OF THE RIGHT TYPE
    """
    if value == None:
        return False
    elif isinstance(value, str) and json_type == "string":
        return value
    elif is_list(value):
        return False
    elif is_data(value) and json_type == "object":
        return True
    elif isinstance(value, (int, float, Date)) and json_type == "number":
        return True
    return False


from mo_json.decoder import json_decoder
from mo_json.encoder import json_encoder, pypy_json_encode
