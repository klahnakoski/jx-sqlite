# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import math
from datetime import timedelta, timezone

from hjson import loads as hjson2value

from mo_dots import (
    Data,
    FlatList,
    Null,
    SLOT,
    to_data,
    leaves_to_data,
    null_types,
)
from mo_dots.objects import DataObject
from mo_future import (
    integer_types,
    is_binary,
    is_text,
)
from mo_imports import delay_import
from mo_json.types import *
from mo_logs import Except, strings
from mo_logs.strings import expand_template, toString, FORMATTERS
from mo_times import Duration

logger = delay_import("mo_logs.logger")

FIND_LOOPS = True  # FIND LOOPS IN DATA STRUCTURES
SNAP_TO_BASE_10 = False  # Identify floats near a round base10 value (has 000 or 999) and shorten
CAN_NOT_DECODE_JSON = "Can not decode JSON"


true, false, null = True, False, None

_get = object.__getattribute__


ESCAPE_DCT = {
    "\\": "\\\\",
    '"': '\\"',
    "\b": "\\b",
    "\f": "\\f",
    "\n": "\\n",
    "\r": "\\r",
    "\t": "\\t",
}
for i in range(0x20):
    ESCAPE_DCT.setdefault(chr(i), "\\u{0:04x}".format(i))

ESCAPE = re.compile(r'[\x00-\x1f\\"\b\f\n\r\t]')


def replace(match):
    return ESCAPE_DCT[match.group(0)]


def quote(s):
    return '"' + ESCAPE.sub(replace, s) + '"'


def float2json(value):
    """
    CONVERT NUMBER TO JSON STRING, WITH BETTER CONTROL OVER ACCURACY
    :param value: float, int, long, Decimal
    :return: unicode
    """
    if value == 0:
        return "0"
    try:
        sign = "-" if value < 0 else ""
        value = abs(value)
        sci = value.__format__(".15e")
        mantissa, str_exp = sci.split("e")
        digits, more_digits = _snap_to_base_10(mantissa)
        int_exp = int(str_exp) + more_digits
        if int_exp > 15:
            return sign + digits[0] + "." + (digits[1:].rstrip("0") or "0") + "e" + text(int_exp)
        elif int_exp >= 0:
            return sign + (digits[: 1 + int_exp] + "." + digits[1 + int_exp :].rstrip("0")).rstrip(".")
        elif -4 < int_exp:
            digits = ("0" * (-int_exp)) + digits
            return sign + (digits[:1] + "." + digits[1:].rstrip("0")).rstrip(".")
        else:
            return sign + digits[0] + "." + (digits[1:].rstrip("0") or "0") + "e" + text(int_exp)
    except Exception as e:
        logger.error("not expected", e)


def _snap_to_base_10(mantissa):
    # TODO: https://lists.nongnu.org/archive/html/gcl-devel/2012-10/pdfkieTlklRzN.pdf
    digits = mantissa.replace(".", "")
    if SNAP_TO_BASE_10:
        f9 = strings.find(digits, "999")
        f0 = strings.find(digits, "000")
        if f9 == 0:
            return "1000000000000000", 1
        elif f9 < f0:
            digits = text(int(digits[:f9]) + 1) + ("0" * (16 - f9))
        else:
            digits = digits[:f0] + ("0" * (16 - f0))
    return digits, 0


def _scrub_number(value):
    d = float(value)
    i_d = int(d)
    if float(i_d) == d:
        return i_d
    else:
        return d


def _keep_whitespace(value):
    if value.strip():
        return value
    else:
        return None


def trim_whitespace(value):
    value_ = value.strip()
    if value_:
        return value_
    else:
        return None


def is_number(s):
    try:
        s = float(s)
        return not math.isnan(s)
    except Exception:
        return False


def scrub(value, scrub_text=_keep_whitespace, scrub_number=_scrub_number):
    """
    REMOVE/REPLACE VALUES THAT CAN NOT BE JSON-IZED
    """
    return _scrub(value, set(), [], scrub_text=scrub_text, scrub_number=scrub_number)


def _scrub(value, is_done, stack, scrub_text, scrub_number):
    if FIND_LOOPS:
        _id = id(value)
        if _id in stack and type(_id).__name__ not in ["int"]:
            logger.error("loop in JSON")
        stack = stack + [_id]
    type_ = value.__class__

    if type_ in null_types:
        return None
    elif type_ is text:
        return scrub_text(value)
    elif type_ is float:
        if math.isnan(value) or math.isinf(value):
            return None
        return scrub_number(value)
    elif type_ is bool:
        return value
    elif type_ in integer_types:
        return scrub_number(value)
    elif type_ in (date, datetime):
        return scrub_number(datetime2unix(value))
    elif type_ is timedelta:
        return value.total_seconds()
    elif type_ is Date:
        return scrub_number(value.unix)
    elif type_ is Duration:
        return scrub_number(value.seconds)
    elif type_ is str:
        return value.decode("utf8")
    elif type_ is Decimal:
        return scrub_number(value)
    elif type_ is Data:
        return _scrub(_get(value, SLOT), is_done, stack, scrub_text, scrub_number)
    elif is_data(value):
        _id = id(value)
        if _id in is_done:
            # logger.warning("possible loop in structure detected")
            return '"<LOOP IN STRUCTURE>"'
        is_done.add(_id)
        try:
            output = {}
            for k, v in value.items():
                if is_text(k):
                    pass
                elif is_binary(k):
                    k = k.decode("utf8")
                else:
                    logger.error("keys must be strings")
                v = _scrub(v, is_done, stack, scrub_text, scrub_number)
                if v != None or is_data(v):
                    output[k] = v
        finally:
            is_done.discard(_id)
        return output
    elif type_ in (tuple, list, FlatList):
        output = []
        for v in value:
            v = _scrub(v, is_done, stack, scrub_text, scrub_number)
            output.append(v)
        return output  # if output else None
    elif type_ is type:
        return value.__name__
    elif type_.__name__ == "bool_":  # DEAR ME!  Numpy has it's own booleans (value==False could be used, but 0==False in Python.  DOH!)
        if value == False:
            return False
        else:
            return True
    elif not isinstance(value, Except) and isinstance(value, Exception):
        return _scrub(Except.wrap(value), is_done, stack, scrub_text, scrub_number)
    elif hasattr(value, "__json__"):
        try:
            j = value.__json__()
            if is_text(j):
                data = json_decoder(j)
            else:
                data = json_decoder("".join(j))
            return _scrub(data, is_done, stack, scrub_text, scrub_number)
        except Exception as cause:
            logger.error("problem with calling __json__()", cause)
    elif hasattr(value, "__data__"):
        try:
            return _scrub(value.__data__(), is_done, stack, scrub_text, scrub_number)
        except Exception as cause:
            logger.error("problem with calling __data__()", cause)
    elif hasattr(value, "co_code") or hasattr(value, "f_locals"):
        return None
    elif hasattr(value, "__iter__"):
        output = []
        for v in value:
            v = _scrub(v, is_done, stack, scrub_text, scrub_number)
            output.append(v)
        return output
    elif hasattr(value, "__call__"):
        return text(repr(value))
    elif is_number(value):
        # for numpy values
        return scrub_number(value)
    else:
        return _scrub(DataObject(value), is_done, stack, scrub_text, scrub_number)


def value2json(obj, pretty=False, sort_keys=False, keep_whitespace=True):
    """
    :param obj:  THE VALUE TO TURN INTO JSON
    :param pretty: True TO MAKE A MULTI-LINE PRETTY VERSION
    :param sort_keys: True TO SORT KEYS
    :param keep_whitespace: False TO strip() THE WHITESPACE IN THE VALUES
    :return:
    """
    if FIND_LOOPS:
        obj = scrub(obj, scrub_text=_keep_whitespace if keep_whitespace else trim_whitespace)
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
        logger.error("Can not encode into JSON: {{value}}", value=text(repr(obj)), cause=e)


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
    :param flexible: REMOVE COMMENTS
    :param leaves: ASSUME JSON KEYS ARE DOT-DELIMITED
    :return: Python value
    """
    json_string = text(json_string)
    if not is_text(json_string) and json_string.__class__.__name__ != "FileString":
        logger.error("only unicode json accepted")

    try:
        if len(params):
            # LOOKUP REFERENCES
            json_string = _simple_expand(json_string, (params, ))

        if flexible:
            value = hjson2value(json_string)
        else:
            value = to_data(json_decoder(text(json_string)))

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
                sample = "..." + line[column - 20 :]
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
    return separator.join("{:02X}".format(x) for x in value)


DATETIME_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

DATE_EPOCH = date(1970, 1, 1)


def datetime2unix(value):
    try:
        if value == None:
            return None
        elif isinstance(value, datetime):
            if value.tzinfo:
                diff = value - DATETIME_EPOCH
            else:
                diff = value - DATETIME_EPOCH.replace(tzinfo=None)
            return diff.total_seconds()
        elif isinstance(value, date):
            diff = value - DATE_EPOCH
            return diff.total_seconds()
        else:
            logger.error(
                "Can not convert {{value}} of type {{type}}", value=value, type=value.__class__,
            )
    except Exception as e:
        logger.error("Can not convert {{value}}", value=value, cause=e)


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
                Log.warning(
                    "Can not expand " + "|".join(ops) + " in template: {{template_|json}}",
                    template_=template,
                    cause=cause,
                )
            return "[template expansion error: (" + str(cause.message) + ")]"

    return _variable_pattern.sub(replacer, template)


from mo_json.decoder import json_decoder
from mo_json.encoder import json_encoder, pypy_json_encode
