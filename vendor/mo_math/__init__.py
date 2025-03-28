# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

import base64
from math import (
    pow as math_pow,
    exp as math_exp,
    log as math_log,
    isnan as math_isnan,
    ceil as math_ceil,
    log10 as math_log10,
    floor as math_floor,
)

from mo_dots import Null, coalesce, is_container, exists
from mo_future import round as math_round, __builtin__, binary_type
from mo_imports import delay_import

logger = delay_import("mo_logs.Log")
math_abs = __builtin__.abs
INFINITY = float("+inf")
LOG10_2 = math_log10(2)


"""
MATH FUNCTIONS THAT ASSUME None IMPLY *NOT APPLICABLE* RATHER THAN *MISSING*
LET "." BE SOME OPERATOR (+, -, *, etc)
a.None == None
None.a == None
.None == None
func(None, *kwargs)) == None
"""


def bayesian_add(*args):
    a = args[0]
    if a >= 1 or a <= 0:
        logger.error("Only allowed values *between* zero and one")

    for b in args[1:]:
        if b == None:
            continue
        if b >= 1 or b <= 0:
            logger.error("Only allowed values *between* zero and one")
        a = a * b / (a * b + (1 - a) * (1 - b))

    return a


def bayesian_subtract(a, b):
    return bayesian_add(a, 1 - b)


def abs(v):
    if v == None:
        return Null
    return math_abs(v)


def pow(n, p):
    if n == None or p == None:
        return None
    return math_pow(n, p)


def exp(v):
    if v == None:
        return Null
    return math_exp(v)


def log(v, base=None):
    try:
        if v == None:
            return Null
        if v == 0.0:
            return Null
        if base == None:
            return math_log(v)
        return math_log(v, base)
    except Exception as e:

        logger.error("error in log", cause=e)


def log10(v):
    try:
        return math_log(v, 10)
    except Exception as e:
        return Null


# FOR GOODNESS SAKE - IF YOU PROVIDE A METHOD abs(), PLEASE PROVIDE ITS COMPLEMENT
# x = abs(x)*sign(x)
# FOUND IN numpy, BUT WE USUALLY DO NOT NEED TO BRING IN A BIG LIB FOR A SIMPLE DECISION


def sign(v):
    if v == None:
        return Null
    if v < 0:
        return -1
    if v > 0:
        return +1
    return 0


def is_nan(s):
    return s == None or math_isnan(s)


def is_finite(s):
    try:
        f = float(s)
        if math_isnan(f) or math_abs(f) == INFINITY:
            return False
        return True
    except Exception:
        return False


def is_hex(value):
    try:
        int(value, 16)
        return True
    except Exception:
        return False


def is_integer(s):
    if s is True or s is False:
        return False

    try:
        if float(s) == math_round(float(s), 0):
            return True
        return False
    except Exception:
        return False


def to_integer(s):
    if s is True:
        return 1
    if s is False:
        return 0

    try:
        if float(s) == round(float(s), 0):
            return int(s)
        return Null
    except Exception:
        return Null


def to_float(s):
    if s is True:
        return 1.0
    if s is False:
        return 0.0

    try:
        return float(s)
    except Exception:
        return Null


def round(value, decimal=0, digits=None):
    """
    ROUND TO GIVEN NUMBER OF DIGITS, OR GIVEN NUMBER OF DECIMAL PLACES
    decimal - NUMBER OF DIGITS AFTER DECIMAL POINT (NEGATIVE IS VALID)
    digits - NUMBER OF SIGNIFICANT DIGITS (LESS THAN 1 IS INVALID)
    """
    if value == None:
        return None
    elif value == 0:
        return 0
    else:
        value = float(value)

    if digits != None:
        try:
            if digits <= 0:
                return sign(value) * pow(10, round(math_log10(abs(value)), 0))
            m = pow(10, math_ceil(math_log10(abs(value))))
            return math_round(value / m, digits) * m
        except Exception as e:

            logger.error("not expected", e)
    elif decimal <= 0:
        return int(math_round(value, decimal))
    else:
        return math_round(value, decimal)


def floor(value, mod=1):
    """
    x == floor(x, a) + mod(x, a)  FOR ALL a, x
    RETURN None WHEN GIVEN INVALID ARGUMENTS
    """
    if value == None:
        return None
    elif mod <= 0:
        return None
    elif mod == 1:
        return int(math_floor(value))
    elif is_integer(mod):
        return int(math_floor(value / mod)) * mod
    else:
        return math_floor(value / mod) * mod


def mod(value, mod=1):
    """
    RETURN NON-NEGATIVE MODULO
    RETURN None WHEN GIVEN INVALID ARGUMENTS
    """
    if value == None or mod <= 0:
        return None
    return value % mod


# RETURN A VALUE CLOSE TO value, BUT WITH SHORTER len(str(value))<len(str(value)):


def approx_str(value):
    v = str(value)
    d = v.find(".")
    if d == -1:
        return value

    if round(value) == value:
        return int(value)

    i = v.find("9999", d)
    if i == -1:
        i = v.find("0000", d)
        if i == -1:
            return value

    return round(value, decimal=i - d - 1)


def ceiling(value, mod=1):
    """
    RETURN SMALLEST INTEGER GREATER THAN value
    """
    if value == None:
        return None
    mod = int(mod)

    v = int(math_floor(value + mod))
    return v - (v % mod)


def min(*values):
    return MIN(values)


def range(start, stop, interval):
    i = start
    while i < stop:
        yield i
        i += interval


def max(*values):
    return MAX(values)


def COUNT(values):
    """
    DECISIVE COUNT
    :return: NUMBER OF NOT_MISSING VALUES
    """
    count = 0
    for v in values:
        if exists(v):
            count += 1
    return count


def MIN(values, *others):
    if others:
        logger.warning("Calling wrong")
        return MIN([values] + list(others))

    output = None
    for v in values:
        if v == None:
            continue
        elif output == None or v < output:
            output = v
        else:
            pass
    return output


def MAX(values, *others):
    """
    DECISIVE MAX
    :param values:
    :param others:
    :return:
    """

    if others:
        logger.warning("Calling wrong")
        return MAX([values] + list(others))

    output = Null
    for v in values:
        if v == None:
            continue
        elif output == None or v > output:
            output = v
        else:
            pass
    return output


def SUM(values):
    output = Null
    for v in values:
        if v == None:
            continue
        if isinstance(v, float) and math_isnan(v):
            continue
        if output == None:
            output = v
            continue
        output += v
    return output


def PRODUCT(values, *others):
    if len(others) > 0:
        logger.error("no longer accepting args, use a single list")

    output = Null
    for v in values:
        if v == None:
            continue
        if isinstance(v, float) and math_isnan(v):
            continue
        if output == None:
            output = v
            continue
        output *= v
    return output


def AND(values, *others):
    if len(others) > 0:
        logger.error("no longer accepting args, use a single list")

    for v in values:
        if v == None:
            continue
        if not v:
            return False
    return True


def OR(values, *others):
    if len(others) > 0:
        logger.error("no longer accepting args, use a single list")

    for v in values:
        if v == None:
            continue
        if v:
            return True
    return False


def UNION(values, *others):
    if len(others) > 0:
        logger.error("no longer accepting args, use a single list")

    output = set()
    for v in values:
        if v == None:
            continue
        if is_container(v):
            output.update(v)
            continue
        else:
            output.add(v)
    return output


def is_number(s):
    if s is True or s is False or s == None:
        return False

    try:
        s = float(s)
        return not math_isnan(s)
    except Exception:
        return False


def INTERSECT(values, *others):
    if len(others) > 0:
        logger.error("no longer accepting args, use a single list")

    output = set(values[0])
    for v in values[1:]:
        output -= set(v)
        if not output:
            return output  # EXIT EARLY
    return output


def almost_equal(first, second, digits=None, places=None, delta=None):
    try:
        if first == second:
            return True

        if delta is not None:
            if abs(first - second) <= delta:
                return True
            return False

        if first == 0:
            first, second = second, first

        places = coalesce(places, digits, 18)
        factor = ceiling(log10(abs(first)))
        diff = log10(abs(first - second))
        return diff < factor - places - LOG10_2
    except Exception as e:
        logger.error("problem comparing", cause=e)


def bytes2base64(value):
    if isinstance(value, bytearray):
        value = binary_type(value)
    return base64.b64encode(value).decode("latin1")


def bytes2base64URL(value):
    """
    RETURN URL-FRIENDLY VERSION OF BASE64
    """
    if isinstance(value, bytearray):
        value = binary_type(value)
    return base64.b64encode(value, b"-_").rstrip(b"=").decode("latin1")


def base642bytes(value):
    if value == None:
        return b""
    else:
        return base64.b64decode(value)


def int2base64(value):
    return bytes2base64(value.to_bytes((value.bit_length() + 7) // 8, byteorder="big"))


def base642int(value):
    return int.from_bytes(base642bytes(value), byteorder="big")
