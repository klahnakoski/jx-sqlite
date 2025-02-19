import re

from mo_dots import is_missing, is_finite
from mo_logs import strings, logger

SNAP_TO_BASE_10 = False  # Identify floats near a round base10 value (has 000 or 999) and shorten




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
    return f'"{ESCAPE.sub(replace, s)}"'


def float2json(value):
    """
    CONVERT NUMBER TO JSON STRING, WITH BETTER CONTROL OVER ACCURACY
    :param value: float, int, long, Decimal
    :return: unicode
    """
    if is_missing(value):
        return "null"
    if not is_finite(value):
        return "null"

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
            return sign + digits[0] + "." + (digits[1:].rstrip("0") or "0") + "e" + str(int_exp)
        elif int_exp >= 0:
            return sign + (digits[: 1 + int_exp] + "." + digits[1 + int_exp :].rstrip("0")).rstrip(".")
        elif -4 < int_exp:
            digits = ("0" * (-int_exp)) + digits
            return sign + (digits[:1] + "." + digits[1:].rstrip("0")).rstrip(".")
        else:
            tail = digits[1:].rstrip("0")
            if not tail:
                return f"{sign}{digits[0]}e{int_exp}"
            else:
                return f"{sign}{digits[0]}.{tail}e{int_exp}"
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
            digits = str(int(digits[:f9]) + 1) + ("0" * (16 - f9))
        else:
            digits = digits[:f0] + ("0" * (16 - f0))
    return digits, 0

