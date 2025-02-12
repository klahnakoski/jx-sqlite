# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


import datetime
import re

from mo_dots import dict_to_data, register_primitive
from mo_imports import delay_import
from mo_math import MIN, is_nan, is_number, abs, floor, round

Date = delay_import("mo_times.Date")
logger = delay_import("mo_logs.logger")


class Duration(object):
    __slots__ = ["_milli", "month"]

    def __new__(cls, value=None, **kwargs):
        output = object.__new__(cls)
        if value == None:
            if kwargs:
                output.milli = datetime.timedelta(**kwargs).total_seconds() * 1000
                output.month = 0
                return output
            else:
                return None

        if isinstance(value, Duration):
            output.milli = value.milli
            output.month = value.month
            return output
        elif is_number(value):
            output._milli = float(value) * 1000
            output.month = 0
            return output
        elif isinstance(value, str):
            return parse(value)
        elif isinstance(value, float) and is_nan(value):
            return None
        else:
            logger.error("Do not know type of object ({value|json})of to make a Duration", value=value)

    @staticmethod
    def range(start, stop, step):
        if not step:
            logger.error("Expecting a non-zero duration for interval")
        output = []
        c = start
        while c < stop:
            output.append(c)
            c += step
        return output

    def __eq__(self, other):
        if other == None:
            return False
        other = Duration(other)
        return self.milli == other.milli and self.month == other.month

    def __hash__(self):
        return hash((self.milli, self.month))

    def __req__(self, other):
        other = Duration(other)
        return self.milli == other.milli and self.month == other.month

    def __add__(self, other):
        output = Duration(0)
        output.milli = self.milli + other.milli
        output.month = self.month + other.month
        return output

    def __radd__(self, other):
        if other == None:
            return None
        if isinstance(other, datetime.datetime):
            return Date(other).add(self)
        elif isinstance(other, Date):
            return other.add(self)
        return self + other

    def __mul__(self, amount):
        output = Duration(0)
        output.milli = self.milli * amount
        output.month = self.month * amount
        return output

    def __neg__(self):
        output = Duration(0)
        output.milli = -self.milli
        output.month = -self.month
        return output

    def __rmul__(self, amount):
        amount = float(amount)
        output = Duration(0)
        output.milli = self.milli * amount
        output.month = self.month * amount
        return output

    def __div__(self, amount):
        if isinstance(amount, Duration):
            if amount.month:
                m = self.month
                r = self.milli

                # DO NOT CONSIDER TIME OF DAY
                tod = r % MILLI_VALUES.day
                r = r - tod

                if m == 0 and r > (MILLI_VALUES.year / 3):
                    m = floor(12 * self.milli / MILLI_VALUES.year)
                    r -= (m / 12) * MILLI_VALUES.year
                else:
                    r = r - (self.month * MILLI_VALUES.month)
                    if r >= MILLI_VALUES.day * 31:
                        logger.error("Do not know how to handle")
                r = MIN([29 / 30, (r + tod) / (MILLI_VALUES.day * 30)])

                output = floor(m / amount.month) + r
                return output
            else:
                return self.milli / amount.milli
        elif is_number(amount):
            output = Duration(0)
            output.milli = self.milli / amount
            output.month = self.month / amount
            return output
        else:
            logger.error("Do not know how to divide by {type}", type=type(amount).__name__)

    def __truediv__(self, other):
        return self.__div__(Duration(other))

    def __rdiv__(self, other):
        return Duration(other).__div__(self)

    def __rtruediv__(self, other):
        return Duration(other).__div__(self)

    def __sub__(self, duration):
        output = Duration(0)
        output.milli = self.milli - duration.milli
        output.month = self.month - duration.month
        return output

    def __rsub__(self, time):
        if isinstance(time, Duration):
            output = Duration(0)
            output.milli = time.milli - self.milli
            output.month = time.month - self.month
            return output
        else:
            # ASSUME time CAN BE CONVERTED TO A Date
            return Date(time) - self

    def __lt__(self, other):
        if other == None:
            return False
        return self.milli < Duration(other).milli

    def __le__(self, other):
        if other == None:
            return False
        return self.milli <= Duration(other).milli

    def __ge__(self, other):
        if other == None:
            return True
        return self.milli >= Duration(other).milli

    def __gt__(self, other):
        if other == None:
            return True
        return self.milli > Duration(other).milli

    def ceiling(self, interval=None):
        if interval is None:
            interval = DAY
        return (self + interval).floor(interval) - interval

    def floor(self, interval=None):
        if not isinstance(interval, Duration):
            logger.error("Expecting an interval as a Duration object")

        output = Duration(0)
        if interval.month:
            if self.month:
                output.month = int(floor(self.month / interval.month) * interval.month)
                output.milli = output.month * MILLI_VALUES.month
                return output

            # A MONTH OF DURATION IS BIGGER THAN A CANONICAL MONTH
            output.month = int(floor(self.milli * 12 / MILLI_VALUES["year"] / interval.month) * interval.month)
            output.milli = output.month * MILLI_VALUES.month
        else:
            output.milli = floor(self.milli / (interval.milli)) * (interval.milli)
        return output

    @property
    def seconds(self):
        return self._milli / 1000

    @property
    def milli(self):
        return self._milli

    @milli.setter
    def milli(self, value):
        if not isinstance(value, float):
            logger.error("not allowed")
        self._milli = value

    def total_seconds(self):
        return float(self.milli) / 1000

    def __float__(self):
        return self.seconds

    def __int__(self):
        return int(self.seconds)

    def __str__(self):
        if not self.milli:
            return "zero"

        output = ""
        rest = self.milli - (MILLI_VALUES.month * self.month)  # DO NOT INCLUDE THE MONTH'S MILLIS
        isNegative = rest < 0
        rest = abs(rest)

        # MILLI
        rem = rest % 1000
        if rem != 0:
            output = f"+{rem}milli" + output
        rest = floor(rest / 1000)

        # SECOND
        rem = rest % 60
        if rem != 0:
            output = f"+{rem}second" + output
        rest = floor(rest / 60)

        # MINUTE
        rem = rest % 60
        if rem != 0:
            output = f"+{rem}minute" + output
        rest = floor(rest / 60)

        # HOUR
        rem = rest % 24
        if rem != 0:
            output = f"+{rem}hour" + output
        rest = floor(rest / 24)

        # DAY
        if (rest < 11 and rest != 7) or rest % 10 == 0:
            rem = rest
            rest = 0
        else:
            rem = rest % 7
            rest = floor(rest / 7)

        if rem != 0:
            output = f"+{rem}day" + output

        # WEEK
        if rest != 0:
            output = f"+{rest}week" + output

        if isNegative:
            output = output.replace("+", "-")

        # MONTH AND YEAR
        if self.month:
            sign = "-" if self.month < 0 else "+"
            month = abs(self.month)

            if month <= 18 and month != 12:
                output = f"{sign}{month}month" + output
            else:
                m = month % 12
                if m != 0:
                    output = f"{sign}{m}month" + output
                y = floor(month / 12)
                output = f"{sign}{y}year" + output

        if output[0] == "+":
            output = output[1::]
        if output[0] == "1" and not is_number(output[1]):
            output = output[1::]
        return output

    def format(self, interval, decimal):
        return self.round(Duration(interval), decimal) + interval

    def round(self, interval, decimal=0):
        output = self / interval
        output = round(output, decimal)
        return output


register_primitive(Duration)


def _string2Duration(text):
    """
    CONVERT SIMPLE <float><type> TO A DURATION OBJECT
    """
    if text == "" or text == "zero":
        return ZERO

    amount, interval = re.match(r"([\d.]*)(\w*)", text).groups()
    amount = int(amount) if amount else 1

    if interval not in MILLI_VALUES:
        logger.error(
            "{{interval|quote}} in {{text|quote}} is not a recognized duration type (did you use the pural form by"
            " mistake?",
            interval=interval,
            text=text,
        )

    output = Duration(0)
    if MONTH_VALUES[interval] == 0:
        output.milli = amount * MILLI_VALUES[interval]
    else:
        output.month = amount * MONTH_VALUES[interval]
        output.milli = output.month * MILLI_VALUES.month

    return output


def parse(value):
    output = Duration(0)

    # EXPECTING CONCAT OF <sign><integer><type>
    plist = value.split("+")
    for p, pplist in enumerate(plist):
        mlist = pplist.split("-")
        output = output + _string2Duration(mlist[0])
        for m in mlist[1::]:
            output = output - _string2Duration(m)
    return output


MILLI_VALUES = dict_to_data({
    "year": float(52 * 7 * 24 * 60 * 60 * 1000),  # 52weeks
    "quarter": float(13 * 7 * 24 * 60 * 60 * 1000),  # 13weeks
    "month": float(28 * 24 * 60 * 60 * 1000),  # 4weeks
    "week": float(7 * 24 * 60 * 60 * 1000),
    "day": float(24 * 60 * 60 * 1000),
    "hour": float(60 * 60 * 1000),
    "minute": float(60 * 1000),
    "second": float(1000),
    "milli": float(1),
    "zero": float(0),
})

MONTH_VALUES = dict_to_data({
    "year": 12,
    "quarter": 3,
    "month": 1,
    "week": 0,
    "day": 0,
    "hour": 0,
    "minute": 0,
    "second": 0,
    "milli": 0,
})

# A REAL MONTH IS LARGER THAN THE CANONICAL MONTH
MONTH_SKEW = MILLI_VALUES["year"] / 12 - MILLI_VALUES.month


def compare(a, b):
    return a.milli - b.milli


DOMAIN = {"type": "duration", "compare": compare}

ZERO = Duration(0)
SECOND = Duration("second")
MINUTE = Duration("minute")
HOUR = Duration("hour")
DAY = Duration("day")
WEEK = Duration("week")
MONTH = Duration("month")
QUARTER = Duration("quarter")
YEAR = Duration("year")

COMMON_INTERVALS = [
    Duration("second"),
    Duration("15second"),
    Duration("30second"),
    Duration("minute"),
    Duration("5minute"),
    Duration("15minute"),
    Duration("30minute"),
    Duration("hour"),
    Duration("2hour"),
    Duration("3hour"),
    Duration("6hour"),
    Duration("12hour"),
    Duration("day"),
    Duration("2day"),
    Duration("week"),
    Duration("2week"),
    Duration("month"),
    Duration("2month"),
    Duration("quarter"),
    Duration("6month"),
    Duration("year"),
]
