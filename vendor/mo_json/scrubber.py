import json
import math
from datetime import timedelta, timezone

from mo_dots import null_types, from_data, exists
from mo_dots import datas, lists
from mo_dots.objects import DataObject
from mo_future import integer_types, is_text
from mo_imports import delay_import
from mo_logs import Except
from mo_times import Duration

from mo_json.types import *

FIND_LOOPS = True  # FIND LOOPS IN DATA STRUCTURES
DATETIME_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
DATE_EPOCH = date(1970, 1, 1)


logger = delay_import("mo_logs.logger")
json_decoder = json.JSONDecoder().decode
_get = object.__getattribute__


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


class Scrubber:
    def __init__(self, scrub_text=_keep_whitespace, scrub_number=_scrub_number):
        self.scrub_text = lambda value, is_done, stack: scrub_text(value)
        self.scrub_number = lambda value, is_done, stack: scrub_number(value)

        self.scrubbers = {
            **{t: lambda value, is_done, stack: None for t in null_types},
            str: self.scrub_text,
            float: lambda value, is_done, stack: None
            if math.isnan(value) or math.isinf(value)
            else scrub_number(value),
            **{t: self.scrub_number for t in integer_types},
            **{t: self._scrub_many for t in lists.many_types},
            **{t: self._scrub_data for t in datas.data_types},
            bool: lambda value, is_done, stack: value,
            date: lambda value, is_done, stack: scrub_number(datetime2unix(value)),
            datetime: lambda value, is_done, stack: scrub_number(datetime2unix(value)),
            timedelta: lambda value, is_done, stack: scrub_number(value.total_seconds()),
            Date: lambda value, is_done, stack: scrub_number(value.unix),
            Duration: lambda value, is_done, stack: scrub_number(value.seconds),
            bytes: lambda value, is_done, stack: value.decode("latin1"),
            Decimal: lambda value, is_done, stack: scrub_number(value),
            type: lambda value, is_done, stack: value.__name__,
        }
        self.more_scrubbers = [
            # (TEST, SCRUB) PAIRS
            (
                lambda value: hasattr(value, "__json__"),
                lambda value, is_done, stack: self._scrub_json(value, is_done, stack),
            ),
            (
                lambda value: hasattr(value, "__data__"),
                lambda value, is_done, stack: self._scrub(value.__data__(), is_done, stack),
            ),
            (
                lambda value: isinstance(value, Exception),
                lambda value, is_done, stack: self._scrub(Except.wrap(value), is_done, stack),
            ),
            (is_number, scrub_number),
            (
                lambda value: value.__class__.__name__ == "bool_",
                lambda value, is_done, stack: False if value == False else True,
            ),
            (
                lambda value: hasattr(value, "co_code") or hasattr(value, "f_locals"),
                lambda value, is_done, stack: None,
            ),
            (lambda value: hasattr(value, "__call__"), lambda value, is_done, stack: str(repr(value))),
        ]

    def scrub(self, value):
        """
        REMOVE/REPLACE VALUES THAT CAN NOT BE JSON-IZED
        """
        return self._scrub(value, set(), [])

    def _scrub(self, value, is_done, stack):
        while isinstance(value, DataObject):
            value = from_data(value)

        if FIND_LOOPS:
            _id = id(value)
            if _id in stack and type(_id).__name__ not in ["int"]:
                logger.error("loop in JSON")
            stack = stack + [_id]

        type_ = value.__class__
        scrubber = self.scrubbers.get(type_)
        if scrubber:
            return scrubber(value, is_done, stack)

        for test, scrub in self.more_scrubbers:
            try:
                if test(value):
                    return scrub(value, is_done, stack)
            except Exception:
                pass
        # FINALLY, WRAP IN OBJECT AND ATTEMPT TO SERIALIZE
        return self._scrub_data(DataObject(value), is_done, stack)

    def _scrub_data(self, value, is_done, stack):
        """
        REMOVE/REPLACE VALUES THAT CAN NOT BE JSON-IZED
        """
        output = {}
        for k, v in value.items():
            if not isinstance(k, str):
                logger.error("keys must be strings")
            v = self._scrub(v, is_done, stack)
            if isinstance(v, list) or exists(v):
                output[k] = v
        return output

    def _scrub_many(self, value, is_done, stack):
        output = []
        for v in value:
            v = self._scrub(v, is_done, stack)
            output.append(v)
        return output  # if output else None

    def _scrub_json(self, value, is_done, stack):
        try:
            j = value.__json__()
            if is_text(j):
                data = json_decoder(j)
            else:
                data = json_decoder("".join(j))
            return self._scrub(data, is_done, stack)
        except Exception as cause:
            logger.error("problem with calling __json__()", cause)
