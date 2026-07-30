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
import os
import types
from unittest import SkipTest, TestCase

import mo_math
from mo_dots import (
    coalesce,
    is_list,
    literal_field,
    from_data,
    to_data,
    is_data,
    is_many,
    get_attr,
    is_missing,
    Null,
    is_null,
    is_finite,
)
from mo_future import is_text, zip_longest, first, get_function_name, generator_types
from mo_logs import Except, Log, suppress_exception
from mo_logs.strings import expand_template, quote
from mo_math import is_number, log10, COUNT
from mo_times import dates


os.environ.setdefault("TESTING", "1")


class FuzzyTestCase(TestCase):
    def __init__(self, *args, **kwargs):
        TestCase.__init__(self, *args, **kwargs)
        self.default_places = 15

    def set_default_places(self, places):
        """
        WHEN COMPARING float, HOW MANY DIGITS ARE SIGNIFICANT BY DEFAULT
        """
        self.default_places = places

    def matcher(self, *, digits=None, places=None, delta=None):
        """
        OVERRIDE TO COMPARE WITH DIFFERENT RULES
        """
        return FuzzyMatcher(digits=digits, places=places, delta=delta)

    def assert_almost_equal(self, test_value, expected, msg=None, *, digits=None, places=None, delta=None):
        if delta or digits:
            self.matcher(digits=digits, places=places, delta=delta).compare(test_value, expected, msg)
        else:
            self.matcher(
                digits=digits, places=coalesce(places, self.default_places), delta=delta
            ).compare(test_value, expected, msg)

    def assert_equal(self, test_value, expected, msg=None, *, digits=None, places=None, delta=None):
        if expected == None:
            expected = []
        self.assert_almost_equal(test_value, expected, msg=msg, digits=digits, places=places, delta=delta)

    def assert_raises(self, problem=None, function=None, *args, **kwargs):
        if function is None:
            return RaiseContext(self, problem=problem or Exception)

        with RaiseContext(self, problem=problem):
            function(*args, **kwargs)

    # BACKWARD COMPATIBILITY (ALSO WHAT unittest CALLS)
    assertAlmostEqual = assert_almost_equal
    assertEqual = assert_equal
    assertRaises = assert_raises


class RaiseContext:
    def __init__(self, testcase, problem=Exception):
        self.testcase = testcase
        self.problem = problem

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_val:
            Log.error("Expecting an error")
        f = Except.wrap(exc_val)

        if isinstance(self.problem, (list, tuple)):
            problems = self.problem
        else:
            problems = [self.problem]

        causes = []
        for problem in problems:
            if (
                isinstance(problem, object.__class__)
                and issubclass(problem, BaseException)
                and isinstance(exc_val, problem)
            ):
                return True
            try:
                self.testcase.assertIn(problem, f)
                return True
            except Exception as cause:
                causes.append(cause)
        Log.error("problem is not raised", cause=first(causes))


def assert_almost_equal(test, expected, *, digits=None, places=None, msg=None, delta=None):
    """
    COMPARE STRUCTURE AND NUMBERS

    STRUCTURE IS COMPARED BY ...
    * PROPERTIES IN THE expected STRUCTURE ARE TESTED TO EXIST IN test
    * PROPERTIES IN test THAT ARE NOT FOUND IN expected ARE IGNORED
    * IF expected IS A FUNCTION, THEN IT IS CALLED WITH test AS ARGUMENT
    * IF expected IS A SET, THEN ORDER DOES NOT MATTER
    * SINGLETON LIST MATCHES THE SINGLETON
    * IF expected IS AN EMPTY LIST, THEN test MUST BE MISSING (None, or empty list)
    * ELEMENTS OF test BEYOND THE END OF expected ARE IGNORED

    NUMBERS ARE MATCHED BY ...
    * places (UP TO GIVEN SIGNIFICANT DIGITS)
    * digits (UP TO GIVEN DECIMAL PLACES, WITH NEGATIVE MEANING LEFT-OF-UNITS)
    * delta (MAXIMUM ABSOLUTE DIFFERENCE FROM expected)
    """
    return FuzzyMatcher(digits=digits, places=places, delta=delta).compare(test, expected, msg)


assertAlmostEqual = assert_almost_equal  # BACKWARD COMPATIBILITY


class FuzzyMatcher:
    """
    THE RULES ARE DOCUMENTED ON assert_almost_equal

    SUBCLASS AND OVERRIDE compare_many() TO COMPARE SEQUENCES DIFFERENTLY
    """

    def __init__(self, *, digits=None, places=None, delta=None):
        self.digits = digits
        self.places = places
        self.delta = delta

    def compare(self, test, expected, msg=None):
        test = from_data(test)
        if isinstance(test, generator_types):
            Log.error("can not accept generators as test value")
        expected = from_data(expected)
        try:
            if test is expected:
                return
            elif is_null(expected):
                return
            elif is_missing(expected) and is_missing(test):
                return
            elif is_text(expected):
                self.compare_value(test, expected, msg=msg)
            elif is_null_op(expected) or is_list(expected) and len(expected) == 0:
                if is_missing(test):
                    return
                Log.error(
                    "{test|json|limit(10000)} is expected to not exist", test=test, expected=expected,
                )
            elif is_list(expected) and len(expected) == 1:
                return self.compare(test, expected[0], msg=msg)
        except Exception as cause:
            Log.error(
                "{test|json|limit(10000)} does not match expected {expected|json|limit(10000)}",
                test=test,
                expected=expected,
                cause=cause,
            )

        first_cause = None
        if is_list(test) and len(test) == 1 and is_many(test[0]) and is_many(expected):
            try:
                return self.compare(test[0], expected, msg=msg)
            except Exception as cause:
                first_cause = cause

        if is_many(test) and isinstance(expected, set):
            test = set(to_data(t) for t in test)
            if len(test) != len(expected):
                Log.error(
                    "Sets do not match, element count different:\n{test|json|indent}\nexpecting{expectedtest|json|indent}",
                    test=test,
                    expected=expected,
                )

            try:
                if len(test | expected) != len(test):
                    raise Exception()
            except:
                for e in expected:
                    for t in test:
                        try:
                            self.compare(t, e, msg=msg)
                            break
                        except Exception as _:
                            pass
                    else:
                        Log.error("Sets do not match. {value|json} not found in {test|json}", value=e, test=test)
            return  # ok

        if is_data(expected) and is_data(test):
            try:
                for k, e in from_data(expected).items():
                    if is_missing(k):
                        k = Null
                    t = test.get(k)
                    try:
                        self.compare(t, e, msg=coalesce(msg, "") + "key " + quote(k) + ": ")
                    except Exception as cause:
                        Log.error("key {k}={t} does not match expected {k}={e}", k=k, t=t, e=e, cause=cause)
                return
            except Exception as cause:
                first_cause = first_cause or cause

        if is_data(expected):
            try:
                if is_many(test):
                    test = list(test)
                    if len(test) != 1:
                        Log.error("Expecting data, not a list")
                    test = test[0]
                for k, e in expected.items():
                    t = get_attr(test, literal_field(k))
                    try:
                        self.compare(t, e, msg=msg)
                    except Exception as cause:
                        Log.error("key {k}={t} does not match expected {k}={e}", k=k, t=t, e=e, cause=cause)
                return
            except Exception as cause:
                first_cause = first_cause or cause

        if isinstance(expected, types.FunctionType):
            try:
                return expected(test)
            except Exception as cause:
                first_cause = first_cause or cause

        if is_many(test) and is_many(expected):
            try:
                return self.compare_many(test, expected, msg=msg)
            except Exception as cause:
                first_cause = first_cause or cause
        try:
            return self.compare_value(test, expected, msg=msg)
        except Exception as cause:
            first_cause = first_cause or cause

        Log.error(
            "{test|json|limit(10000)} does not match expected {expected|json|limit(10000)}",
            test=test,
            expected=expected,
            cause=first_cause,
        )

    def compare_many(self, test, expected, msg=None):
        """
        COMPARE TWO SEQUENCES, PAIRWISE
        """
        if test.__class__.__name__ == "ndarray":  # numpy
            test = test.tolist()
        elif test.__class__.__name__ == "DataFrame":  # pandas
            test = test[test.columns[0]].values.tolist()
        elif test.__class__.__name__ == "Series":  # pandas
            test = test.values.tolist()

        if not expected and test == None:
            return
        if expected == None:
            expected = []  # REPRESENT NOTHING
        for t, e in zip_longest(test, expected):
            self.compare(t, e, msg=msg)


    def compare_value(self, test, expected, msg=None):
        """
        Snagged from unittest/case.py, then modified (Aug2014)
        """
        digits, places, delta = self.digits, self.places, self.delta
        if test == expected:
            return
        if isinstance(expected, (dates.Date, datetime.datetime, datetime.date)):
            return self.compare_value(dates.Date(test).unix, dates.Date(expected).unix, msg=msg)
        if is_finite(test) and len(test) == 1:
            return self.compare(first(test), expected, msg=msg)
        if not is_number(expected):
            raise AssertionError(expand_template("{test|json} != {expected|json}", locals()))

        expected = float(expected)
        if not is_number(test):
            try:
                # ASSUME IT IS A UTC DATE
                test = dates.parse(test).unix
            except Exception as e:
                raise AssertionError(expand_template("{test|json} != {expected}", locals()))

        # WE NOW ASSUME test IS A NUMBER
        test = float(test)
        if test == expected:
            return

        if COUNT([digits, places, delta]) > 1:
            raise TypeError("specify only one of digits, places or delta")

        if digits is not None:
            with suppress_exception:
                diff = round(abs(test - expected) * pow(10, digits))
                if diff == 0:
                    return

            standardMsg = expand_template("{test|json} != {expected|json} within {digits} decimal places", locals())
        elif delta is not None:
            if abs(test - expected) <= delta:
                return

            standardMsg = expand_template("{test|json} != {expected|json} within {delta} delta", locals())
        else:
            if places is None:
                places = 15

            with suppress_exception:
                factor = mo_math.ceiling(log10(abs(test)))
                diff = log10(abs(test - expected)) - factor + places
                if diff < -0.3:
                    return

            standardMsg = expand_template("{test|json} != {expected|json} within {places} places", locals())

        raise AssertionError(coalesce(msg, "") + ": (" + standardMsg + ")")


def assert_almost_equal_value(test, expected, digits=None, places=None, msg=None, delta=None):
    """
    COMPARE NUMBERS (SEE assert_almost_equal FOR THE TOLERANCE RULES)
    """
    return FuzzyMatcher(digits=digits, places=places, delta=delta).compare_value(test, expected, msg)


assertAlmostEqualValue = assert_almost_equal_value  # BACKWARD COMPATIBILITY


def is_null_op(v):
    return v.__class__.__name__ == "NullOp"


def add_error_reporting(suite):
    """
    Both unittest and pytest have become sophisticated enough to hide
    the problems cause by a test failure. Making debugging difficult.
    This method ensures a detailed error message is logged
    :param suite: The TestCase class (as @decorator)
    """

    def add_handler(function):
        test_name = get_function_name(function)

        def error_hanlder(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except SkipTest as cause:
                raise cause
            except Exception as cause:
                Log.warning("{test_name} failed", cause=cause, test_name=test_name, static_template=False)
                raise cause

        return error_hanlder

    if not hasattr(suite, "FuzzyTestCase.__modified__"):
        setattr(suite, "FuzzyTestCase.__modified__", True)
        # find all methods, and wrap in exception handler
        for name, func in vars(suite).items():
            if name.startswith("test"):
                h = add_handler(func)
                h.__name__ = get_function_name(func)
                setattr(suite, name, h)
    return suite


class StructuredLogger_usingList:
    def __init__(self):
        self.lines = []

    def write(self, template, params):
        self.lines.append(expand_template(template, params))

    def stop(self):
        self.lines.append("logger stopped")
