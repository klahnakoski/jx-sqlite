# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from datetime import timedelta
from time import time

from mo_dots import coalesce, to_data
from mo_imports import delay_import

from mo_times.durations import Duration

logger = delay_import("mo_logs.logger")

START = time()


class Timer:
    """
    USAGE:
    with Timer("doing hard time"):
        something_that_takes_long()
    OUTPUT:
        doing hard time took 45.468 sec
    """

    def __init__(
        self,
        description,  # A DESCRIPTION
        param=None,  # description CAN HAVE PARAMETERS, PUT THEM HERE
        silent=None,  # DO NOT LOG
        verbose=None,  # PLEASE LOG
        too_long=0,  # ONLY LOG IF MORE THAN THIS NUMBER OF SECONDS
    ):
        self.template = description
        self.param = to_data(coalesce(param, {}))
        self.verbose = coalesce(verbose, False if silent is True else too_long == 0)
        self.agg = 0
        self.too_long = too_long  # ONLY SHOW TIMING FOR DURATIONS THAT ARE too_long
        self.start = 0
        self.end = 0
        self.interval = None

    def __enter__(self):
        if self.verbose:
            logger.note(
                "Timer start: " + self.template, default_params=self.param, stack_depth=1, static_template=False
            )
        self.start = time()
        return self

    def __exit__(self, type, value, traceback):
        self.end = time()
        self.interval = self.end - self.start
        self.agg += self.interval
        self.param.duration = timedelta(seconds=self.interval)
        if self.verbose:
            if self.too_long == 0:
                logger.note(
                    "Timer end  : " + self.template + " (took {{duration}})",
                    default_params=self.param,
                    stack_depth=1,
                    static_template=False,
                )
            elif self.interval >= self.too_long:
                logger.note(
                    "Time too long: " + self.template + " ({{duration}})",
                    default_params=self.param,
                    stack_depth=1,
                    static_template=False,
                )

    @property
    def duration(self):
        end = time()
        if not self.end:
            return Duration(end - self.start)

        return Duration(self.interval)

    @property
    def total(self):
        if not self.end:
            logger.error("please ask for total time outside the context of measuring")

        return Duration(self.agg)
