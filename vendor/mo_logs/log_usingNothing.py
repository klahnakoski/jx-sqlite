# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
class StructuredLogger:
    """
    ABSTRACT BASE CLASS FOR JSON LOGGING
    """

    def write(self, template, params):
        pass

    def stop(self):
        pass


StructuredLogger_usingNothing = StructuredLogger
