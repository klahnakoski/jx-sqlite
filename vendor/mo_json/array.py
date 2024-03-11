# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

class Array:
    """
    JX IS UNABLE TO DISTINGUISH BETWEEN A SINGLE OBJECT AND AN ARRAY WITH A SINGLE OBJECT; THEY ARE BOTH A SINGLE OBJECT
    THIS CLASS IS USED TO TALK ABOUT SINGLETON ARRAYS, FOR USE BY OTHER LANGUAGES
    """

    __slots__ = ['value']

    def __init__(self, value):
        self.value = value

