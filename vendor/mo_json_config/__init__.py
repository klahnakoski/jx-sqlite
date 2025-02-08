# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_json_config.configuration import Configuration
from mo_json_config.expander import get, get_file, expand, is_url

__all__ = ["get", "get_file", "expand", "configuration", "Configuration", "is_url"]


configuration = Configuration({})
