# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

import re

from mo_dots import Data, join_field
from mo_files import URL
from mo_future import get_function_name
from mo_logs import logger, Except
from mo_imports import delay_import

RETRY_SECONDS = 1
TIMEOUT_SECONDS = 60

Till = delay_import("mo_threads.till.Till")

has_failed = False
call_counts = Data()


def _retry(func):
    func_name = get_function_name(func)

    def output(*args, **kwargs):
        timeout = Till(seconds=TIMEOUT_SECONDS)
        cause = None
        while not timeout:
            try:
                call_counts[func_name] += 1
                return func(*args, **kwargs)
            except Exception as cause:
                cause = Except.wrap(cause)
                if "ThrottlingException" in cause.message:
                    logger.warning("Throttled", cause=cause)
                    Till(seconds=RETRY_SECONDS).wait()
                    continue
                logger.error("failure with {func}", func=func_name, cause=cause)
        logger.error("timeout with {func}", func=func_name, cause=cause)
    return output


def get_ssm(ref, doc_path=None, location=None):
    global has_failed

    output = Data()

    if has_failed:
        return output
    try:
        import boto3
    except Exception:
        logger.error("Missing boto3: `pip install boto3` to use ssm://")

    if isinstance(ref, str):
        ref = URL(ref)

    try:
        ssm = boto3.client("ssm")
        describe_parameters = _retry(ssm.describe_parameters)
        get_parameter = _retry(ssm.get_parameter)

        filters = [{
            'Key': 'Name',
            'Option': 'BeginsWith',
            'Values': [ref.path.rstrip('/')]
        }]

        result = describe_parameters(MaxResults=10, ParameterFilters=filters)
        prefix = re.compile("^" + re.escape(ref.path.rstrip("/")) + "(?:/|$)")
        while True:
            for param in result["Parameters"]:
                name = param["Name"]
                found = prefix.match(name)
                if not found:
                    continue
                tail = join_field(name[found.regs[0][1] :].split("/"))
                if not tail:
                    return get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
                detail = get_parameter(Name=name, WithDecryption=True)
                output[tail] = detail["Parameter"]["Value"]

            next_token = result.get("NextToken")
            if not next_token:
                break
            result = describe_parameters(NextToken=next_token, MaxResults=10, ParameterFilters=filters)
    except Exception as cause:
        has_failed = True
        logger.warning("Could not get ssm parameters", cause=cause)
        return output

    if len(output) == 0:
        logger.error("No ssm parameters found at {path}", path=ref.path)
    return output

