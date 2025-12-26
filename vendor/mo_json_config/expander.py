# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from mo_dots import is_data, is_list, set_default, to_data, get_attr, listwrap, unwraplist
from mo_files import File
from mo_files.url import URL
from mo_logs import Except, logger, get_stacktrace

from mo_json_config.expand_locals import _replace_locals, _replace_str
from mo_json_config.schemes import scheme_loaders

NOTSET = {}
LOOKBACK = 1


def get(url):
    caller = URL("file://" + File(get_stacktrace(start=LOOKBACK)[0]["file"]).abs_path)
    return expand(to_data({"$ref": url}), doc_url=caller)


get_file = get


def expand(doc, doc_url="param://", params=None):
    """
    ASSUMING YOU ALREADY PULLED THE doc FROM doc_url, YOU CAN STILL USE THE
    EXPANDING FEATURE

    USE mo_json_config.expand({}) TO ASSUME CURRENT WORKING DIRECTORY

    :param doc: THE DATA STRUCTURE FROM JSON SOURCE
    :param doc_url: THE URL THIS doc CAME FROM (DEFAULT USES params AS A DOCUMENT SOURCE)
    :param params: EXTRA PARAMETERS NOT FOUND IN THE doc_url PARAMETERS (WILL SUPERSEDE PARAMETERS FROM doc_url)
    :return: EXPANDED JSON-SERIALIZABLE STRUCTURE
    """
    url = URL(doc_url)
    if not url.scheme:
        logger.error("{url} must have a protocol (eg https://) declared", url=doc_url)

    url.query = set_default(params, url.query)
    phase1 = _replace_foreign_ref((doc, None), url)  # BLANK URL ONLY WORKS IF url IS ABSOLUTE
    phase2 = _replace_locals((phase1, None), url)
    return to_data(phase2)


def _replace_foreign_ref(path, url):
    """
    RECURSIVELY REPLACE FOREIGN REFERENCES IN THE DATA STRUCTURE
    :param path:
    :param url:
    :return:
    """
    if url.path.endswith("/"):
        url.path = url.path[:-1]

    node, _ = path
    if is_list(node):
        output = [_replace_foreign_ref((n, path), url) for n in node]
        return output
    elif not is_data(node):
        return node

    if "$ref" not in node:
        output = {}
        for k, v in node.items():
            k = _replace_str(k, path, url)
            output[k] = _replace_foreign_ref((v, path), url)
        return output

    refs = URL(_replace_str(str(node["$ref"]), path, url))
    if "$default" in node:
        defaults = _replace_foreign_ref((node["$default"], path), url)
    else:
        defaults = NOTSET

    output = {}
    for k, v in node.items():
        if k not in ("$ref", "$default"):
            k = _replace_str(k, path, url)
            output[k] = _replace_foreign_ref((v, path), url)

    if not refs:
        if defaults is not NOTSET:
            return defaults
        return output

    ref_found = False
    ref_error = None
    ref_remain = []
    for ref in listwrap(refs):
        if not ref.scheme and not ref.path:
            # DO NOT TOUCH LOCAL REF YET
            ref_remain.append(ref)
            ref_found = True
            continue

        if not ref.scheme:
            # SCHEME RELATIVE IMPLIES SAME PROTOCOL AS LAST TIME, WHICH
            # REQUIRES THE CURRENT DOCUMENT'S SCHEME
            ref.scheme = url.scheme

        # FIND THE SCHEME AND LOAD IT
        if ref.scheme not in scheme_loaders:
            raise logger.error("unknown protocol {scheme}", scheme=ref.scheme)
        try:
            new_value = scheme_loaders[ref.scheme](ref, (node, path), url)
            ref_found = True
        except Exception as cause:
            ref_error = Except.wrap(cause)
            continue

        if ref.fragment:
            new_value = get_attr(new_value, ref.fragment)

        if not output:
            output = new_value
        elif isinstance(output, str):
            pass  # WE HAVE A VALUE
        else:
            set_default(output, new_value)

    if not ref_found:
        if defaults is NOTSET:
            raise ref_error
    if ref_remain:
        output["$ref"] = unwraplist(ref_remain)
        output["$default"] = defaults
    if not output and defaults is not NOTSET:
        output = defaults
    return output
