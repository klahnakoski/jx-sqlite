# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

import os
import re

from mo_dots import (
    is_data,
    is_list,
    set_default,
    from_data,
    to_data,
    is_sequence,
    coalesce,
    get_attr,
    listwrap,
    unwraplist,
    dict_to_data,
)
from mo_files import File
from mo_files.url import URL
from mo_future import first
from mo_json import json2value
from mo_logs import Except, logger, get_stacktrace

from mo_json_config.configuration import Configuration
from mo_json_config.convert import ini2value
from mo_json_config.ssm import get_ssm as _get_ssm

CAN_NOT_READ_FILE = "Can not read file {filename}"
DEBUG = False


def get_file(file):
    file = File(file)
    return get("file://" + file.abs_path)


def get(url):
    """
    USE json.net CONVENTIONS TO LINK TO INLINE OTHER JSON
    """
    url = str(url)
    if "://" not in url:
        logger.error("{url} must have a prototcol (eg http://) declared", url=url)
    path = (dict_to_data({"$ref": url}), None)

    if url.startswith("file://") and url[7] != "/":
        causes= []
        candidates = [os.path.dirname(os.path.abspath(get_stacktrace(start=1)[0]['file'])), os.getcwd()]
        for candidate in candidates:
            if os.sep == "\\":
                base = URL("file:///" + candidate.replace(os.sep, "/").rstrip("/") + "/.")
            else:
                base = URL("file://" + candidate.rstrip("/") + "/.")
            try:
                phase1 = _replace_ref(path, base)
                break
            except Exception as cause:
                if CAN_NOT_READ_FILE in cause:
                    # lower priority cause
                    causes.append(cause)
                else:
                    causes.insert(0, cause)
        else:
            logger.error("problem replacing ref in {url}", url=url, cause=first(causes))
    else:
        phase1 = _replace_ref(path, URL(""))  # BLANK URL ONLY WORKS IF url IS ABSOLUTE

    try:
        phase2 = _replace_locals((phase1,None), url)
        return to_data(phase2)
    except Exception as cause:
        logger.error("problem replacing locals in\n{phase1}", phase1=phase1, cause=cause)


def expand(doc, doc_url="param://", params=None):
    """
    ASSUMING YOU ALREADY PULED THE doc FROM doc_url, YOU CAN STILL USE THE
    EXPANDING FEATURE

    USE mo_json_config.expand({}) TO ASSUME CURRENT WORKING DIRECTORY

    :param doc: THE DATA STRUCTURE FROM JSON SOURCE
    :param doc_url: THE URL THIS doc CAME FROM (DEFAULT USES params AS A DOCUMENT SOURCE)
    :param params: EXTRA PARAMETERS NOT FOUND IN THE doc_url PARAMETERS (WILL SUPERSEDE PARAMETERS FROM doc_url)
    :return: EXPANDED JSON-SERIALIZABLE STRUCTURE
    """
    if "://" not in doc_url:
        logger.error("{url} must have a protocol (eg https://) declared", url=doc_url)

    url = URL(doc_url)
    url.query = set_default(url.query, params)
    phase1 = _replace_ref((doc, None), url)  # BLANK URL ONLY WORKS IF url IS ABSOLUTE
    phase2 = _replace_locals((phase1,None), url)
    return to_data(phase2)


is_url = re.compile(r"\{([0-9a-zA-Z]+://[^}]*)}")


def _replace_str(text, path, url):
    acc = []
    end = 0
    for found in is_url.finditer(text):
        acc.append(text[end: found.start()])
        try:
            ref = URL(found.group(1))
            if ref.scheme not in scheme_loaders:
                raise logger.error("unknown protocol {ref}", ref=ref)
            value = scheme_loaders[ref.scheme](ref, path, url)
            if value == None:
                raise logger.error("value not found {ref}", ref=ref)
            acc.append(value)
        except Exception as cause:
            raise logger.error("problem replacing {ref}", ref=found.group(1), cause=cause)
        end = found.end()
    if end == 0:
        return text
    return "".join(acc) + text[end:]


def _replace_ref(path, url):
    if url.path.endswith("/"):
        url.path = url.path[:-1]

    node = path[0]
    if is_data(node):
        refs = None
        output = {}
        for k, v in node.items():
            if k == "$ref":
                refs = URL(_replace_str(str(v), path, url))
            else:
                output[k] = _replace_ref((v, path), url)

        if not refs:
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

            DEBUG and logger.note("Replace {ref} with {new_value}", ref=ref, new_value=new_value)

            if not output:
                output = new_value
            elif isinstance(output, str):
                pass  # WE HAVE A VALUE
            else:
                set_default(output, new_value)

        if not ref_found:
            raise ref_error
        if ref_remain:
            output["$ref"] = unwraplist(ref_remain)
        DEBUG and logger.note("Return {output}", output=output)
        return output
    elif is_list(node):
        output = [_replace_ref((n, path), url) for n in node]
        return output

    return node


def _replace_locals(path, url):
    node = path[0]
    if is_data(node):
        # RECURS, DEEP COPY
        ref = None
        output = {}
        for k, v in node.items():
            if k == "$ref":
                ref = URL(_replace_str(str(v), path, url))
            elif k == "$concat":
                if not is_sequence(v):
                    logger.error("$concat expects an array of strings")
                return coalesce(node.get("separator"), "").join(_replace_locals((vv, path), url) for vv in v)
            elif v == None:
                continue
            else:
                output[k] = _replace_locals((v, path), url)

        if not ref:
            return output

        new_value = _get_value_from_fragment(ref, path, url)

        if not output:
            return new_value  # OPTIMIZATION FOR CASE WHEN node IS {}
        else:
            return from_data(set_default(output, new_value))

    elif is_list(node):
        return [_replace_locals((n, path), url) for n in node]

    elif isinstance(node, str):
        return _replace_str(node, path[1], url)
    return node


def _get_value_from_fragment(ref, path, url):
    # REFER TO SELF
    frag = ref.fragment
    if frag[0] == ".":
        doc = (None, path)
        # RELATIVE
        for i, c in enumerate(frag):
            if c == ".":
                if not isinstance(doc, tuple):
                    logger.error("{frag|quote} reaches up past the root document", frag=frag)
                doc = doc[1]
            else:
                break
        new_value = get_attr(doc[0], frag[i::])
    else:
        # ABSOLUTE
        top_doc = path
        while isinstance(top_doc, tuple) and top_doc[1]:
            top_doc = top_doc[1]
        new_value = get_attr(top_doc[0], frag)
    new_value = _replace_locals((new_value, path), url)
    return new_value


###############################################################################
## SCHEME LOADERS ARE BELOW THIS LINE
###############################################################################


def _get_file(ref, path, url):

    if ref.path.startswith("~"):
        home_path = os.path.expanduser("~")
        if os.sep == "\\":
            home_path = "/" + home_path.replace(os.sep, "/")
        if home_path.endswith("/"):
            home_path = home_path[:-1]

        ref.path = home_path + ref.path[1::]
    elif not ref.path.startswith("/"):
        # CONVERT RELATIVE TO ABSOLUTE
        if ref.path[0] == ".":
            num_dot = 1
            while ref.path[num_dot] == ".":
                num_dot += 1

            parent = url.path.rstrip("/").split("/")[:-num_dot]
            ref.path = "/".join(parent) + ref.path[num_dot:]
        else:
            parent = url.path.rstrip("/").split("/")[:-1]
            ref.path = "/".join(parent) + "/" + ref.path

    path = ref.path if os.sep != "\\" else ref.path[1::].replace("/", "\\")

    try:
        DEBUG and logger.note("reading file {path}", path=path)
        content = File(path).read()
    except Exception as e:
        content = None
        logger.error(CAN_NOT_READ_FILE, filename=File(path).os_path, cause=e)

    try:
        new_value = json2value(content, params=ref.query, flexible=True, leaves=True)
    except Exception as e:
        e = Except.wrap(e)
        try:
            new_value = ini2value(content)
        except Exception:
            raise logger.error(CAN_NOT_READ_FILE, filename=path, cause=e)
    new_value = _replace_ref((new_value, path), ref)
    return new_value


def get_http(ref, doc_path, url):
    import requests

    params = url.query
    new_value = json2value(requests.get(str(ref)).text, params=params, flexible=True, leaves=True)
    return new_value


def _get_env(ref, doc_path, url):
    # GET ENVIRONMENT VARIABLES
    ref = ref.host
    raw_value = os.environ.get(ref)
    if not raw_value:
        logger.error("expecting environment variable with name {env_var}", env_var=ref)

    try:
        new_value = json2value(raw_value)
    except Exception as e:
        new_value = raw_value
    return new_value


def _get_keyring(ref, doc_path, url):
    try:
        import keyring
    except Exception:
        logger.error("Missing keyring: `pip install keyring` to use this feature")

    # GET PASSWORD FROM KEYRING
    service_name = ref.host
    if "@" in service_name:
        username, service_name = service_name.split("@")
    else:
        username = ref.query.username

    raw_value = keyring.get_password(service_name, username)
    if not raw_value:
        logger.error(
            "expecting password in the keyring for service_name={service_name} and username={username}",
            service_name=service_name,
            username=username,
        )

    try:
        new_value = json2value(raw_value)
    except Exception as e:
        new_value = raw_value
    return new_value


def _get_param(ref, doc_path, url):
    # GET PARAMETERS FROM url
    param = url.query
    new_value = param[ref.host]
    return new_value

def _nothing(ref, doc_path, url):
    return f"{{{ref}}}"


scheme_loaders = {
    "http": get_http,
    "https": get_http,
    "file": _get_file,
    "env": _get_env,
    "param": _get_param,
    "keyring": _get_keyring,
    "ssm": _get_ssm,
    "ref": _get_value_from_fragment,
    "scheme": _nothing,
}


configuration = Configuration({})
