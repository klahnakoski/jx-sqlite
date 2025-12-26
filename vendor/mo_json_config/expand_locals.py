import re

from mo_dots import is_data, is_list, set_default, from_data, is_sequence, coalesce, is_missing
from mo_files.url import URL
from mo_logs import logger

from mo_json_config.schemes import scheme_loaders

DEBUG = False
NOTSET = {}


def _replace_locals(path, url):
    node, parent = path
    if is_data(node):
        for op, func in operators.items():
            if op in node:
                return func(node, path, url)
        return _replace_data(node, path, url)
    elif is_list(node):
        return [_replace_locals((n, path), url) for n in node]
    elif isinstance(node, str):
        return _replace_str(node, parent, url)
    return node


def _replace_data(node, path, url):
    output = {}
    for k, v in node.items():
        if is_missing(v) or k in operators:
            continue
        output[k] = _replace_locals((v, path), url)
    return output


def _replace_ref(ref_node, path, url):
    ref = URL(_replace_str(str(ref_node["$ref"]), path, url))
    new_value = scheme_loaders["ref"](ref, path, url)
    defaults = _replace_default(ref_node, path, url)

    output = {}
    for k, v in ref_node.items():
        if is_missing(v) or k in ("$ref", "$default"):
            continue
        k = _replace_str(k, path, url)
        output[k] = _replace_locals((v, path), url)

    if not output:
        if defaults is NOTSET:
            return from_data(new_value)
        else:
            return from_data(set_default(new_value, defaults))
    else:
        if defaults is NOTSET:
            return from_data(set_default(output, new_value))
        else:
            return from_data(set_default(output, new_value, defaults))


def _replace_default(node, path, url):
    defaults = node.get("$default", NOTSET)
    if defaults is NOTSET:
        return NOTSET
    return _replace_locals((node["$default"], path), url)


def _replace_concat(node, path, url):
    # RECURS, DEEP COPY
    v = node.get("$concat")
    if not is_sequence(v):
        logger.error("$concat expects an array of strings")
    return coalesce(node.get("separator"), "").join(_replace_locals((vv, path), url) for vv in v)


is_url = re.compile(r"\{([0-9a-zA-Z]+://[^}]*)}")


def _replace_str(text, path, url):
    acc = []
    end = 0
    for found in is_url.finditer(text):
        acc.append(text[end  : found.start()])
        try:
            ref = URL(found.group(1))
            if ref.scheme not in scheme_loaders:
                raise logger.error("unknown protocol {ref}", ref=ref)
            value = scheme_loaders[ref.scheme](ref, path, url)
            if is_missing(value):
                raise logger.error("value not found {ref}", ref=ref)
            acc.append(value)
        except Exception as cause:
            raise logger.error("problem replacing {ref}", ref=found.group(1), cause=cause)
        end = found.end()
    if end == 0:
        return text
    return "".join(acc) + text[end:]


operators = {
    "$ref": _replace_ref,
    "$concat": _replace_concat,
    "$default": _replace_default,
}
