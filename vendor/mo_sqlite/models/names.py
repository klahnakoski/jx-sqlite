# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
# BUILD THE Names (NAMESPACE) SEEN FROM ONE TABLE OF A SNOWFLAKE.
# EVERY LEAF COLUMN HAS ONE CANONICAL COORDINATE: ITS FACT-ABSOLUTE TYPED PATH
# (concat(nested_path[0], es_column), eg testing._a.$A.b.$S).  ALL OTHER NAMES ARE DERIVED
# FROM IT BY PURE PATH ALGEBRA - NEVER MIX THE TYPED AND UNTYPED NAMESPACES MID-COMPUTATION.
from jx_base.models.names import Names, Scope
from mo_dots import concat_field, relative_field, startswith_field
from mo_json import EXISTS, OBJECT
from mo_sql.utils import GUID, ORDER, PARENT, UID, untype_field

HIDDEN = (GUID, UID, ORDER, PARENT)


def snowflake_names(snowflake, origin: str) -> Names:
    """
    THE NAMESPACE SEEN FROM origin (A FULL TABLE NAME IN snowflake.query_paths)
    """
    return build_names(snowflake.query_paths, snowflake.columns, origin)


def build_names(query_paths, columns, origin: str) -> Names:
    """
    SCOPE ORDER (NEAREST FIRST, MATCHING Schema.leaves SEARCH ORDER):
        origin, ANCESTORS UP TO FACT, THEN DESCENDANTS OF origin (SHALLOWEST FIRST)
    EACH TABLE'S SCOPE COVERS ITS WHOLE SUBTREE, KEYED BY UNTYPED PATH RELATIVE TO THAT
    TABLE.  PHYSICAL (TYPED) NAMES ARE aliases AT THE ORIGIN SCOPE ONLY: PHYSICAL
    ADDRESSES DO NOT TRAVEL THE SCOPE CHAIN.
    HIDDEN COLUMNS (_id/__id__/__order__/__parent__) ARE NOT PART OF THE NAMESPACE.
    """
    tables = list(query_paths)
    ancestors = sorted((t for t in tables if startswith_field(origin, t)), key=len, reverse=True)
    descendants = sorted((t for t in tables if startswith_field(t, origin) and t != origin), key=len)
    fact = next(t for t in tables if all(startswith_field(u, t) for u in tables))

    columns = [
        c
        for c in columns
        if c.json_type not in (OBJECT, EXISTS) and c.es_column not in HIDDEN and c.name not in HIDDEN
    ]

    scopes = []
    for i, table in enumerate([*ancestors, *descendants]):
        names = {}
        aliases = {}
        boundaries = {}
        fan_out = {}
        for c in columns:
            home = c.nested_path[0]  # THE TABLE THIS COLUMN LIVES IN (AUTHORITATIVE, NOT es_index)
            if not startswith_field(home, table):
                continue  # NOT IN THIS SCOPE'S SUBTREE
            typed_name = relative_field(concat_field(home, c.es_column), table)
            untyped_name, _ = untype_field(typed_name)
            names.setdefault(untyped_name, []).append(c)
            # THE ARRAY HOLDING THE VALUE, RELATIVE TO THIS SCOPE ("." = THE SCOPE'S OWN TABLE)
            boundaries.setdefault(untyped_name, []).append(untype_field(relative_field(home, table))[0])
            # ON THE PERSPECTIVE'S LINE (ANCESTOR, SELF, OR DESCENDANT OF origin) = ONE VALUE PER
            # ELEMENT OF THE PERSPECTIVE.  ANYTHING ELSE IS A SIBLING/COUSIN ARRAY: A FAN-OUT
            fan_out.setdefault(untyped_name, []).append(
                not startswith_field(home, origin) and not startswith_field(origin, home)
            )
            if i == 0:
                aliases.setdefault(typed_name, []).append(c)
        scopes.append(Scope(
            {k: tuple(v) for k, v in names.items()},
            {k: tuple(v) for k, v in aliases.items()},
            root_is_array=(table != fact),
            boundaries={k: tuple(v) for k, v in boundaries.items()},
            fan_out={k: tuple(v) for k, v in fan_out.items()},
        ))
    return Names(scopes)
