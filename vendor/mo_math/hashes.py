# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.hashes import SHA256, Hash


def sha256(bytes):
    digest = Hash(SHA256(), backend=default_backend())
    digest.update(bytes)
    return digest.finalize()
