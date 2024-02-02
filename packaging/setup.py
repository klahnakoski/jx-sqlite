# encoding: utf-8
# THIS FILE IS AUTOGENERATED!
from setuptools import setup
setup(
    author='Rohit Kumar, Kyle Lahnakoski',
    author_email='rohitkumar.a255@gmail.com, kyle@lahnakoski.com',
    classifiers=["Development Status :: 4 - Beta","Topic :: Software Development :: Libraries","Topic :: Software Development :: Libraries :: Python Modules","License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)","Programming Language :: Python :: 3.8","Programming Language :: Python :: 3.9","Programming Language :: Python :: 3.10","Programming Language :: Python :: 3.11","Programming Language :: Python :: 3.12"],
    description='JSON query expressions using SQLite',
    extras_require={"tests":["mo-testing"]},
    include_package_data=True,
    install_requires=["hjson","jx-python==4.525.24033","mo-sql==4.524.24033","mo-sqlite==1.525.24033","requests"],
    license='MPL 2.0',
    long_description='# jx-sqlite \n\nJSON query expressions using SQLite\n\n\n[![PyPI Latest Release](https://img.shields.io/pypi/v/jx-sqlite.svg)](https://pypi.org/project/jx-sqlite/)\n[![Build Status](https://app.travis-ci.com/klahnakoski/jx-sqlite.svg?branch=master)](https://travis-ci.com/github/klahnakoski/jx-sqlite)\n\n\n## Summary\n\nThis library will manage your database schema to store JSON documents. You get all the speed of a well-formed database schema without the schema migration headaches. \n\n\n## Status\n\nSignificant updates to the supporting libraries has broken this code.  It still works for the simple cases that require it\n\n**Jan 2024** - 91/320 tests ignored\n\n\n## Installation\n\n    pip install jx-sqlite\n\n## Code Example\n\nThe smoke test, found in the `tests` is a simple example of how to use this library.\n\nOpen a database \n\n```python\nimport jx_sqlite\n\ntable = (\n    jx_sqlite\n    .Container(filename="my.db")\n    .get_or_create_facts("my_table")\n    .add({"os":"linux", "value":42})\n    .query({\n        "select": "os", \n        "where": {"gt": {"value": 0}}\n    })\n)\n```\n\n## More\n\nAn attempt to store JSON documents in SQLite so that they are accessible via SQL. The hope is this will serve a basis for a general document-relational map (DRM), and leverage the database\'s query optimizer.\njx-sqlite  is also responsible for making the schema, and changing it dynamically as new JSON schema are encountered and to ensure that the old queries against the new schema have the same meaning.\n\nThe most interesting, and most important feature is that we query nested object arrays as if they were just another table.  This is important for two reasons:\n\n1. Inner objects `{"a": {"b": 0}}` are a shortcut for nested arrays `{"a": [{"b": 0}]}`, plus\n2. Schemas can be expanded from one-to-one  to one-to-many `{"a": [{"b": 0}, {"b": 1}]}`.\n\n\n## Motivation\n\nJSON is a nice format to store data, and it has become quite prevalent. Unfortunately, databases do not handle it well, often a human is required to declare a schema that can hold the JSON before it can be queried. If we are not overwhelmed by the diversity of JSON now, we soon will be. There will be more JSON, of more different shapes, as the number of connected devices( and the information they generate) continues to increase.\n\n## Contributing\n\nContributions are always welcome! The best thing to do is find a failing test, and try to fix it.\n\nThese instructions will get you a copy of the project up and running on your local machine for development and testing purposes.\n\n    $ git clone https://github.com/mozilla/jx-sqlite\n    $ cd jx-sqlite\n\n### Running tests\n\nThere are over 200 tests used to confirm the expected behaviour: They test a variety of JSON forms, and the queries that can be performed on them. Most tests are further split into three different output formats ( list, table and cube).\n\n    export PYTHONPATH=.\n    python -m unittest discover -v -s tests\n\n### Technical Docs\n\n* [Json Query Expression](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx.md)\n* [Nomenclature](https://github.com/mozilla/jx-sqlite/blob/master/docs/Nomenclature.md)\n* [Snowflake](https://github.com/mozilla/jx-sqlite/blob/master/docs/Perspective.md)\n* [JSON in Database](https://github.com/mozilla/jx-sqlite/blob/master/docs/JSON%20in%20Database.md)\n* [The Future](https://github.com/mozilla/jx-sqlite/blob/master/docs/The%20Future.md)\n\n## License\n\nThis project is licensed under Mozilla Public License, v. 2.0. If a copy of the MPL was not distributed with this file, You can obtain one at http://mozilla.org/MPL/2.0/.\n\n\n## History\n\n*Sep 2018* - Upgrade libs, start refactoring to work with other libs\n\n*Dec 2017* - A number of tests were added, but they do not pass.\n\n*Sep 2017* - GSoC work completed, all but a few tests pass.\n \n\n## GSOC\n\nGood work by Rohit Kumar.  You may see the end result on [gsoc branch](https://github.com/klahnakoski/jx-sqlite/tree/gsoc).  Installation requires python2.7,  and will require some version fixing to get running.\n\nSee [the demonstration video](https://www.youtube.com/watch?v=0_YLzb7BegI&list=PLSE8ODhjZXja7K1hjZ01UTVDnGQdx5v5U&index=26&t=260s)\n\n\nWork done up to the deadline of GSoC\'17:\n\n* [Pull Requests](https://github.com/mozilla/jx-sqlite/pulls?utf8=%E2%9C%93&q=is%3Apr%20author%3Arohit-rk)\n* [Commits](https://github.com/mozilla/jx-sqlite/commits?author=rohit-rk)\n\n\n\n',
    long_description_content_type='text/markdown',
    name='jx-sqlite',
    package_dir={},
    packages=["jx_sqlite","jx_sqlite.models","jx_sqlite.expressions"],
    url='https://github.com/klahnakoski/jx-sqlite',
    version='5.525.24033'
)