# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from dataclasses import is_dataclass, fields
from typing import Optional

from mo_dots import unwraplist, Data, is_missing
from mo_future import allocate_lock as _allocate_lock
from mo_logs import Except, logger
from mo_logs.exceptions import get_stacktrace
from mo_sql import sql_iso
from mo_sql.utils import untype_field
from mo_threads import Lock

from mo_sqlite.utils import CommandItem, FORMAT_COMMAND, ROLLBACK, COMMIT, quote_column


class Transaction(object):
    def __init__(self, db, parent, thread):
        self.db = db
        self.locker = Lock(f"transaction {id(self)} todo lock")
        self.todo = []
        self.complete = 0
        self.end_of_life = False
        self.exception = None
        self.parent = parent
        self.thread = thread

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        causes = []
        try:
            if isinstance(exc_val, Exception):
                causes.append(Except.wrap(exc_val))
                self.rollback()
            else:
                self.commit()
        except Exception as cause:
            causes.append(Except.wrap(cause))
            logger.error("Transaction failed", cause=unwraplist(causes))

    def transaction(self):
        with self.db.locker:
            output = Transaction(self.db, parent=self, thread=self.thread)
            self.db.available_transactions.append(output)
        return output

    def execute(self, command):
        if self.end_of_life:
            logger.error("Transaction is dead")
        trace = get_stacktrace(1) if self.db.trace else None
        with self.locker:
            self.todo.append(CommandItem(str(command), None, None, trace, self))

    def do_all(self):
        # ENSURE PARENT TRANSACTION IS UP TO DATE
        c = None
        try:
            if self.parent == self:
                logger.warning("Transactions parent is equal to itself.")
            if self.parent:
                self.parent.do_all()
            # GET THE REMAINING COMMANDS
            with self.locker:
                todo = self.todo[self.complete:]
                self.complete = len(self.todo)

            # RUN THEM
            for c in todo:
                self.db.debug and logger.note(FORMAT_COMMAND, command=c.command, **c.trace[0])
                self.db.db.execute(str(c.command))
        except Exception as e:
            logger.error("problem running commands", current=c, cause=e)

    def query(self, query, *, as_dataclass=None):
        if self.db.closed:
            logger.error("database is closed")

        signal = _allocate_lock()
        signal.acquire()
        result = Data()
        trace = get_stacktrace(1) if self.db.trace else None
        self.db.queue.add(CommandItem(str(query), result, signal, trace, self))
        signal.acquire()
        if result.exception:
            logger.error("Problem with Sqlite call", cause=result.exception)
        if as_dataclass is None:
            # RETURN TABLE
            return result
        return table_to_list(result, as_dataclass=as_dataclass)

    def about(self, table_name):
        """
        :param table_name: TABLE OF INTEREST
        :return: SOME INFORMATION ABOUT THE TABLE
            (cid, name, dtype, notnull, dfft_value, pk) tuples
        """
        details = self.query("PRAGMA table_info" + sql_iso(quote_column(table_name)))
        return details.data

    def rollback(self):
        self.query(ROLLBACK)

    def commit(self):
        self.query(COMMIT)


def table_to_list(result, *, as_dataclass):
    if not is_dataclass(as_dataclass):
        logger.error("expecting @dataclass")

    # map header to fields
    field_names = fields(as_dataclass)
    fields_to_index = [None] * len(field_names)
    for fi, f in enumerate(field_names):
        for hi, h in enumerate(result.header):
            if untype_field(h)[0] == f.name:
                fields_to_index[fi] = hi

    output = []
    for row in result.data:
        obj = object.__new__(as_dataclass)
        obj.__dict__ = {
            f.name: None if hi is None else row.get(hi)
            for f, hi in zip(field_names, fields_to_index)
        }
        output.append(obj)
    return output


def value_to_type(value, type):
    if is_missing(value):
        return None
    return type(value)
