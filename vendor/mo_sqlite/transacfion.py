# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from mo_dots import unwraplist, Data
from mo_future import allocate_lock as _allocate_lock
from mo_logs import Except, logger
from mo_logs.exceptions import get_stacktrace
from mo_threads import Lock

from mo_sqlite.utils import CommandItem, FORMAT_COMMAND, ROLLBACK, COMMIT


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
                todo = self.todo[self.complete :]
                self.complete = len(self.todo)

            # RUN THEM
            for c in todo:
                self.db.debug and logger.note(FORMAT_COMMAND, command=c.command, **c.trace[0])
                self.db.db.execute(str(c.command))
        except Exception as e:
            logger.error("problem running commands", current=c, cause=e)

    def query(self, query):
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
        return result

    def rollback(self):
        self.query(ROLLBACK)

    def commit(self):
        self.query(COMMIT)
