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
import sys

from mo_dots import Data, coalesce, list_to_data, from_data
from mo_files import File
from mo_future import allocate_lock as _allocate_lock, text, zip_longest
from mo_imports import delay_import
from mo_kwargs import override
from mo_logs import ERROR, logger, Except, get_stacktrace, format_trace
from mo_math.stats import percentile
from mo_sql import *
from mo_threads import Lock, Queue, Thread, Till
from mo_times import Timer

from mo_sqlite.transacfion import Transaction
from mo_sqlite.utils import quote_column, sql_query, CommandItem, COMMIT, quote_value, BEGIN, ROLLBACK, FORMAT_COMMAND

jx_expression = delay_import("jx_base.jx_expression")
table2csv = delay_import("jx_python.convert.table2csv")
Relation = delay_import("jx_sqlite.models.relation.Relation")


DEBUG = False
TRACE = True

DOUBLE_TRANSACTION_ERROR = "You can not query outside a transaction you have open already"
TOO_LONG_TO_HOLD_TRANSACTION = 10

_sqlite3 = None
_load_extension_warning_sent = False
_upgraded = False
known_databases = {}


class Sqlite(DB):
    """
    Allows multi-threaded access
    Loads extension functions (like SQRT)
    """

    @override
    def __init__(
        self, filename=None, db=None, trace=None, upgrade=False, load_functions=False, debug=False, kwargs=None,
    ):
        """
        :param filename:  FILE TO USE FOR DATABASE
        :param db: AN EXISTING sqlite3 DB YOU WOULD LIKE TO USE (INSTEAD OF USING filename)
        :param trace: GET THE STACK TRACE AND THREAD FOR EVERY DB COMMAND (GOOD FOR DEBUGGING)
        :param upgrade: REPLACE PYTHON sqlite3 DLL WITH MORE RECENT ONE, WITH MORE FUNCTIONS (NOT WORKING)
        :param load_functions: LOAD EXTENDED MATH FUNCTIONS (MAY REQUIRE upgrade)
        :param kwargs:
        """
        global _upgraded
        global _sqlite3

        self.settings = kwargs
        if not _upgraded:
            if upgrade:
                _upgrade()
            _upgraded = True
            import sqlite3 as _sqlite3

            _ = _sqlite3

        if filename is None:
            self.filename = None
        else:
            file = File(filename)
            file.parent.create()
            self.filename = file.abs_path
            if known_databases.get(self.filename):
                logger.error(
                    "Not allowed to create more than one Sqlite instance for {{file}}", file=self.filename,
                )
            else:
                known_databases[self.filename] = self
        self.debug = debug | DEBUG
        self.trace = coalesce(trace, TRACE) or self.debug

        # SETUP DATABASE
        self.debug and logger.note("Sqlite version {{version}}", version=_sqlite3.sqlite_version)
        try:
            if not isinstance(db, _sqlite3.Connection):
                self.db = _sqlite3.connect(
                    database=coalesce(self.filename, ":memory:"), check_same_thread=False, isolation_level=None,
                )
            else:
                self.db = db
        except Exception as e:
            logger.error("could not open file {{filename}}", filename=self.filename, cause=e)
        self.upgrade = upgrade
        load_functions and self._load_functions()

        self.locker = Lock()
        self.available_transactions = []  # LIST OF ALL THE TRANSACTIONS BEING MANAGED
        self.queue = Queue("sql commands")  # HOLD (command, result, signal, stacktrace) TUPLES

        self.closed = False

        # WORKER VARIABLES
        self.transaction_stack = []  # THE TRANSACTION OBJECT WE HAVE PARTIALLY RUN
        self.last_command_item = None  # USE THIS TO HELP BLAME current_transaction FOR HANGING ON TOO LONG
        self.too_long = None
        self.delayed_queries = []
        self.delayed_transactions = []
        self.worker = None
        self.worker = Thread.run("sqlite db thread", self._worker, parent_thread=self)

        self.debug and logger.note(
            "Sqlite version {{version}}", version=self.query("select sqlite_version()").data[0][0],
        )

    def _enhancements(self):
        def regex(pattern, value):
            return 1 if re.match(pattern + "$", value) else 0

        con = self.db.create_function("regex", 2, regex)

        class Percentile(object):
            def __init__(self, percentile):
                self.percentile = percentile
                self.acc = []

            def step(self, value):
                self.acc.append(value)

            def finalize(self):
                return percentile(self.acc, self.percentile)

        con.create_aggregate("percentile", 2, Percentile)

    def read_sql(self, filename):
        """
        EXECUTE THE SQL FOUND IN FILE

        YOU CAN CREATE THE FILE WITH
        sqlite> .output chinook.sql
        sqlite> .dump
        """
        with self.transaction() as t:

            def commands():
                with open(File(filename).os_path, "r+b") as file:
                    acc = []
                    for line in file.readlines():
                        line = line.decode("utf8")
                        acc.append(line)
                        if line.strip().endswith(";"):
                            yield "\n".join(acc)
                            acc = []
                    yield "\n".join(acc)

            for command in commands():
                if "BEGIN TRANS" in command or "COMMIT" in command:
                    continue
                t.execute(command)

    def transaction(self):
        thread = Thread.current()
        parent = None
        with self.locker:
            for t in self.available_transactions:
                if t.thread is thread:
                    parent = t

        output = Transaction(self, parent=parent, thread=thread)
        self.available_transactions.append(output)
        return output

    def about(self, table_name):
        """
        :param table_name: TABLE OF INTEREST
        :return: SOME INFORMATION ABOUT THE TABLE
            (cid, name, dtype, notnull, dfft_value, pk) tuples
        """
        details = self.query("PRAGMA table_info" + sql_iso(quote_column(table_name)))
        return details.data

    def get_tables(self):
        result = self.query(sql_query({
            "from": "sqlite_master",
            "where": {"eq": {"type": "table"}},
            "orderby": "name",
        }))
        return list_to_data([{k: d for k, d in zip(result.header, row)} for row in result.data])

    def get_relations(self, table_name):
        """
        :param table_name: TABLE OF INTEREST
        :return: THE FOREIGN KEYS
        """
        result = self.query("PRAGMA foreign_key_list" + sql_iso(quote_column(table_name)))
        relations = Data()
        for row in result.data:
            desc = {h: v for h, v in zip(result.header, row)}
            id = str(row[0])
            seq = row[1]
            if not relations[id]:
                relations[id] = []
            relations[id][seq] = desc

        return [
            Relation(cols[0]["table"], [c["to"] for c in cols], table_name, [c["from"] for c in cols],)
            for id, cols in from_data(relations).items()
        ]

    def query(self, command):
        """
        WILL BLOCK CALLING THREAD UNTIL THE command IS COMPLETED
        :param command: COMMAND FOR SQLITE
        :return: list OF RESULTS
        """
        if self.closed:
            logger.error("database is closed")

        signal = _allocate_lock()
        signal.acquire()
        result = Data()
        trace = get_stacktrace(1) if self.trace else None

        if self.trace:
            current_thread = Thread.current()
            with self.locker:
                for t in self.available_transactions:
                    if t.thread is current_thread:
                        logger.error(DOUBLE_TRANSACTION_ERROR)

        self.queue.add(CommandItem(str(command), result, signal, trace, None))
        signal.acquire()

        if result.exception:
            logger.error("Problem with Sqlite call", cause=result.exception)
        return result

    def stop(self):
        """
        OPTIONAL COMMIT-AND-CLOSE
        IF THIS IS NOT DONE, THEN THE THREAD THAT SPAWNED THIS INSTANCE WILL
        """
        self.closed = True
        signal = _allocate_lock()
        signal.acquire()
        self.queue.add(CommandItem(COMMIT, Data(), signal, None, None))
        signal.acquire()
        self.worker.stop().join()
        self.worker = None

    def remove_child(self, child):
        if child is self.worker:
            self.worker = None

    def add_child(self, child):
        pass

    def close(self):
        logger.error("Use stop()")

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def _load_functions(self):
        global _load_extension_warning_sent
        library_loc = File.new_instance(sys.modules[__name__].__file__, "../../..")
        full_path = File.new_instance(library_loc, "vendor/sqlite/libsqlitefunctions.so").abs_path
        try:
            trace = get_stacktrace(0)[0]
            if self.upgrade:
                if os.name == "nt":
                    file = File.new_instance(trace["file"], "../../vendor/sqlite/libsqlitefunctions.so")
                else:
                    file = File.new_instance(trace["file"], "../../vendor/sqlite/libsqlitefunctions")

                full_path = file.abs_path
                self.db.enable_load_extension(True)
                self.db.execute(str(SQL_SELECT + "load_extension" + sql_iso(quote_value(full_path))))
        except Exception as e:
            if not _load_extension_warning_sent:
                _load_extension_warning_sent = True
                logger.warning(
                    "Could not load {{file}}, doing without. (no SQRT for you!)", file=full_path, cause=e,
                )

    def create_new_functions(self):
        def regexp(pattern, item):
            return re.search(pattern, item) is not None

        self.db.create_function("REGEXP", 2, regexp)

    def show_transactions_blocked_warning(self):
        blocker = self.last_command_item
        blocked = (self.delayed_queries + self.delayed_transactions)[0]

        logger.warning(
            "Query on thread {{blocked_thread|json}} at\n"
            "{{blocked_trace|indent}}"
            "is blocked by {{blocker_thread|json}} at\n"
            "{{blocker_trace|indent}}"
            "this message brought to you by....",
            blocker_trace=format_trace(blocker.trace),
            blocked_trace=format_trace(blocked.trace),
            blocker_thread=blocker.transaction.thread.name if blocker.transaction is not None else None,
            blocked_thread=blocked.transaction.thread.name if blocked.transaction is not None else None,
        )

    def _close_transaction(self, command_item):
        query, result, signal, trace, transaction = command_item

        transaction.end_of_life = True
        with self.locker:
            self.available_transactions.remove(transaction)
            assert transaction not in self.available_transactions

            old_length = len(self.transaction_stack)
            old_trans = self.transaction_stack[-1]
            del self.transaction_stack[-1]

            assert old_length - 1 == len(self.transaction_stack)
            assert old_trans
            assert old_trans not in self.transaction_stack
        if not self.transaction_stack:
            # NESTED TRANSACTIONS NOT ALLOWED IN sqlite3
            self.debug and logger.note(FORMAT_COMMAND, command=query, **command_item.trace[0])
            self.db.execute(query)

        has_been_too_long = False
        with self.locker:
            if self.too_long is not None:
                self.too_long, too_long = None, self.too_long
                # WE ARE CHEATING HERE: WE REACH INTO THE Signal MEMBERS AND REMOVE WHAT WE ADDED TO THE INTERNAL job_queue
                with too_long.lock:
                    has_been_too_long = bool(too_long)
                    too_long.job_queue = None

            # PUT delayed BACK ON THE QUEUE, IN THE ORDER FOUND, BUT WITH QUERIES FIRST
            if self.delayed_transactions:
                for c in reversed(self.delayed_transactions):
                    self.queue.push(c)
                del self.delayed_transactions[:]
            if self.delayed_queries:
                for c in reversed(self.delayed_queries):
                    self.queue.push(c)
                del self.delayed_queries[:]
        if has_been_too_long:
            logger.note("Transaction blockage cleared")

    def _worker(self, please_stop):
        try:
            # MAIN EXECUTION LOOP
            while not please_stop:
                command_item = self.queue.pop(till=please_stop)
                if command_item is None:
                    break
                try:
                    self._process_command_item(command_item)
                except Exception as cause:
                    logger.warning("can not execute command {{command}}", command=command_item.command, cause=cause)
        except Exception as e:
            e = Except.wrap(e)
            if not please_stop:
                logger.warning("Problem with sql", cause=e)
        finally:
            self.closed = True
            self.debug and logger.note("Database is closed")
            self.db.close()
            if self.filename:
                del known_databases[self.filename]
            else:
                self.filename = ":memory:"
            self.debug and logger.note("Database {name|quote} is closed", name=self.filename)

    def _process_command_item(self, command_item):
        query, result, signal, trace, transaction = command_item

        with Timer("SQL Timing", verbose=self.debug):
            if transaction is None:
                # THIS IS A TRANSACTIONLESS QUERY, DELAY IT IF THERE IS A CURRENT TRANSACTION
                if self.transaction_stack:
                    with self.locker:
                        if self.too_long is None:
                            self.too_long = Till(seconds=TOO_LONG_TO_HOLD_TRANSACTION)
                            self.too_long.then(self.show_transactions_blocked_warning)
                        self.delayed_queries.append(command_item)
                    return
            elif self.transaction_stack and self.transaction_stack[-1] not in [
                transaction,
                transaction.parent,
            ]:
                # THIS TRANSACTION IS NOT THE CURRENT TRANSACTION, DELAY IT
                with self.locker:
                    if self.too_long is None:
                        self.too_long = Till(seconds=TOO_LONG_TO_HOLD_TRANSACTION)
                        self.too_long.then(self.show_transactions_blocked_warning)
                    self.delayed_transactions.append(command_item)
                return
            else:
                # ENSURE THE CURRENT TRANSACTION IS UP TO DATE FOR THIS query
                if not self.transaction_stack:
                    # sqlite3 ALLOWS ONLY ONE TRANSACTION AT A TIME
                    self.debug and logger.note(FORMAT_COMMAND, command=BEGIN, **command_item.trace[0])
                    self.db.execute(BEGIN)
                    self.transaction_stack.append(transaction)
                elif transaction is not self.transaction_stack[-1]:
                    self.transaction_stack.append(transaction)
                elif transaction.exception and query is not ROLLBACK:
                    result.exception = Except(
                        context=ERROR,
                        template="Not allowed to continue using a transaction that failed",
                        cause=transaction.exception,
                        trace=trace,
                    )
                    signal.release()
                    return

                try:
                    transaction.do_all()
                except Exception as cause:
                    # DEAL WITH ERRORS IN QUEUED COMMANDS
                    # WE WILL UNWRAP THE OUTER EXCEPTION TO GET THE CAUSE
                    err = Except(
                        context=ERROR,
                        template="Bad call to Sqlite3 while " + FORMAT_COMMAND,
                        params={"command": cause.params.current.command},
                        cause=cause.cause,
                        trace=cause.params.current.trace,
                    )
                    transaction.exception = result.exception = err

                    if query in [COMMIT, ROLLBACK]:
                        self._close_transaction(CommandItem(ROLLBACK, result, signal, trace, transaction))

                    signal.release()
                    return

            try:
                # DEAL WITH END-OF-TRANSACTION MESSAGES
                if query in [COMMIT, ROLLBACK]:
                    self._close_transaction(command_item)
                    return

                # EXECUTE QUERY
                self.last_command_item = command_item
                self.debug and logger.note(FORMAT_COMMAND, command=query, **command_item.trace[0])
                curr = self.db.execute(query)
                result.meta.format = "table"
                result.header = [d[0] for d in curr.description] if curr.description else None
                result.data = curr.fetchall()
                if self.debug and result.data:
                    csv = table2csv(list(result.data))
                    logger.note("Result:\n{{data|limit(1000)|indent}}", data=csv)
            except Exception as cause:
                cause = Except.wrap(cause)
                err = Except(
                    context=ERROR,
                    template="Bad call to Sqlite while " + FORMAT_COMMAND,
                    params={"command": query},
                    trace=trace,
                    cause=cause,
                )
                result.exception = err
                if transaction:
                    transaction.exception = err
            finally:
                signal.release()


def _upgrade():
    global _upgraded
    global _sqlite3

    try:
        import sys
        import platform

        if "windows" in platform.system().lower():
            original_dll = File.new_instance(sys.exec_prefix, "dlls/sqlite3.dll")
            if platform.architecture()[0] == "32bit":
                source_dll = File("vendor/jx-sqlite/vendor/sqlite/sqlite3_32.dll")
            else:
                source_dll = File("vendor/jx-sqlite/vendor/sqlite/sqlite3_64.dll")

            if not all(a == b for a, b in zip_longest(source_dll.read_bytes(), original_dll.read_bytes())):
                original_dll.backup()
                File.copy(source_dll, original_dll)
        else:
            pass
    except Exception as e:
        logger.warning("could not upgrade python's sqlite", cause=e)

    import sqlite3 as _sqlite3

    _ = _sqlite3
    _upgraded = True
