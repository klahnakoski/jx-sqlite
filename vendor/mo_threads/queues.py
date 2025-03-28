# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
# THIS THREADING MODULE IS PERMEATED BY THE please_stop SIGNAL.
# THIS SIGNAL IS IMPORTANT FOR PROPER SIGNALLING WHICH ALLOWS
# FOR FAST AND PREDICTABLE SHUTDOWN AND CLEANUP OF THREADS


import types
from collections import deque
from copy import copy
from queue import Empty, Full
from time import time

from mo_dots import Null, coalesce
from mo_logs import Except, logger

from mo_threads.lock import Lock
from mo_threads.signals import Signal
from mo_threads.threads import PLEASE_STOP, THREAD_TIMEOUT, Thread
from mo_threads.till import Till

DEBUG = False

DEFAULT_WAIT_TIME = 10 * 60  # SECONDS


class Queue:
    """
    SIMPLE MULTI-THREADED QUEUE
    """

    def __init__(self, name, max=None, silent=False, unique=False, allow_add_after_close=False):
        """
        max - LIMIT THE NUMBER IN THE QUEUE, IF TOO MANY add() AND extend() WILL BLOCK
        silent - COMPLAIN IF THE READERS ARE TOO SLOW
        unique - SET True IF YOU WANT ONLY ONE INSTANCE IN THE QUEUE AT A TIME
        """
        self.name = name
        self.max = coalesce(max, 2 ** 10)
        self.silent = silent
        self.allow_add_after_close = allow_add_after_close
        self.unique = unique
        self.closed = Signal(f"{name} is closed")
        self.lock = Lock(f"lock for queue {name}")
        self.queue = deque()

    def __iter__(self):
        try:
            while True:
                value = self.pop()
                if value is PLEASE_STOP:
                    break
                if value is not None:
                    yield value
        except Exception as cause:
            logger.warning("Tell me about what happened here", cause)

    def add(self, value, timeout=None, force=False, till=None):
        """
        :param value:  ADDED TO THE QUEUE
        :param till: A `Signal` WHEN TO GIVE UP WAITING FOR SPACE IN THE QUEUE (INSTEAD OF timeout)
        :param timeout:  HOW MANY SECONDS TO WAIT FOR QUEUE TO HAVE SPACE
        :param force:  ADD TO QUEUE, EVEN IF FULL (USE ONLY WHEN CONSUMER IS RETURNING WORK TO THE QUEUE)
        :return: self
        """
        till = till or Till(seconds=coalesce(timeout, DEFAULT_WAIT_TIME))
        with self.lock:
            if value is PLEASE_STOP:
                # INSIDE THE lock SO THAT EXITING WILL RELEASE wait()
                self.closed.go()
                return

            if not force:
                self._wait_for_queue_space(till)
            if not self.unique or value not in self.queue:
                self.queue.append(value)
        return self

    def push(self, value):
        """
        SNEAK value TO FRONT OF THE QUEUE
        """
        with self.lock:
            self._wait_for_queue_space(None)
            self.queue.appendleft(value)
        return self

    def push_all(self, values):
        """
        SNEAK values TO FRONT OF THE QUEUE
        """
        with self.lock:
            self._wait_for_queue_space(None)
            self.queue.extendleft(values)
        return self

    def pop_message(self, till=None):
        """
        RETURN TUPLE (message, payload) CALLER IS RESPONSIBLE FOR CALLING message.delete() WHEN DONE
        DUMMY IMPLEMENTATION FOR DEBUGGING
        """

        return Null, self.pop(till=till)

    def extend(self, values):
        with self.lock:
            # ONCE THE queue IS BELOW LIMIT, ALLOW ADDING MORE
            self._wait_for_queue_space(None)
            if self.unique:
                for v in values:
                    if v is PLEASE_STOP:
                        self.closed.go()
                        continue
                    if v not in self.queue:
                        self.queue.append(v)
            else:
                for v in values:
                    if v is PLEASE_STOP:
                        self.closed.go()
                        continue
                    self.queue.append(v)
        return self

    def _wait_for_queue_space(self, till):
        """
        EXPECT THE self.lock TO BE HAD, WAITS FOR self.queue TO HAVE A LITTLE SPACE

        :param timeout:  IN SECONDS
        """
        (DEBUG and len(self.queue) > 1 * 1000 * 1000) and logger.warning("Queue {name} has over a million items")

        start = time()

        while not self.closed and len(self.queue) >= self.max:
            if till:
                logger.error(THREAD_TIMEOUT, name=self.name)

            if self.silent:
                self.lock.wait(till)
            else:
                self.lock.wait(Till(seconds=5))
                if not till and len(self.queue) >= self.max:
                    now = time()
                    logger.alert(
                        "Queue with name {name|quote} is full with ({num} items),"
                        " thread(s) have been waiting {wait_time} sec",
                        name=self.name,
                        num=len(self.queue),
                        wait_time=now - start,
                    )
        if self.closed and not self.allow_add_after_close:
            logger.error("Do not add to closed queue")

    def __len__(self):
        with self.lock:
            return len(self.queue)

    def __nonzero__(self):
        with self.lock:
            return any(r != PLEASE_STOP for r in self.queue)

    def pop(self, till=None):
        """
        WAIT FOR NEXT ITEM ON THE QUEUE
        RETURN PLEASE_STOP IF QUEUE IS CLOSED
        RETURN None IF till IS REACHED AND QUEUE IS STILL EMPTY

        :param till:  A `Signal` to stop waiting and return None
        :return:  A value, or a PLEASE_STOP or None
        """
        with self.lock:
            while True:
                if self.queue:
                    return self.queue.popleft()
                if self.closed:
                    break
                if not self.lock.wait(till=self.closed | till):
                    if self.closed:
                        break
                    return None
        (DEBUG or not self.silent) and logger.info("{name} queue closed", name=self.name, stack_depth=1)
        return PLEASE_STOP

    def pop_all(self):
        """
        NON-BLOCKING POP ALL IN QUEUE, IF ANY
        """
        with self.lock:
            output = list(self.queue)
            self.queue.clear()

        return output

    def pop_one(self):
        """
        NON-BLOCKING POP IN QUEUE, IF ANY
        """
        with self.lock:
            if self.closed:
                return PLEASE_STOP
            elif not self.queue:
                return None
            else:
                v = self.queue.popleft()
                if v is PLEASE_STOP:  # SENDING A STOP INTO THE QUEUE IS ALSO AN OPTION
                    self.closed.go()
                return v

    def close(self):
        self.closed.go()

    def commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # python queue.Queue
    def qsize(self):
        with self.lock:
            return len(self.queue)

    def empty(self):
        with self.lock:
            return not bool(self.queue)

    def full(self):
        with self.lock:
            return len(self.queue) >= self.max

    def put(self, item, block=True, timeout=None):
        if block:
            try:
                self.add(item, timeout=timeout, force=not block)
                return
            except Exception as cause:
                if THREAD_TIMEOUT in cause:
                    raise Full()
                raise
        self.put_nowait(item)

    def put_nowait(self, item):
        self.add(item, force=True)

    def get(self, block=True, timeout=None):
        if block:
            if timeout is None:
                return self.pop()
            else:
                till = Till(seconds=timeout)
                value = self.pop(till)
                if value is None:
                    raise Empty()
                return value
        return self.get_nowait()

    def get_nowait(self):
        value = self.pop(True)
        if value is None:
            raise Empty()
        return value

    def task_done(self):
        pass

    def join(self, till=None):
        """
        WAIT FOR ALL ITEMS TO BE PROCESSED
        DIFFERS FROM PYTHON queue.Queue IN THAT IT DOES NOT WAIT FOR task_done()
        """
        self.closed.wait(till)
        with self.lock:
            while not till:
                if not self.queue:
                    break
                self.lock.wait(till)
        return self


class PriorityQueue(Queue):
    """
    ADDS ITEMS TO THEIR PRIORITY AND POP'S THE HIGHEST PRIORITY VALUE (UNLESS REQUESTED OTHERWISE)
    """

    def __init__(
        self, name, numpriorities, max=None, silent=False, unique=False, allow_add_after_close=False,
    ):
        Queue.__init__(
            self, name=name, max=max, silent=silent, unique=False, allow_add_after_close=False,
        )

        self.numpriorities = numpriorities
        self.queue = [
            Queue(name=name, max=max, silent=silent, unique=False, allow_add_after_close=False,)
            for _ in range(numpriorities)
        ]

    def __iter__(self):
        try:
            while True:
                value = self.pop(self.closed)
                if value is PLEASE_STOP:
                    break
                if value is not None:
                    yield value
        except Exception as cause:
            logger.warning("Tell me about what happened here", cause)

        if not self.silent:
            logger.info("queue iterator is done")

    def add(self, value, timeout=None, priority=0, till=None):
        till = till or Till(seconds=coalesce(timeout, DEFAULT_WAIT_TIME))
        with self.lock:
            if value is PLEASE_STOP:
                # INSIDE THE lock SO THAT EXITING WILL RELEASE wait()
                self.queue[priority].queue.append(value)
                self.closed.go()
                return

            self.queue[priority]._wait_for_queue_space(till)
            if self.unique:
                if value not in self.queue[priority].queue:
                    self.queue[priority].queue.append(value)
            else:
                self.queue[priority].queue.append(value)
        return self

    def push(self, value, priority=0):
        """
        SNEAK value TO FRONT OF THE QUEUE
        """
        if self.closed and not self.queue[priority].allow_add_after_close:
            logger.error("Do not push to closed queue")
        with self.lock:
            self.queue[priority]._wait_for_queue_space(None)
            self.queue[priority].queue.appendleft(value)
        return self

    def __len__(self):
        with self.lock:
            return sum([len(q.queue) for q in self.queue])

    def __nonzero__(self):
        with self.lock:
            return any(any(r != PLEASE_STOP for r in q.queue) for q in self.queue)

    def highest_entry(self):
        for count, q in enumerate(self.queue):
            if len(q) > 0:
                return count
        return None

    def pop(self, till=None, priority=None):
        """
        WAIT FOR NEXT ITEM ON THE QUEUE
        RETURN PLEASE_STOP IF QUEUE IS CLOSED
        RETURN None IF till IS REACHED AND QUEUE IS STILL EMPTY

        :param till:  A `Signal` to stop waiting and return None
        :return:  A value, or a PLEASE_STOP or None
        """
        if till is not None and not isinstance(till, Signal):
            logger.error("expecting a signal")

        with self.lock:
            while True:
                if not priority:
                    priority = self.highest_entry()
                if priority:
                    value = self.queue[priority].queue.popleft()
                    return value
                if self.closed:
                    break
                if not self.lock.wait(till=till | self.closed):
                    if self.closed:
                        break
                    return None
        (DEBUG or not self.silent) and logger.info(self.name + " queue stopped")
        return PLEASE_STOP

    def pop_all(self, priority=None):
        """
        NON-BLOCKING POP ALL IN QUEUE, IF ANY
        """
        output = []
        with self.lock:
            if not priority:
                priority = self.highest_entry()
            if priority:
                output = list(self.queue[priority].queue)
                self.queue[priority].queue.clear()
        return output

    def pop_all_queues(self):
        """
        NON-BLOCKING POP ALL IN QUEUE, IF ANY
        """
        output = []
        with self.lock:
            for q in self.queue:
                output.extend(list(q.queue))
                q.queue.clear()

        return output

    def pop_one(self, priority=None):
        """
        NON-BLOCKING POP IN QUEUE, IF ANY
        """
        with self.lock:
            if not priority:
                priority = self.highest_entry()
            if self.closed:
                return [PLEASE_STOP]
            elif not self.queue:
                return None
            else:
                v = self.pop(priority=priority)
                if v is PLEASE_STOP:  # SENDING A STOP INTO THE QUEUE IS ALSO AN OPTION
                    self.closed.go()
                return v


class ThreadedQueue(Queue):
    """
    DISPATCH TO ANOTHER (SLOWER) queue IN BATCHES OF GIVEN size
    TODO: Check that this queue is not dropping items at shutdown
    """

    def __init__(
        self,
        name,
        slow_queue,  # THE SLOWER QUEUE
        batch_size=None,  # THE MAX SIZE OF BATCHES SENT TO THE SLOW QUEUE
        max_size=None,  # SET THE MAXIMUM SIZE OF THE QUEUE, WRITERS WILL BLOCK IF QUEUE IS OVER THIS LIMIT
        period=None,  # MAX TIME (IN SECONDS) BETWEEN FLUSHES TO SLOWER QUEUE
        silent=False,  # WRITES WILL COMPLAIN IF THEY ARE WAITING TOO LONG
        error_target=None  # CALL error_target(error, buffer) **buffer IS THE LIST OF OBJECTS ATTEMPTED**
        # BE CAREFUL!  THE THREAD MAKING THE CALL WILL NOT BE YOUR OWN!
        # DEFAULT BEHAVIOUR: THIS WILL KEEP RETRYING WITH WARNINGS
    ):
        if period != None and not isinstance(period, (int, float)):
            logger.error("Expecting a float for the period")
        period = coalesce(period, 1)  # SECONDS
        batch_size = coalesce(batch_size, int(max_size / 2) if max_size else None, 900)
        max_size = coalesce(max_size, batch_size * 2)  # REASONABLE DEFAULT

        Queue.__init__(self, name=name, max=max_size, silent=silent)

        self.name = name
        self.slow_queue = slow_queue
        self.thread = (
            Thread
            .run(f"threaded queue for {name}", self.worker_bee, batch_size, period, error_target, parent_thread=self)
            .release()
        )

    def worker_bee(self, batch_size, period, error_target, please_stop):
        please_stop.then(lambda: self.add(PLEASE_STOP))

        _buffer = []
        _post_push_functions = []
        now = time()
        next_push = Till(till=now + period)  # THE TIME WE SHOULD DO A PUSH
        last_push = now - period

        def push_to_queue():
            if self.slow_queue.__class__.__name__ == "Index":
                if self.slow_queue.settings.index.startswith("saved"):
                    logger.alert("INSERT SAVED QUERY {data|json}", data=copy(_buffer))
            self.slow_queue.extend(_buffer)
            del _buffer[:]
            for ppf in _post_push_functions:
                ppf()
            del _post_push_functions[:]

        while not please_stop:
            try:
                if not _buffer:
                    item = self.pop()
                    now = time()
                    if now > last_push + period:
                        next_push = Till(till=now + period)
                else:
                    item = self.pop(till=next_push)
                    now = time()

                if item is PLEASE_STOP:
                    push_to_queue()
                    please_stop.go()
                    break
                elif isinstance(item, types.FunctionType):
                    _post_push_functions.append(item)
                elif item is not None:
                    _buffer.append(item)
            except Exception as cause:
                cause = Except.wrap(cause)
                if error_target:
                    try:
                        error_target(cause, _buffer)
                    except Exception as f:
                        logger.warning(
                            "`error_target` should not throw, just deal", name=self.name, cause=f,
                        )
                else:
                    logger.warning("Unexpected problem", name=self.name, cause=cause)

            try:
                if len(_buffer) >= batch_size or next_push:
                    if _buffer:
                        push_to_queue()
                        last_push = now = time()
                    next_push = Till(till=now + period)
            except Exception as cause:
                cause = Except.wrap(cause)
                if error_target:
                    try:
                        error_target(cause, _buffer)
                    except Exception as f:
                        logger.warning(
                            "`error_target` should not throw, just deal", name=self.name, cause=f,
                        )
                else:
                    logger.warning(
                        "Problem with {name} pushing {num} items to data sink",
                        name=self.name,
                        num=len(_buffer),
                        cause=cause,
                    )

        if _buffer:
            # ONE LAST PUSH, DO NOT HAVE TIME TO DEAL WITH ERRORS
            push_to_queue()
        self.slow_queue.add(PLEASE_STOP)

    def add_child(self, child):
        pass

    def add(self, value, timeout=None, till=None):
        till = till or Till(seconds=coalesce(timeout, DEFAULT_WAIT_TIME))
        with self.lock:
            self._wait_for_queue_space(till)
            self.queue.append(value)
        return self

    def extend(self, values, till=None):
        till = till or Till(seconds=DEFAULT_WAIT_TIME)
        with self.lock:
            # ONCE THE queue IS BELOW LIMIT, ALLOW ADDING MORE
            self._wait_for_queue_space(till)
            self.queue.extend(values)
            if not self.silent:
                logger.info("{name} has {num} items", name=self.name, num=len(self.queue))
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.add(PLEASE_STOP)
        if isinstance(exc_val, BaseException):
            self.thread.please_stop.go()
        self.thread.join()

    def stop(self):
        self.add(PLEASE_STOP)
        self.thread.join()
        return self
