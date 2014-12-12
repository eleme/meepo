# -*- coding: utf-8 -*-

from __future__ import absolute_import

import collections
import logging
import os
import signal
import time

from multiprocessing import Process

from ..._compat import Empty


def _deduplicate(queue, max_size):
    items = []
    for i in range(0, max_size):
        try:
            items.append(queue.get_nowait())
        except Empty:
            break
    items = set(items)
    for item in items:
        queue.put(item)


class Worker(Process):
    """Worker process

    The worker wraps the user supplied callback func. It uses multiprocessing
    queue as the task queue and will retry when a task failed.
    """
    MAX_PK_COUNT = 256

    def __init__(self, queue, name, cb, multi=False, logger_name=None,
                 retry=True, queue_limit=10000, max_retry_count=10,
                 max_retry_interval=60):
        """
        :param multi: allow multiple pks to be sent in one callback
        :param retry: retry on pk if callback failed
        :param queue_limit: queue size limit for deduplication
        :param max_retry_count: max retry count for a single pk
        :param max_retry_interval: max sleep time when callback failed
        """
        super(Worker, self).__init__()
        self.name = name
        self.queue = queue
        self.queue_limit = queue_limit
        self.cb = cb
        self.multi = multi
        self.retry = retry

        # config logger
        logger_name = logger_name or "name-%s" % id(self)
        self.logger = logging.getLogger(logger_name)
        self.logger.debug("worker %s initing..." % self.name)

        # config retry
        self._max_retry_interval = max_retry_interval
        self._max_retry_count = max_retry_count
        self._retry_stats = collections.Counter()

    def run(self):
        self.logger.debug("worker %s running..." % self.name)
        while True:
            try:
                pks = set()

                try:
                    max_size = self.queue.qsize()
                    if max_size > self.queue_limit:
                        self.logger.info("worker %s deduplicating" % self.name)
                        _deduplicate(self.queue, max_size)
                except NotImplementedError:
                    pass

                # try get all pks from queue at once
                while not self.queue.empty():
                    pks.add(self.queue.get())
                    if len(pks) > self.MAX_PK_COUNT:
                        break

                # take a nap if queue is empty
                if not pks:
                    time.sleep(1)
                    continue

                # keep order to match the results
                pks = list(pks)

                try:
                    # Mac / UNIX don't support qsize
                    self.logger.info("{0} -> {1} - qsize: {2}".format(
                        self.name, pks, self.queue.qsize()))
                except NotImplementedError:
                    self.logger.info("{0} -> {1}".format(self.name, pks))

                if self.multi:
                    results = self.cb(pks)
                else:
                    results = [self.cb(pk) for pk in pks]

                if not self.retry:
                    continue

                # check failed task and retry
                for pk, r in zip(pks, results):
                    if r:
                        self.on_success(pk)
                    else:
                        self.on_fail(pk)

                # take a nap on fail
                if not all(results):
                    time.sleep(min(3 * results.count(False),
                                   self._max_retry_interval))

            except KeyboardInterrupt:
                self.logger.debug("KeyboardInterrupt stop %s" % self.name)
                break

            except Exception as e:
                self.logger.exception(e)
                time.sleep(10)

    def on_fail(self, pk):
        self._retry_stats[pk] += 1
        if self._retry_stats[pk] > self._max_retry_count:
            del self._retry_stats[pk]
            self.logger.error("callback on pk failed -> %s" % pk)
        else:
            # put failed pk back to queue
            self.queue.put(pk)
            self.logger.warn(
                "callback on pk failed for %s times -> %s" % (
                    self._retry_stats[pk], pk))

    def on_success(self, pk):
        if pk in self._retry_stats:
            del self._retry_stats[pk]


class WorkerPool(object):
    """Manage a set of workers and recreate worker when worker dead.
    """
    def __init__(self, queues, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._queues = queues

        self._sentinel_worker = None
        self.waiting_time = kwargs.pop("waiting_time", 10)

    def _make_worker(self, queue):
        return Worker(queue, *self._args, **self._kwargs)

    def terminate(self):
        os.kill(self._sentinel_worker.pid, signal.SIGINT)
        self._sentinel_worker.join()

    def start(self):
        logger = logging.getLogger("meepo.replicator.sentinel")

        def _f():
            worker_map = {
                q: self._make_worker(q) for q in self._queues
            }
            for _, worker in worker_map.items():
                worker.start()

            logger.info("starting sentinel...")
            try:
                while True:
                    logger.debug("ping {} worker".format(self._args[0]))
                    dead = qsize = 0
                    for queue, worker in worker_map.items():
                        try:
                            qsize += queue.qsize()
                        except NotImplementedError:
                            qsize = None

                        if not worker.is_alive():
                            dead += 1
                            logger.warn(
                                "{} worker {} dead, recreating...".format(
                                    self._args[0], worker.pid))

                            worker_map[queue] = self._make_worker(queue)
                            worker_map[queue].start()

                    msg = ["{} total qsize {}".format(self._args[0], qsize),
                           "{} worker alive, {} worker dead".format(
                               len(worker_map) - dead, dead)]

                    logger.info("; ".join(msg))

                    time.sleep(self.waiting_time)
            except KeyboardInterrupt:
                pass
            finally:
                for worker in worker_map.values():
                    worker.terminate()

        self._sentinel_worker = Process(target=_f)
        self._sentinel_worker.start()
