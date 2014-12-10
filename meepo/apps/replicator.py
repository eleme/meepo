# -*- coding: utf-8 -*-

"""Meepo Replicators based on events.
"""

import collections
import logging
import time
import ketama
import zmq
import os
import signal

from multiprocessing import Process, Queue
from zmq.utils.strtypes import asbytes

from .._compat import Empty

zmq_ctx = zmq.Context()


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


class Replicator(object):
    """Replicator base class.
    """
    def __init__(self, name="meepo.replicator.zmq"):
        # replicator logger naming
        self.name = name
        self.logger = logging.getLogger(name)

    def run(self):
        raise NotImplementedError()


class QueueReplicator(Replicator):
    """Replicator using Queue as worker task queue

    This Replicator receives events from upstream zmq devices and put them into
    a set of python multiprocessing queues using ketama consistent hashing.
    Each queue has a worker. We use :class:`WorkerPool` to manage a set of
    queues.
    """

    def __init__(self, listen=None, **kwargs):
        """
        :param listen: zeromq dsn to connect, can be a list
        """
        super(QueueReplicator, self).__init__(**kwargs)

        self.listen = listen

        # init workers
        self.workers = {}
        self.worker_queues = {}

        # init zmq socket
        self.socket = zmq_ctx.socket(zmq.SUB)

    def event(self, *topics, **kwargs):
        """Topic callback registry.

        callback func should receive two args: topic and pk, and then process
        the replication job.

        Note: The callback func must return True/False. When passed a list of
        pks, the func should return a list of True/False with the same length
        of pks.

        :param topics: a list of topics
        :param workers: how many workers to process this topic
        :param multi: whether pass multiple pks
        :param queue_limit: when queue size is larger than the limit,
        the worker should run deduplicate procedure
        """
        workers = kwargs.pop("workers", 1)
        multi = kwargs.pop("multi", False)
        queue_limit = kwargs.pop("queue_limit", 10000)

        def wrapper(func):
            for topic in topics:
                queues = [Queue() for _ in range(workers)]
                hash_ring = ketama.Continuum()
                for q in queues:
                    hash_ring[str(hash(q))] = q
                self.worker_queues[topic] = hash_ring
                self.workers[topic] = WorkerPool(
                    queues, topic, func, multi=multi, queue_limit=queue_limit,
                    logger_name="%s.%s" % (self.name, topic))
                self.socket.setsockopt(zmq.SUBSCRIBE, asbytes(topic))
            return func
        return wrapper

    def run(self):
        """Run the replicator.

        Main process receive messages and distribute them to worker queues.
        """
        for worker_pool in self.workers.values():
            worker_pool.start()

        if isinstance(self.listen, list):
            for i in self.listen:
                self.socket.connect(i)
        else:
            self.socket.connect(self.listen)

        try:
            while True:
                msg = self.socket.recv_string()
                lst = msg.split()
                if len(lst) == 2:
                    topic, pks = lst[0], [lst[1], ]
                elif len(lst) > 2:
                    topic, pks = lst[0], lst[1:]
                else:
                    self.logger.error("msg corrupt -> %s" % msg)
                    continue

                self.logger.debug("replicator: {0} -> {1}".format(topic, pks))
                for pk in pks:
                    self.worker_queues[topic][str(hash(pk))].put(pk)
        except Exception as e:
            self.logger.exception(e)
        finally:
            for worker_pool in self.workers.values():
                worker_pool.terminate()
