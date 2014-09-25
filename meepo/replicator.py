# -*- coding: utf-8 -*-

"""
    meepo.replicator
    ~~~~~~~~~~~~~~~~

    Meepo Replicators based on events.

    Replicate from database to targets.
"""

import collections
import logging
import time

from multiprocessing import Process, Queue

import zmq
from zmq.utils.strtypes import asbytes

from .utils import ConsistentHashRing


class Worker(Process):
    MAX_PK_COUNT = 256

    def __init__(self, name, queue, cb, multi=False, logger_name=None,
                 retry=True, max_retry_count=10, max_retry_interval=60):
        """kwargs:
        multi: allow multiple pks to be sent in one callback.
        retry: retry on pk if callback failed.
        max_retry_count: max retry count for a single pk.
        max_retry_interval: max sleep time when callback failed.
        """
        super(Worker, self).__init__()
        self.name = name
        self.queue = queue
        self.cb = cb
        self.multi = multi
        self.retry = retry

        # config logger
        logger_name = logger_name or "name-%s" % id(self)
        self.logger = logging.getLogger(logger_name)

        # config retry
        self._max_retry_interval = max_retry_interval
        self._max_retry_count = max_retry_count
        self._retry_stats = collections.Counter()

    def run(self):
        try:
            while True:
                pks = set()

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

                self.logger.info("{0} -> {1} - qsize: {2}".format(
                    self.name, pks, self.queue.qsize()))

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
                    time.sleep(min(3 * sum(results), self._max_retry_interval))

        except KeyboardInterrupt:
            self.logger.debug("KeyboardInterrupt stop %s" % self.name)

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


class ZmqReplicator(object):
    """Replicator base class.

    Trigger retrive, process, store steps to do replication.
    """

    def __init__(self, listen, name="meepo.replicator.zmq"):
        self.listen = listen
        self.workers = {}
        self.worker_queues = {}

        # replicator logger naming
        self.name = name
        self.logger = logging.getLogger(name)

        # init zmq socket
        self._ctx = zmq.Context()
        self.socket = self._ctx.socket(zmq.SUB)

    def event(self, *topics, **kwargs):
        """Topic callback registry.

        callback func should receive two args: topic and pk, and then process
        the replication job.
        """
        workers = kwargs.pop("workers", 1)
        multi = kwargs.pop("multi", False)

        def wrapper(func):
            for topic in topics:
                queues = [Queue() for _ in range(workers)]
                hash_ring = ConsistentHashRing()
                for q in queues:
                    hash_ring[hash(q)] = q
                self.worker_queues[topic] = hash_ring
                self.workers[topic] = [
                    Worker(topic, q, func, multi=multi,
                           logger_name="%s.%s" % (self.name, topic))
                    for q in queues]
                self.socket.setsockopt(zmq.SUBSCRIBE, asbytes(topic))
            return func
        return wrapper

    def run(self):
        """Run replicator.

        Main process receive messages and distribute them to worker queues.
        """
        for workers in self.workers.values():
            for w in workers:
                w.start()

        self.socket.connect(self.listen)
        while True:
            msg = self.socket.recv_string()
            lst = msg.split()
            if len(lst) == 2:
                topic, pks = lst[0], [lst[1], ]
            elif len(lst) > 2:
                topic, pks = lst[0], lst[1:]
            else:
                self.logger.error("msg corrupt -> %s" % msg)
                return

            self.logger.debug("replicator: {0} -> {1}".format(topic, pks))
            for pk in pks:
                self.worker_queues[topic][hash(pk)].put(pk)
