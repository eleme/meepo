# -*- coding: utf-8 -*-

"""
    meepo.replicator
    ~~~~~~~~~~~~~~~~

    Meepo Replicators based on events.

    Replicate from database to targets.
"""

import logging
import time

from multiprocessing import Process, Queue

import zmq
from zmq.utils.strtypes import asbytes

from .utils import ConsistentHashRing


class Worker(Process):
    def __init__(self, name, queue, cb, logger_name=None,
                 max_retry_count=10, max_retry_interval=60):
        super(Worker, self).__init__()
        self.name = name
        self.queue = queue
        self.cb = cb

        # config logger
        logger_name = logger_name or "name-%s" % id(self)
        self.logger = logging.getLogger(logger_name)

        # config retry
        self._max_retry_interval = max_retry_interval
        self._max_retry_count = max_retry_count
        self._retry_stats = {}

    def run(self):
        try:
            for pk in iter(self.queue.get, None):
                self.logger.info("{0} -> {1} - qsize: {2}".format(
                    self.name, pk, self.queue.qsize()))

                if not self.cb(pk):
                    self.on_fail(pk)
                else:
                    self.on_success(pk)

        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt stop %s" % self.name)

    def on_fail(self, pk):
        if pk not in self._retry_stats:
            self._retry_stats[pk] = 0

        if self._retry_stats[pk] > self._max_retry_count:
            self.logger.error("callback on pk failed -> %s" % pk)
            del self._retry_stats[pk]
            return

        # sleep on fail
        time.sleep(min(2 ** self._retry_stats[pk], self._max_retry_interval))

        self._retry_stats[pk] += 1
        self.queue.put(pk)
        self.logger.warn("callback on pk failed for %s times -> %s" % (
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

        def wrapper(func):
            for topic in topics:
                queues = [Queue() for _ in range(workers)]
                hash_ring = ConsistentHashRing()
                for q in queues:
                    hash_ring[hash(q)] = q
                self.worker_queues[topic] = hash_ring
                self.workers[topic] = [Worker(
                    topic, q, func, logger_name=self.name) for q in queues]
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
            topic, pk = msg.split()
            self.logger.debug("replicator: {0} -> {1}".format(topic, pk))
            self.worker_queues[topic][hash(pk)].put(pk)
