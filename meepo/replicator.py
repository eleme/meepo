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
    MAX_INTERVAL = 60

    def __init__(self, name, queue, cb, logger_name=None):
        super(Worker, self).__init__()
        self.name = name
        self.queue = queue
        self.cb = cb

        if logger_name:
            self.logger = logging.getLogger(logger_name)

        self._interval = 1

    def run(self):
        try:
            for pk in iter(self.queue.get, None):
                self.logger.info("{0} -> {1} - qsize: {2}".format(
                    self.name, pk, self.queue.qsize()))

                if not self.cb(pk):
                    self.queue.put(pk)
                    self._sleep()
                else:
                    self._interval = 1
        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt stop %s" % self.name)

    def _sleep(self):
        time.sleep(self._interval)
        self._interval = min(self._interval + 2, self.MAX_INTERVAL)


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
