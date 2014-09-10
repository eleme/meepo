# -*- coding: utf-8 -*-

"""
    meepo.replicator
    ~~~~~~~~~~~~~~~~

    Meepo Replicators based on events.

    Replicate from database to targets.
"""

import bisect
import hashlib
import logging
from multiprocessing import Process, Queue

import zmq
from zmq.utils.strtypes import asbytes


class Worker(Process):
    def __init__(self, name, queue, cb, logger_name=None):
        super(Worker, self).__init__()
        self.name = name
        self.queue = queue
        self.cb = cb

        if logger_name:
            self.logger = logging.getLogger(logger_name)

    def run(self):
        for pk in iter(self.queue.get, None):
            self.logger.info("{0} -> {1} - qsize: {2}".format(
                self.name, pk, self.queue.qsize()))
            self.cb(pk)


class ZmqReplicator(object):
    """Replicator base class.

    Trigger retrive, process, store steps to do replication.
    """

    def __init__(self, listen=None, name="meepo.replicator.zmq"):
        self.listen = listen
        self.workers = {}
        self.worker_queues = {}

        # replicator logger naming
        self.name = name
        self.logger = logging.getLogger(name)

        # init zmq socket
        self._ctx = zmq.Context()
        self.socket = self._ctx.socket(zmq.SUB)

    def set_forwarder(self, forwarder):
        self.listen = forwarder

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


class ConsistentHashRing(object):
    def __init__(self, replicas=100):
        self.replicas = replicas
        self._keys = []
        self._nodes = {}

    def _hash(self, key):
        """Given a string key, return a hash value."""

        key = str(key)
        return int(hashlib.md5(str.encode(key)).hexdigest(), 16)

    def _repl_iterator(self, nodename):
        """Given a node name, return an iterable of replica hashes."""

        return (self._hash("%s:%s" % (nodename, i))
                for i in range(self.replicas))

    def __setitem__(self, nodename, node):
        for hash_ in self._repl_iterator(nodename):
            if hash_ in self._nodes:
                raise ValueError("Node name %r is already present" % nodename)
            self._nodes[hash_] = node
            bisect.insort(self._keys, hash_)

    def __delitem__(self, nodename):
        """Remove a node, given its name."""

        for hash_ in self._repl_iterator(nodename):
            # will raise KeyError for nonexistent node name
            del self._nodes[hash_]
            index = bisect.bisect_left(self._keys, hash_)
            del self._keys[index]

    def __getitem__(self, key):
        hash_ = self._hash(key)
        start = bisect.bisect(self._keys, hash_)
        if start == len(self._keys):
            start = 0
        return self._nodes[self._keys[start]]
