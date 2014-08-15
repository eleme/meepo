# -*- coding: utf-8 -*-

"""
    meepo.replicator
    ~~~~~~~~~~~~~~~~

    Meepo Replicators based on events.

    Replicate from database to targets.
"""

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
            self.logger.info("{0} -> {1}".format(self.name, pk))
            self.cb(pk)


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

    def event(self, *topics):
        """Topic callback registry.

        callback func should receive two args: topic and pk, and then process
        the replication job.
        """
        def wrapper(func):
            for topic in topics:
                self.worker_queues[topic] = Queue()
                self.workers[topic] = Worker(
                    topic, self.worker_queues[topic], func,
                    logger_name=self.name)
                self.socket.setsockopt(zmq.SUBSCRIBE, asbytes(topic))
            return func
        return wrapper

    def run(self):
        """Run replicator.

        Main process receive messages and distribute them to worker queues.
        """
        for w in self.workers.values():
            w.start()

        self.socket.connect(self.listen)
        while True:
            msg = self.socket.recv_string()
            topic, pk = msg.split()
            self.logger.debug("replicator: {0} -> {1}".format(topic, pk))
            self.worker_queues[topic].put(pk)
