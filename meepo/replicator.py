# -*- coding: utf-8 -*-

"""
    meepo.replicator
    ~~~~~~~~~~~~~~~~

    Meepo Replicators based on events.

    Replicate from database to targets.
"""

import itertools
from multiprocessing import Process, Queue

import zmq
from zmq.utils.strtypes import asbytes


def replicate(topic, pk_queue, callback):
    while True:
        pk = pk_queue.get()
        callback(topic, pk)


class Replicator(object):
    """Replicator base class.

    Trigger retrive, process, store steps to do replication.
    """

    def __init__(self, listen, tables):
        assert isinstance(tables, (tuple, set, list)) and len(tables)

        # subscribe to events
        self._ctx = zmq.Context()
        self.socket = self._ctx.socket(zmq.SUB)
        self.socket.connect(listen)

        self._topics = ["{0}_{1}".format(tpc[0], tpc[1])
                        for tpc in itertools.product(
                        tables, ('write', 'update', 'delete'))]
        for tpc in self._topics:
            self.socket.setsockopt(zmq.SUBSCRIBE, asbytes(tpc))

        # callback registry
        self._callbacks = {}

    def callback(self, topic):
        """Topic callback registry.

        callback func should receive two args: topic and pk, and then process
        the replication job.
        """
        def wrapper(func):
            self._callbacks[topic] = func
            return func
        return wrapper

    def _run_workers(self):
        """Start workers for replication.
        Each topic starts a worker to process it's events.
        """
        # workers registry
        self._workers = {}
        self._worker_queues = {}
        for tpc in self._topics:
            q = Queue()

            # start worker
            cb = self._callbacks[tpc]
            p = Process(name=tpc, target=replicate, args=(tpc, q, cb))
            p.start()

            self._workers[tpc] = p
            self._worker_queues[tpc] = q

    def run(self):
        """Run replicator.

        Main process receive messages and distribute them to worker queues.
        """
        self._run_workers()

        while True:
            msg = self.socket.recv_string()
            topic, pk = msg.split()
            self._worker_queues[topic].put(pk)
