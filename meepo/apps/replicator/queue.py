# -*- coding: utf-8 -*-

from __future__ import absolute_import

import ketama
import zmq

from multiprocessing import Queue
from zmq.utils.strtypes import asbytes

from . import Replicator
from .worker import WorkerPool


class QueueReplicator(Replicator):
    """Replicator using Queue as worker task queue

    This Replicator receives events from upstream zmq devices and put them into
    a set of python multiprocessing queues using ketama consistent hashing.
    Each queue has a worker. We use :class:`WorkerPool` to manage a set of
    queues.
    """

    def __init__(self, *args, **kwargs):
        super(QueueReplicator, self).__init__(*args, **kwargs)

        # init workers
        self.workers = {}
        self.worker_queues = {}

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
