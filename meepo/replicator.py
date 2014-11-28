# -*- coding: utf-8 -*-

"""
    meepo.replicator
    ~~~~~~~~~~~~~~~~

    Meepo Replicators based on events.

    Replicate from database to targets.
"""

import collections
import logging
import redis
import time
import ketama

from multiprocessing import Process, Queue
import zmq
from zmq.utils.strtypes import asbytes

from ._compat import Empty

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
        """kwargs:
        multi: allow multiple pks to be sent in one callback.
        retry: retry on pk if callback failed.
        max_retry_count: max retry count for a single pk.
        max_retry_interval: max sleep time when callback failed.
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
    def __init__(self, queues, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._queues = queues

        self._sentinel_worker = None

    def _make_worker(self, queue):
        return Worker(queue, *self._args, **self._kwargs)

    def terminate(self):
        self._sentinel_worker.terminate()

    def start(self):
        logger = logging.getLogger("meepo.replicator.sentinel")

        def _f():
            worker_map = {
                q: self._make_worker(q) for q in self._queues
            }
            for _, worker in worker_map.items():
                worker.start()

            logger.info("starting sentinel...")
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
                        logger.warn("{} worker {} dead, recreating...".format(
                            self._args[0], worker.pid))

                        worker_map[queue] = self._make_worker(queue)
                        worker_map[queue].start()

                msg = ["{} total qsize {}".format(self._args[0], qsize),
                       "{} worker alive, {} worker dead".format(
                           len(worker_map) - dead, dead)]

                logger.info("; ".join(msg))

                time.sleep(10)

        self._sentinel_worker = Process(target=_f)
        self._sentinel_worker.start()


class Replicator(object):
    def __init__(self, name="meepo.replicator.zmq"):
        # replicator logger naming
        self.name = name
        self.logger = logging.getLogger(name)

    def run(self):
        raise NotImplementedError()


class ZmqReplicator(Replicator):
    """Replicator base class.

    Trigger retrieve, process, store steps to do replication.
    """

    def __init__(self, listen=None, **kwargs):
        super(ZmqReplicator, self).__init__(**kwargs)

        self.listen = listen

        # init workers
        self.workers = {}
        self.worker_queues = {}

        self.topics = set()

    def _pub_socket(self):
        socket = zmq_ctx.socket(zmq.SUB)
        for topic in self.topics:
            socket.setsockopt(zmq.SUBSCRIBE, topic)

        if isinstance(self.listen, list):
            for i in self.listen:
                socket.connect(i)
        else:
            socket.connect(self.listen)
        return socket

    def event(self, *topics, **kwargs):
        """Topic callback registry.

        callback func should receive two args: topic and pk, and then process
        the replication job.
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
                self.topics.add(asbytes(topic))
            return func
        return wrapper

    def run(self, reconnect_time=600, socket_timeout=10):
        """Run zmq replicator.

        Main process receive messages and distribute them to worker queues.
        """
        poller = zmq.Poller()

        for worker_pool in self.workers.values():
            worker_pool.start()

        start = time.time()
        socket = self._pub_socket()

        poller.register(socket)

        try:
            while True:
                sockets = poller.poll(timeout=socket_timeout * 1000)
                if not sockets:
                    continue

                if time.time() - start >= reconnect_time:
                    socket_compl = self._pub_socket()

                msg = socket.recv_string()

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

                if time.time() - start >= reconnect_time:
                    self.logger.info("reconnecting")

                    start = time.time()

                    poller.unregister(socket)
                    socket = socket_compl
                    poller.register(socket)

        except Exception as e:
            self.logger.exception(e)
        finally:
            for worker_pool in self.workers.values():
                worker_pool.terminate()


class RedisCacheReplicator(Replicator):
    def __init__(self, listen, redis_dsn, namespace=None, **kwargs):
        super(RedisCacheReplicator, self).__init__(**kwargs)

        self.listen = listen
        self.redis_dsn = redis_dsn
        self.namespace = namespace or "meepo.repl.rcache"

        self.r = redis.Redis.from_url(self.redis_dsn)

        # init workers
        self.workers = {}
        self.update_queues = {}
        self.delete_queues = {}

        # init zmq socket
        self.socket = zmq_ctx.socket(zmq.SUB)

    def _cache_update_gen(self, table, serializer, multi=False):
        def cache_update_multi(pks):
            keys = ["%s:%s:%s" % (self.namespace, table, p) for p in pks]
            self.logger.debug("cache update -> %s:%s" % (table, pks))
            return [self.r.mset(dict(zip(keys, serializer(pks))))] * len(pks)

        def cache_update(pk):
            key = "%s:%s:%s" % (self.namespace, table, pk)
            self.logger.debug("cache update -> %s:%s" % (table, pk))
            return self.r.set(key, serializer(pk))
        return cache_update_multi if multi else cache_update

    def _cache_delete_gen(self, table):
        def cache_delete(pks):
            keys = set("%s:%s:%s" % (self.namespace, table, p) for p in pks)
            self.logger.debug("cache delete -> %s:%s" % (table, pks))
            return [self.r.delete(*keys) >= 0] * len(pks)
        return cache_delete

    def cache(self, *tables, **kwargs):
        """Table cache serializer registry.

        serializer callback should receive two args: topic and pk, and then
        return the serialized bytes.
        """
        workers = kwargs.pop("workers", 1)
        multi = kwargs.pop("multi", False)

        def wrapper(func):
            for table in tables:
                # hash ring for cache update
                queues = [Queue() for _ in range(workers)]
                hash_ring = ketama.Continuum()
                for q in queues:
                    hash_ring[str(hash(q))] = q
                self.update_queues[table] = hash_ring

                cache_update = self._cache_update_gen(table, func, multi=multi)
                self.workers[table] = [
                    Worker("%s_cache_update" % table, q, cache_update,
                           multi=multi,
                           logger_name="%s.%s" % (self.name, table))
                    for q in queues]

                # single worker for cache delete
                delete_q = Queue()
                self.delete_queues[table] = delete_q
                cache_delete = self._cache_delete_gen(table)
                self.workers[table].append(
                    Worker("%s_cache_delete" % table, delete_q, cache_delete,
                           multi=True,
                           logger_name="%s.%s" % (self.name, table)))

                self.socket.setsockopt(zmq.SUBSCRIBE, asbytes(table))
            return func
        return wrapper

    def run(self):
        """Run redis cache replicator.

        Main process receive messages, process and distribute them to worker
        queues.
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
                continue

            self.logger.debug("redis cache replicator: {0} -> {1}".format(
                topic, pks))
            table, action = topic.rsplit('_', 1)
            if action in ("write", "update"):
                for pk in pks:
                    self.update_queues[table][hash(pk)].put(pk)
            elif action == "delete":
                for pk in pks:
                    self.delete_queues[table].put(pk)
            else:
                self.logger.error("msg corrupt -> %s" % msg)
                continue
