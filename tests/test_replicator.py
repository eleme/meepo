# -*- coding: utf-8 -*-

from __future__ import absolute_import

import os
import signal
import itertools
import time
import zmq
import random

from multiprocessing import Queue, Process, Manager

from meepo.apps.replicator.worker import Worker, WorkerPool
from meepo.apps.replicator import QueueReplicator, RqReplicator
from meepo.utils import setup_logger
setup_logger("DEBUG")


def test_worker():
    result = Manager().list()

    def func():
        def f(pks):
            result.extend(pks)
            return [True for _ in pks]

        queue = Queue()
        for i in range(10):
            queue.put(i)

        worker = Worker(queue, "test", f, multi=True)
        worker.run()

    p = Process(target=func)
    try:
        p.start()
    finally:
        for i in range(200):
            time.sleep(0.3)
            if len(result) == 10:
                break

        p.terminate()

    assert [i for i in result] == list(range(10))


def test_worker_retry():
    result = Manager().dict()
    for i in range(3):
        result[i] = 0

    def func():
        def f(pks):
            try:
                return [False for _ in pks]
            finally:
                for pk in pks:
                    result[pk] += 1

        queue = Queue()
        for i in range(3):
            queue.put(i)

        worker = Worker(queue, "test", f, multi=True,
                        max_retry_count=3, max_retry_interval=0.1)
        worker.run()

    p = Process(target=func)
    try:
        p.start()
    finally:
        for i in range(200):
            time.sleep(0.3)
            if result[0] == result[1] == result[2] == 4:
                break
        p.terminate()

    assert result[0] == result[1] == result[2] == 4


def test_worker_pool():
    queues = [Queue() for i in range(3)]
    result = Manager().dict()

    tasks, i = range(30), 0
    for queue in queues:
        for j in tasks[i:i + 10]:
            queue.put(j)
        i += 10

    def f(pks):
        pid = os.getpid()
        if pid in result:
            r = result[pid]
            r.extend(pks)
            result[pid] = r
        else:
            result[pid] = pks
        return [True for _ in pks]

    def func():
        worker_pool = WorkerPool(queues, "test", f, multi=True,
                                 waiting_time=0.5)

        def handler(signum, frame):
            worker_pool.terminate()
        signal.signal(signal.SIGUSR1, handler)

        worker_pool.start()

    p = Process(target=func)
    try:
        p.start()
    finally:
        for i in range(200):
            time.sleep(0.3)
            if len(result) == 3:
                break

        assert len(result) == 3 and \
            set(itertools.chain(*result.values())) == set(range(30))

        pid = list(result.keys())[0]
        # test process recreating
        os.kill(pid, signal.SIGKILL)

        time.sleep(0.6)

        for i in [30, 31, 32]:
            queues[0].put(i)

        for i in range(200):
            time.sleep(0.3)
            if set(itertools.chain(*result.values())) == set(range(33)):
                break

        os.kill(p.pid, signal.SIGUSR1)

        assert len(result) in (3, 4) and \
            set(itertools.chain(*result.values())) == set(range(33))


def test_queue_replicator():
    result = Manager().list()

    def repl_process():
        queue_repl = QueueReplicator("tcp://127.0.0.1:6000")

        @queue_repl.event("test_update", workers=3, multi=True, queue_limit=3)
        def task(pks):
            result.extend(pks)
            return [True for _ in pks]

        Worker.MAX_PK_COUNT = 10
        queue_repl.run()

    rp = Process(target=repl_process)
    rp.start()

    time.sleep(1)

    ctx = zmq.Context()

    def producer():
        sock = ctx.socket(zmq.PUB)
        sock.bind("tcp://127.0.0.1:6000")
        time.sleep(0.5)
        for i in range(50):
            msg = "test_update {}".format(i)
            sock.send_string(msg)

    p = Process(target=producer)
    p.start()
    p.join()

    for i in range(200):
        time.sleep(0.9)
        if len(result) == 50:
            break

    os.kill(rp.pid, signal.SIGINT)
    rp.join()

    assert set(int(i) for i in result) == set(range(50))


def test_rq_replicator():
    result = Manager().list()

    def repl_process():
        rq_repl = RqReplicator("tcp://127.0.0.1:7000")

        @rq_repl.event("restaurant_update")
        def job(pks):
            i = random.randint(0, 5)
            if i == 3:
                raise Exception("random exception, pks: {}".format(pks))
            result.extend(pks)

        rq_repl.run()

    consumer = Process(target=repl_process)
    consumer.start()

    time.sleep(1)

    ctx = zmq.Context()

    def send_string():
        sock = ctx.socket(zmq.PUB)
        sock.bind("tcp://127.0.0.1:7000")
        time.sleep(0.5)
        for i in range(50):
            msg = "restaurant_update {}".format(i)
            sock.send_string(msg)

    producer = Process(target=send_string)
    producer.start()
    producer.join()

    for i in range(200):
        time.sleep(0.3)
        if len(result) == 50:
            break

    consumer.terminate()
    assert set(int(i) for i in result) == set(range(50))
