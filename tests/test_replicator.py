import os
import signal
import itertools
import time
import zmq

from multiprocessing import Queue, Process, Manager
from meepo.utils import setup_logger
setup_logger("DEBUG")

from meepo.apps.replicator import Worker, WorkerPool, QueueReplicator


def test_worker():
    result = Manager().dict()

    def func():
        def f(pks):
            result["pks"] = pks
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
        p.join(timeout=1.3)
        p.terminate()

    assert result["pks"] == list(range(10))


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
        p.join(timeout=1.5)
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
        p.join(timeout=1)

        assert len(result) == 3 and \
            set(itertools.chain(*result.values())) == set(range(30))

        pid = list(result.keys())[0]
        # test process recreating
        os.kill(pid, signal.SIGKILL)

        time.sleep(0.6)

        for i in [30, 31, 32]:
            queues[0].put(i)

        time.sleep(0.6)

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

    time.sleep(10)
    os.kill(rp.pid, signal.SIGINT)
    rp.join()

    assert set(int(i) for i in result) == set(range(50))
