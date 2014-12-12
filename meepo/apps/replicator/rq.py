# -*- coding: utf-8 -*-

from __future__ import absolute_import

import collections
import zmq

from zmq.utils.strtypes import asbytes
from . import Replicator


class RqReplicator(Replicator):
    """Replicator suitable for rq task queue.

    For example:
    >>> rq_repl = RqReplicator("tcp://127.0.0.1:4000")
    >>> @rq_repl.event("a_table_update")
    >>> def job_test(pks):
    >>>     q = rq.Queue("update_cache:a_table")
    >>>     q.enqueue("module.jobs.func", pks)
    >>> rq_repl.run()

    Rq queue should be created in the external code.

    In fact this replicator can be generally used. It will pass pks as argument
    to the supplied callback func and the func can do anything you want.

    The callback func should always accept a list of primary keys.
    """
    def __init__(self, *args, **kwargs):
        super(RqReplicator, self).__init__(*args, **kwargs)

        self.topic_funcs = {}

    def event(self, *topics):
        def wrapper(func):
            for topic in topics:
                self.topic_funcs[topic] = func
                self.socket.setsockopt(zmq.SUBSCRIBE, asbytes(topic))
            return func
        return wrapper

    def run(self):
        if isinstance(self.listen, list):
            for i in self.listen:
                self.socket.connect(i)
        else:
            self.socket.connect(self.listen)

        error_pks = collections.defaultdict(set)

        def do_job(topic, pks):
            try:
                self.topic_funcs[topic](pks)
            except Exception as e:
                self.logger.exception(e)
                error_pks[topic].update(pks)
            else:
                # remove error pks
                if topic in error_pks:
                    error_pks[topic].difference_update(pks)
                    if not error_pks[topic]:
                        error_pks.pop(topic)
        try:
            while True:
                # retry error pks
                for t, p in list(error_pks.items()):
                    self.logger.warn(
                        "process error pks: {} -> {}".format(t, p))
                    do_job(t, p)

                msg = self.socket.recv_string()

                lst = msg.split()
                if len(lst) >= 2:
                    topic, pks = lst[0], lst[1:]
                else:
                    self.logger.error("msg corrupt -> %s" % msg)
                    continue

                self.logger.info("replicator: {} -> {}".format(topic, pks))
                do_job(topic, pks)
        except Exception as e:
            self.logger.exception(e)
