# -*- coding: utf-8 -*-

"""Meepo Replicators based on events.
"""

from __future__ import absolute_import

import logging
import zmq

__all__ = ["QueueReplicator", "RqReplicator"]

zmq_ctx = zmq.Context()


class Replicator(object):
    """Replicator base class.
    """
    def __init__(self, listen=None, name="meepo.replicator.zmq"):
        """
        :param listen: zeromq dsn to connect, can be a list
        """
        # replicator logger naming
        self.name = name
        self.logger = logging.getLogger(name)

        self.listen = listen
        self.socket = zmq_ctx.socket(zmq.SUB)

    def run(self):
        raise NotImplementedError()

    def event(self):
        raise NotImplementedError()


from .queue import QueueReplicator
from .rq import RqReplicator
