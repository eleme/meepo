# -*- coding: utf-8 -*-

"""Meepo Replicators based on events.
"""

from __future__ import absolute_import

import logging
import zmq

__all__ = ["QueueReplicator"]

zmq_ctx = zmq.Context()


class Replicator(object):
    """Replicator base class.
    """
    def __init__(self, name="meepo.replicator.zmq"):
        # replicator logger naming
        self.name = name
        self.logger = logging.getLogger(name)

    def run(self):
        raise NotImplementedError()


from .queue import QueueReplicator
