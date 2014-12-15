# -*- coding: utf-8 -*-

from __future__ import absolute_import

import itertools
import logging

from ..signals import signal


def zmq_sub(bind, tables, forwarder=False, green=False):
    """0mq fanout sub.

    This sub will use zeromq to fanout the events.

    :param bind: the zmq pub socket or zmq device socket.
    :param tables: the events of tables to follow.
    :param forwarder: set to True if zmq pub to a forwarder device.
    :param green: weather to use a greenlet compat zmq
    """
    logger = logging.getLogger("meepo.sub.zmq_sub")

    if not isinstance(tables, (list, set)):
        raise ValueError("tables should be list or set")

    if not green:
        import zmq
    else:
        import zmq.green as zmq

    ctx = zmq.Context()
    socket = ctx.socket(zmq.PUB)

    if forwarder:
        socket.connect(bind)
    else:
        socket.bind(bind)

    events = ("%s_%s" % (tb, action) for tb, action in
              itertools.product(*[tables, ["write", "update", "delete"]]))
    for event in events:
        def _sub(pk, event=event):
            msg = "%s %s" % (event, pk)
            socket.send_string(msg)
            logger.debug("pub msg: %s" % msg)
        signal(event).connect(_sub, weak=False)

    return socket
