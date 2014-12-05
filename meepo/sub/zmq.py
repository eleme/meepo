# -*- coding: utf-8 -*-

import logging

from blinker import signal


def zmq_sub(bind, tables, forwarder=False, green=False):
    """0mq fanout sub.

    This sub will use zeromq to fanout the events.

    :param bind: the zmq pub socket or zmq device socket.
    :param tables: the events of tables to follow.
    :param forwarder: set to True if zmq pub to a forwarder device.
    :param green: weather to use a greenlet compat zmq
    """
    logger = logging.getLogger("meepo.sub.nano_sub")

    if not green:
        import zmq
    else:
        from zmq.green import zmq

    ctx = zmq.Context()
    socket = ctx.socket(zmq.PUB)

    if forwarder:
        socket.connect(bind)
    else:
        socket.bind(bind)

    def _sub(table):
        for action in ("write", "update", "delete"):
            def _sub(pk, action=action):
                msg = "%s_%s %s" % (table, action, pk)
                socket.send_string(msg)
                logger.debug("pub msg: %s" % msg)
            signal("%s_%s" % (table, action)).connect(_sub, weak=False)

    for table in set(tables):
        _sub(table)
