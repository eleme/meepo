# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging

from blinker import signal


def nano_sub(bind, tables):
    """Nanomsg fanout sub. (Experimental)

    This sub will use nanomsg to fanout the events.

    :param bind: the zmq pub socket or zmq device socket.
    :param tables: the events of tables to follow.
    """
    logger = logging.getLogger("meepo.sub.nano_sub")

    from nanomsg import Socket, PUB

    pub_socket = Socket(PUB)
    pub_socket.bind(bind)

    def _sub(table):
        for action in ("write", "update", "delete"):
            def _sub(pk, action=action):
                msg = bytes("%s_%s %s" % (table, action, pk), 'utf-8')
                logger.debug("pub msg %s" % msg)
                pub_socket.send(msg)

            signal("%s_%s" % (table, action)).connect(_sub, weak=False)

    for table in set(tables):
        _sub(table)
