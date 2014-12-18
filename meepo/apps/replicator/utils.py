# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging

import zmq


def zmq_proxy(frontend, backend):
    logger = logging.getLogger("meepo.replicator.zmq_proxy")

    ctx, frontend_socket, backend_socket = None, None, None

    try:
        ctx = zmq.Context()
        frontend_socket = ctx.socket(zmq.XSUB)
        frontend_socket.bind(frontend)

        backend_socket = ctx.socket(zmq.XPUB)
        backend_socket.bind(backend)

        zmq.proxy(frontend_socket, backend_socket)

    except Exception as e:
        logger.exception(e)

    finally:
        if frontend_socket:
            frontend_socket.close()
        if backend_socket:
            backend_socket.close()
        if ctx:
            ctx.term()
