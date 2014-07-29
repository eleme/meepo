# -*- coding: utf-8 -*-

"""
    meepo.apps.mzdevice
    ~~~~~~~~~~~~~~~~~~~

    ZeroMQ Forwarder Device.

    This is a forwarder that connect multiple pubs and subs.

    Refer to http://learning-0mq-with-pyzmq.readthedocs.org/en/latest/pyzmq/devices/forwarder.html  # NOQA
"""

import logging

import click
import zmq

from meepo.logutils import setup_logger


@click.command()
@click.option('-f', '--frontend_bind')
@click.option('-b', '--backend_bind')
def main(frontend_bind, backend_bind):
    setup_logger("INFO")

    logger = logging.getLogger(__name__)

    assert frontend_bind and backend_bind

    try:
        ctx = zmq.Context()
        frontend = ctx.socket(zmq.SUB)
        frontend.bind(frontend_bind)
        frontend.setsockopt(zmq.SUBSCRIBE, b"")

        backend = ctx.socket(zmq.PUB)
        backend.bind(backend_bind)

        zmq.device(zmq.FORWARDER, frontend, backend)

    except Exception as e:
        logger.exception(e)

    finally:
        frontend.close()
        backend.close()
