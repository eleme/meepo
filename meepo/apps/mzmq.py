# -*- coding: utf-8 -*-

"""
    meepo.apps.mzmq
    ~~~~~~~~~~~~~~~

    ZeroMQ pubsub app.

    Sub in python script::

        import zmq

        ctx = zmq.Context()
        sub_socket = ctx.socket(zmq.SUB)
        sub_socket.connect("tcp://127.0.0.1:6000")
        sub_socket.setsockopt(zmq.SUBSCRIBE, '')

        while True:
            print(sub_socket.recv_string())
"""

import logging

import click

from meepo.pub import mysql_pub
from meepo.sub import zmq_sub

from meepo.logutils import setup_logger


@click.command()
@click.option('-m', '--master_dsn')
@click.option('-b', '--bind')
@click.argument('tables', nargs=-1)
def main(master_dsn, bind, tables):
    setup_logger("INFO")

    logger = logging.getLogger(__name__)

    # currently only supports mysql master
    assert master_dsn and master_dsn.startswith("mysql")
    assert bind.startswith("ipc") or bind.startswith("tcp")

    logger.info("event sourcing tables: %s" % ", ".join(tables))
    zmq_sub(bind, tables)
    mysql_pub(master_dsn)
