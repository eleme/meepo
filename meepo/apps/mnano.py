# -*- coding: utf-8 -*-

"""
    meepo.apps.mnano
    ~~~~~~~~~~~~~~~~

    Nanomsg publish app.

    Sub in command line::

        $ nanocat --sub --connect tcp://127.0.0.1:6000 --format ascii

    Sub in python script::

        from nanomsg import Socket, SUB, SUB_SUBSCRIBE

        with Socket(SUB) as sub_socket:
            sub_socket.connect("tcp://127.0.0.1:6000")
            sub_socket.set_string_option(SUB, SUB_SUBSCRIBE, '')

            while True:
                print(sub_socket.recv())
"""

import logging

import click

from meepo.pub import mysql_pub
from meepo.sub import nano_sub

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
    nano_sub(bind, tables)
    mysql_pub(master_dsn)
