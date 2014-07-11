# -*- coding: utf-8 -*-

"""
    meepo.apps.mreplicate
    ~~~~~~~~~~~~~~~~~~~~~

    Database replication app.

    This replication is a limited row-based write/update/delete replication.
    You have to make sure the schema was created and matched between master
    and slave before the replication starts.

    Currently, only replicate from mysql master with row-based binlog is
    supported.
"""

import logging
logging.getLogger().addHandler(logging.StreamHandler())
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger("meepo.mreplicate")

import click

from meepo.pub import mysql_pub
from meepo.sub import replicate_sub


@click.command()
@click.option('-m', '--master_dsn')
@click.option('-s', '--slave_dsn')
@click.argument('tables', nargs=-1)
def main(master_dsn, slave_dsn, tables):
    # currently only supports mysql master
    assert master_dsn.startswith("mysql")

    logger.info("replicating tables: %s" % ", ".join(tables))
    replicate_sub(master_dsn, slave_dsn, tables)
    mysql_pub(master_dsn)
