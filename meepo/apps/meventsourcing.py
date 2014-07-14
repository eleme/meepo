# -*- coding: utf-8 -*-

"""
    meepo.apps.meventsourcing
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    EventSourcing app.

    This app is a simplified version of event sourcing, it only sources about
    the primary keys and their last changing time.
"""

import logging
logging.getLogger().addHandler(logging.StreamHandler())
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger("meepo.meventsourcing")

import click

from meepo.pub import mysql_pub
from meepo.sub import es_sub


@click.command()
@click.option('-m', '--master_dsn')
@click.option('-r', '--redis_dsn')
@click.option('--namespace')
@click.argument('tables', nargs=-1)
def main(master_dsn, redis_dsn, tables, namespace=None):
    # currently only supports mysql master
    assert master_dsn and master_dsn.startswith("mysql")
    assert redis_dsn and redis_dsn.startswith("redis")

    logger.info("event sourcing tables: %s" % ", ".join(tables))
    es_sub(redis_dsn, tables, namespace)
    mysql_pub(master_dsn)
