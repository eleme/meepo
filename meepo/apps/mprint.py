# -*- coding: utf-8 -*-

"""
    meep.apps.mprint
    ~~~~~~~~~~~~~~~~

    Event print app.
"""

import click

from meepo.pub import mysql_pub
from meepo.sub import print_sub

from meepo.logutils import setup_logger


@click.command()
@click.option('-d', '--mysql_dsn')
@click.argument('tables', nargs=-1)
def main(mysql_dsn, tables):
    setup_logger('DEBUG')
    print_sub(tables)
    mysql_pub(mysql_dsn, blocking=False)
