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
@click.option("-d", "--debug", is_flag=True)
@click.option('-m', '--mysql_dsn')
@click.argument('tables', nargs=-1)
def main(mysql_dsn, tables, debug=False):
    level = "DEBUG" if debug else "INFO"
    setup_logger(level)
    print_sub(tables)
    mysql_pub(mysql_dsn, blocking=False)
