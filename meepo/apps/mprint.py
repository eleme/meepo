# -*- coding: utf-8 -*-

"""
    meep.apps.mprint
    ~~~~~~~~~~~~~~~~~~~~

    Event print app.
"""

import click

from meepo.pub import mysql_pub
from meepo.sub import print_sub


@click.command()
@click.option('-d', '--mysql_dsn')
@click.argument('tables', nargs=-1)
def main(mysql_dsn, tables):
    print("listen for tables: %s" % ", ".join(tables))
    print_sub(tables)
    mysql_pub(mysql_dsn)
