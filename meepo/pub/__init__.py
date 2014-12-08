# -*- coding: utf-8 -*-

"""
The core pub part of meepo event system. Meepo follows events from different
sources, e.g. mysql binlog and sqlalchemy events, and shape them into
``table_action pk`` format event, e.g.::

    test_table_write 1
    test_table_update 2

The publishers and subscribers are connected with ``blinker.signal``.

The publisher sends pk by::

    signal("table_action").send(pk)

And subscriber can accept the pk by::

    sg = signal("table_action")

    @sg.connect
    def dummy_print(pk):
        print(pk)

    # or one-line connect
    signal("table_action").connect(lambda pk: print(pk), weak=False)

.. automodule:: meepo.pub.mysql
    :members:

.. automodule:: meepo.pub.sqlalchemy
    :members:

"""

from __future__ import absolute_import

__all__ = ["mysql_pub", "sqlalchemy_pub"]

from .mysql import mysql_pub
from .sqlalchemy import sqlalchemy_pub
