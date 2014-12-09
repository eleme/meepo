# -*- coding: utf-8 -*-

"""
Meepo's core concept is based event pubs, it follows mysql row-based binlog
and sqlalchemy events system and shape them into ``table_action pk``
format signals.

Currently there are 2 pubs implemented: ``mysql_pub`` and ``sqlalchemy_pub``.

The publishers and subscribers are connected with ``blinker.signal``.

Publisher sends pk by::

    signal("table_action").send(pk)

And subscriber can accept the pk by::

    sg = signal("table_action")

    @sg.connect
    def dummy_print(pk):
        print(pk)
"""

from __future__ import absolute_import

__all__ = ["mysql_pub", "sqlalchemy_pub"]

from .mysql import mysql_pub
from .sqlalchemy import sqlalchemy_pub
