# -*- coding: utf-8 -*-

from __future__ import absolute_import

import itertools
import logging

from ..signals import signal


def print_sub(tables):
    """Dummy print sub.

    :param tables: print events of tables.
    """
    logger = logging.getLogger("meepo.sub.print_sub")
    logger.info("print_sub tables: %s" % ", ".join(tables))

    if not isinstance(tables, (list, set)):
        raise ValueError("tables should be list or set")

    events = ("%s_%s" % (tb, action) for tb, action in
              itertools.product(*[tables, ["write", "update", "delete"]]))
    for event in events:
        signal(event).connect(
            lambda pk: logger.info("%s -> %s" % event, pk), weak=False)
