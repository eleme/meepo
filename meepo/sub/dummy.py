# -*- coding: utf-8 -*-

import itertools
import logging

from blinker import signal


def print_sub(tables):
    """Dummy print sub.

    :param tables: print events of tables.
    """
    logger = logging.getLogger("meepo.sub.print_sub")
    logger.info("print_sub tables: %s" % ", ".join(tables))

    events = ("%s_%s" % (tb, action) for tb, action in
              itertools.product(*[tables, ["write", "update", "delete"]]))
    for event in events:
        signal(event).connect(
            lambda pk: logger.info("%s -> %s" % event, pk), weak=False)
