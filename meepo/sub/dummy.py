# -*- coding: utf-8 -*-

import logging

from blinker import signal


def print_sub(tables):
    """Dummy print sub.

    :param tables: print events of tables.
    """
    logger = logging.getLogger("meepo.sub.print_sub")
    logger.info("print_sub tables: %s" % ", ".join(tables))

    for table in set(tables):
        signal("%s_write" % table).connect(
            lambda pk, t=table: logger.info("%s_write -> %s" % (t, pk)),
            weak=False)
        signal("%s_update" % table).connect(
            lambda pk, t=table: logger.info("%s_update -> %s" % (t, pk)),
            weak=False)
        signal("%s_delete" % table).connect(
            lambda pk, t=table: logger.info("%s_delete -> %s" % (t, pk)),
            weak=False)