# -*- coding: utf-8 -*-

from blinker import signal


def print_sub(tables):
    """Events print subscriber.
    """
    for table in tables:
        _print = lambda pk, t=table: print("{} -> {}".format(t, pk))
        signal("{}_write".format(table)).connect(_print, weak=False)
        signal("{}_update".format(table)).connect(_print, weak=False)
        signal("{}_delete".format(table)).connect(_print, weak=False)
