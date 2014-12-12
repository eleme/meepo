# -*- coding: utf-8 -*-

from __future__ import absolute_import

from logging.config import dictConfig
import datetime

from ._compat import bytes, str


def setup_logger(level=None):
    dictConfig({
        'version': 1,
        'disable_existing_loggers': False,

        'root': {
            'handlers': ['console'],
            'level': 'INFO',
        },

        'loggers': {
            'meepo': {
                'handlers': ['console'],
                'propagate': False,
                'level': level or 'INFO',
            },
        },

        'handlers': {
            'console': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'console'
            },
        },

        'formatters': {
            'console': {
                'format': '%(asctime)s [%(levelname)s] [%(name)s][%(process)d]'
                          ': %(message)s',
            },
        }
    })


def cast_bytes(s, encoding='utf8', errors='strict'):
    """cast str or bytes to bytes"""
    if isinstance(s, bytes):
        return s
    elif isinstance(s, str):
        return s.encode(encoding, errors)
    else:
        raise TypeError("Expected unicode or bytes, got %r" % s)
b = cast_bytes


def cast_str(s, encoding='utf8', errors='strict'):
    """cast bytes or str to str"""
    if isinstance(s, bytes):
        return s.decode(encoding, errors)
    elif isinstance(s, str):
        return s
    else:
        raise TypeError("Expected unicode or bytes, got %r" % s)
s = cast_str


def cast_datetime(ts, fmt=None):
    """cast timestamp to datetime or date str"""
    dt = datetime.datetime.fromtimestamp(ts)
    if fmt:
        return dt.strftime(fmt)
    return dt
d = cast_datetime
