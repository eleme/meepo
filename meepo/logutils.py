# -*- coding: utf-8 -*-

from logging.config import dictConfig


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
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            },
        }
    })
