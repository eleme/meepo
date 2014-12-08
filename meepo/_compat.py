# -*- coding: utf-8 -*-

from __future__ import absolute_import


__all__ = ["pickle", "urlparse", "Empty"]

import sys
PY3 = sys.version_info[0] >= 3

if PY3:
    from urllib.parse import urlparse
    from queue import Empty
    import pickle

    bytes = bytes
    str = str

else:
    from urlparse import urlparse
    from Queue import Empty
    import cPickle as pickle

    bytes = str
    str = unicode  # noqa
