# -*- coding: utf-8 -*-

from __future__ import absolute_import


__all__ = ["pickle", "urlparse", "Empty"]

import sys
PY3 = sys.version_info[0] >= 3

if PY3:
    from urllib.parse import urlparse
    from queue import Empty
    import pickle

    def u(s):
        if isinstance(s, str):
            return s
        return s.decode("utf-8")

    text_types = (str, )   # noqa

else:
    from urlparse import urlparse
    from Queue import Empty
    import cPickle as pickle

    def u(s):
        if isinstance(s, unicode):  # noqa
            return s
        return s.decode("utf-8")

    text_types = (unicode, )  # noqa
