# -*- coding: utf-8 -*-

from __future__ import absolute_import


__all__ = ["pickle", "urlparse", "Empty"]

import sys
PY3 = sys.version_info[0] >= 3

if PY3:
    from urllib.parse import urlparse
    from queue import Empty

    import pickle

    text_types = (str, )   # noqa

else:
    from urlparse import urlparse
    from Queue import Empty

    text_types = (unicode, )  # noqa

    import cPickle as pickle
