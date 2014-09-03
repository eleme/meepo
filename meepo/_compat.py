# -*- coding: utf-8 -*-

from __future__ import absolute_import


__all__ = ["urlparse"]

import sys
PY3 = sys.version_info[0] == 3

if PY3:
    from urllib.parse import urlparse

    text_types = (str, )   # noqa

else:
    from urlparse import urlparse

    text_types = (unicode, )  # noqa
