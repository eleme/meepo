# -*- coding: utf-8 -*-

from __future__ import absolute_import


__all__ = ["urlparse"]

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse
