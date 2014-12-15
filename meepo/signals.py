# -*- coding: utf-8 -*-

from __future__ import absolute_import

from ._compat import str, bytes


def _monkey_patch_hashable_func():
    def hashable_identity(obj):
        if hasattr(obj, '__func__'):
            return (id(obj.__func__), id(obj.__self__))
        elif hasattr(obj, 'im_func'):
            return (id(obj.im_func), id(obj.im_self))
        elif isinstance(obj, (str, bytes)):
            return obj
        # hack for session hash info
        elif hasattr(obj, "info") and "name" in obj.info:
            return str(obj.info["name"])
        else:
            return id(obj)

    import blinker.base
    blinker.base.hashable_identity = hashable_identity
_monkey_patch_hashable_func()


from blinker import Namespace

# The namespace for code signals.  If you are not flask code, do
# not put signals in here.  Create your own namespace instead.
_signals = Namespace()
signal = _signals.signal
