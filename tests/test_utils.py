# -*- coding: utf-8 -*-


import datetime
import time

from meepo.utils import b, s, d


def test_cast_bytes():
    assert b("abc") == b"abc"
    assert b(b"abc") == b"abc"


def test_cast_str():
    assert s("abc") == "abc"
    assert s(b"abc") == "abc"


def test_cast_datetime():
    now = time.time()
    now_dt = datetime.datetime.fromtimestamp(now)
    assert d(now) == now_dt
    assert d(now, fmt="%Y%m%d") == now_dt.strftime("%Y%m%d")
