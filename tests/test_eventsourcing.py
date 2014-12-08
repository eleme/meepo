# -*- coding: utf-8 -*-

from blinker import signal

from meepo.sub.eventsourcing import redis_es_sub
from meepo.apps.event_store import MRedisEventStore
from meepo.apps.prepare_commit import MRedisPrepareCommit


def test_redis_es_sub_commit(redis_dsn, mock_session):
    redis_es_sub(["test"], redis_dsn)

    # expose process & storage layer
    prepare_commit = MRedisPrepareCommit(redis_dsn)
    event_store = MRedisEventStore(redis_dsn)

    # mock session commit process
    evt = {"test_write": {1}}
    signal("session_prepare").send(mock_session, event=evt)
    assert prepare_commit.phase(mock_session) == "prepare"
    assert prepare_commit.get_session_info(mock_session) == evt

    signal("session_commit").send(mock_session)
    assert prepare_commit.phase(mock_session) == "commit"
    assert prepare_commit.get_prepare_info() == set()

    signal("test_write").send(1)
    assert event_store.get_all("test_write") == ['1']
