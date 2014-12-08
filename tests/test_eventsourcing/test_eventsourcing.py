# -*- coding: utf-8 -*-

import pytest

from blinker import signal

from meepo.apps.eventsourcing.sub import redis_es_sub


@pytest.fixture(scope="function")
def es_sub(request, redis_dsn):
    event_store, prepare_commit = redis_es_sub(["test"], redis_dsn)

    def fin():
        for action in ["write", "update", "delete"]:
            event_store.clear("test_%s" % action)
        prepare_commit.clear()
    request.addfinalizer(fin)
    return event_store, prepare_commit


def test_redis_es_sub_commit(mock_session, es_sub):
    event_store, prepare_commit = es_sub

    # mock session prepare phase
    evt = {"test_write": {1}}
    signal("session_prepare").send(mock_session, event=evt)
    assert prepare_commit.phase(mock_session) == "prepare"
    assert prepare_commit.session_info(mock_session) == evt

    # mock session commit phase
    signal("session_commit").send(mock_session)
    assert prepare_commit.phase(mock_session) == "commit"
    assert prepare_commit.prepare_info() == set()

    signal("test_write").send(1)
    assert event_store.replay("test_write") == ['1']


def test_redis_es_sub_rollback(mock_session, es_sub):
    event_store, prepare_commit = es_sub

    # mock session prepare phase
    evt = {"test_write": {1}}
    signal("session_prepare").send(mock_session, event=evt)
    assert prepare_commit.phase(mock_session) == "prepare"
    assert prepare_commit.session_info(mock_session) == evt

    # mock session commit phase
    signal("session_rollback").send(mock_session)
    assert prepare_commit.phase(mock_session) == "commit"
    assert prepare_commit.prepare_info() == set()

    assert event_store.replay("test_write") == []
