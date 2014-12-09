# -*- coding: utf-8 -*-

import pytest
import redis
from meepo.apps.eventsourcing.prepare_commit import RedisPrepareCommit


@pytest.fixture(scope="module")
def redis_pc(redis_dsn):
    pc = RedisPrepareCommit(
        redis_dsn, strict=False, namespace="meepo.test.event_store")
    pc.r.flushdb()
    return pc


@pytest.fixture(scope="module")
def redis_strict_pc():
    """Test strict prepare_commit which won't silent the exception.

    We'll pass an error redis dsn here to make sure ConnectionError raised.
    """
    redis_dsn = "redis://non_exists:0/"
    pc = RedisPrepareCommit(
        redis_dsn, strict=True, namespace="meepo.test.event_store")
    return pc


def test_redis_prepare_commit_phase(mock_session, redis_pc):
    # prepare session
    event = {"test_write": {1}, "test_update": {2, 3}, "test_delete": {4}}
    redis_pc.prepare(mock_session, event)

    # test prepare phase recorded
    assert redis_pc.phase(mock_session) == "prepare"
    assert redis_pc.prepare_info() == {mock_session.meepo_unique_id}
    assert redis_pc.session_info(mock_session) == event

    # test commit phase recorded
    redis_pc.commit(mock_session)
    assert redis_pc.phase(mock_session) == "commit"
    assert redis_pc.prepare_info() == set()


def test_redis_strict_prepare_commit_phase(mock_session, redis_strict_pc):
    with pytest.raises(redis.ConnectionError):
        redis_strict_pc.prepare(mock_session, {"test_write": 1})

    with pytest.raises(redis.ConnectionError):
        redis_strict_pc.commit(mock_session)
