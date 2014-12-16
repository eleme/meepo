# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
logging.basicConfig(level=logging.DEBUG)

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

from meepo.apps.eventsourcing import sqlalchemy_es_pub
from meepo.signals import signal

(t_writes, t_updates, t_deletes,
 s_events, s_commits, s_rollbacks) = ([] for _ in range(6))


def _clear():
    del t_writes[:]
    del t_updates[:]
    del t_deletes[:]
    del s_events[:]
    del s_commits[:]
    del s_rollbacks[:]


def setup_module(module):
    def test_sg(sg_list):
        return lambda pk: sg_list.append(pk)

    # connect table action signal
    signal("test_write").connect(test_sg(t_writes), weak=False)
    signal("test_update").connect(test_sg(t_updates), weak=False)
    signal("test_delete").connect(test_sg(t_deletes), weak=False)

    # connect session action signal
    def test_session_prepare(session, event):
        s_events.append({"sid": session.meepo_unique_id, "event": event})

    def test_session_commit(session):
        s_commits.append(session.meepo_unique_id)

    def test_session_rollback(session):
        s_rollbacks.append(session.meepo_unique_id)

    signal("session_prepare").connect(test_session_prepare, weak=False)
    signal("session_commit").connect(test_session_commit, weak=False)
    signal("session_rollback").connect(test_session_rollback, weak=False)


def teardown_module(module):
    pass


def setup_function(function):
    _clear()


def teardown_function(function):
    pass


@pytest.fixture(scope="module")
def model_cls():
    Base = declarative_base()

    class model_cls(Base):
        __tablename__ = "test"
        id = sa.Column(sa.Integer, primary_key=True)
        data = sa.Column(sa.String)
    return model_cls


@pytest.fixture(scope="module")
def session(mysql_dsn):
    # sqlalchemy prepare
    engine = sa.create_engine(mysql_dsn)
    session = scoped_session(sessionmaker(bind=engine,
                                          expire_on_commit=False,
                                          info={"name": "test_session"}))

    # install sqlalchemy_pub hook
    sqlalchemy_es_pub(session, tables=["test"])
    return session


@pytest.fixture(scope="module")
def session_b(mysql_dsn):
    # sqlalchemy prepare
    engine = sa.create_engine(mysql_dsn)
    session_b = scoped_session(sessionmaker(bind=engine,
                                            expire_on_commit=False,
                                            info={"name": "test_session_b"}))

    # install sqlalchemy_pub hook
    sqlalchemy_es_pub(session_b, tables=["test"])
    return session_b


def test_sa_empty_commit(session):
    """Direct commit generates nothing
    """
    session.commit()

    assert [t_writes, t_updates, t_deletes,
            s_events, s_commits, s_rollbacks] == [[]] * 6


def test_sa_single_write(session, model_cls):
    """Write commit generate a write event with row pk.
    """
    t_a = model_cls(data='a')
    session.add(t_a)
    session.commit()

    event, sid = s_events.pop(), s_commits.pop()
    assert event == {"sid": sid, "event": {"test_write": {t_a.id}}}

    assert t_writes == [t_a.id]
    assert [t_updates, t_deletes, s_rollbacks] == [[]] * 3


def test_sa_single_flush_write(session, model_cls):
    """Flush - Write is the same with write.
    """
    t_b = model_cls(data='b')
    session.add(t_b)
    session.flush()
    session.commit()

    event, sid = s_events.pop(), s_commits.pop()
    assert event == {"sid": sid, "event": {"test_write": {t_b.id}}}

    assert t_writes == [t_b.id]
    assert [t_updates, t_deletes, s_rollbacks] == [[]] * 3


def test_sa_multi_writes(session, model_cls):
    # test multiple writes
    t_c = model_cls(data='c')
    t_d = model_cls(data='d')
    session.add(t_c)
    session.add(t_d)
    session.commit()

    event, sid = s_events.pop(), s_commits.pop()
    assert event == {"sid": sid, "event": {"test_write": {t_c.id, t_d.id}}}

    assert set(t_writes) == {t_c.id, t_d.id}
    assert [t_updates, t_deletes, s_rollbacks] == [[]] * 3


def test_sa_single_update(session, model_cls):
    # test single update
    t_a = session.query(model_cls).filter(model_cls.data == 'a').one()
    t_a.data = "aa"
    session.commit()

    event, sid = s_events.pop(), s_commits.pop()
    assert event == {"sid": sid, "event": {"test_update": {t_a.id}}}

    assert set(t_updates) == {t_a.id}
    assert [t_writes, t_deletes, s_rollbacks] == [[]] * 3


def test_sa_single_flush_update(session, model_cls):
    # test single flush - update
    t_a = session.query(model_cls).filter(model_cls.data == 'aa').one()
    t_a.data = "a"
    session.flush()
    session.commit()

    event, sid = s_events.pop(), s_commits.pop()
    assert event == {"sid": sid, "event": {"test_update": {t_a.id}}}

    assert set(t_updates) == {t_a.id}
    assert [t_writes, t_deletes, s_rollbacks] == [[]] * 3


def test_sa_mixed_write_update_delete_and_multi_flushes(session, model_cls):
    """The most compliated situation, the test goes through the following
    process:
    1. add one row, update one row
    2. flush to database
    3. delete one row
    4. flush to database
    5. commit
    """
    t_b, t_c = session.query(model_cls). \
        filter(model_cls.data.in_(('b',  'c'))).all()
    t_e = model_cls(data='e')
    session.add(t_e)
    t_b.data = "x"
    session.flush()
    session.delete(t_c)
    session.flush()
    session.commit()

    # one success commit generates one commit sid
    assert len(s_commits) == 1

    # test session events
    sid = s_commits.pop()

    # since the commit include a flush in it, two events will be triggered and
    # the later event contains the first event.
    assert s_events[0] == {"sid": sid,
                           "event": {"test_write": {t_e.id},
                                     "test_update": {t_b.id}}}
    assert s_events[1] == {"sid": sid,
                           "event": {"test_write": {t_e.id},
                                     "test_update": {t_b.id},
                                     "test_delete": {t_c.id}}}


def test_sa_empty_rollback(session):
    """Direct rollback generates nothing
    """
    session.rollback()

    assert [t_writes, t_updates, t_deletes,
            s_events, s_commits, s_rollbacks] == [[]] * 6


def test_sa_early_rollback(session, model_cls):
    """Rollback happened before flush, nothing recorded.
    """
    t_e = model_cls(data='e')
    session.add(t_e)
    session.rollback()

    assert [t_writes, t_updates, t_deletes,
            s_events, s_commits, s_rollbacks] == [[]] * 6


def test_sa_flush_rollback(session, model_cls):
    """Rollback happened after flush, event recorded.
    Since rollback happened after flush, the write have a pk value.
    """
    t_e = model_cls(data='e')
    session.add(t_e)
    session.flush()
    session.rollback()

    event, sid = s_events.pop(), s_rollbacks.pop()
    assert event == {"sid": sid, "event": {"test_write": {t_e.id}}}

    assert [t_writes, t_updates, t_deletes, s_commits] == [[]] * 4
