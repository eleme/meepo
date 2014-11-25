# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
logging.basicConfig(level=logging.DEBUG)

from blinker import signal


def test_sqlalchemy_pub(mysql_dsn):
    (t_writes, t_updates, t_deletes,
     s_events, s_commits, s_rollbacks) = ([] for _ in range(6))

    def _clear():
        del t_writes[:]
        del t_updates[:]
        del t_deletes[:]
        del s_events[:]
        del s_commits[:]
        del s_rollbacks[:]

    def test_sg(sg_list):
        return lambda pk: sg_list.append(pk)

    # connect table action signal
    signal("test_write").connect(test_sg(t_writes), weak=False)
    signal("test_update").connect(test_sg(t_updates), weak=False)
    signal("test_delete").connect(test_sg(t_deletes), weak=False)

    # connect session action signal
    def session_prepare(event, sid, action):
        s_events.append({"event": event, "sid": sid, "action": action})
    signal("session_prepare").connect(session_prepare, weak=False)
    signal("session_commit").connect(test_sg(s_commits), weak=False)
    signal("session_rollback").connect(test_sg(s_rollbacks), weak=False)

    import sqlalchemy as sa
    from sqlalchemy.orm import scoped_session, sessionmaker
    from sqlalchemy.ext.automap import automap_base

    # sqlalchemy prepare
    engine = sa.create_engine(mysql_dsn)
    base = automap_base()
    base.prepare(engine=engine, reflect=True)
    Session = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))
    Test = base.classes["test"]

    # install sqlalchemy_pub hook
    from meepo.pub import sqlalchemy_pub
    sqlalchemy_pub(Session, strict_tables=["test"])

    # test empty commit
    _clear()
    Session().commit()
    assert [t_writes, t_updates, t_deletes,
            s_events, s_commits, s_rollbacks] == [[]] * 6

    # test single write
    _clear()
    session = Session()
    t_a = Test(data='a')
    session.add(t_a)
    session.commit()
    session.close()

    event, sid = s_events.pop(), s_commits.pop()
    assert t_writes == [1]
    assert event == {"sid": sid, "action": "write", "event": {"test": {1}}}
    assert [t_updates, t_deletes, s_rollbacks] == [[]] * 3

    # test single flush - write
    _clear()
    session = Session()
    t_b = Test(data='b')
    session.add(t_b)
    session.flush()
    session.commit()
    session.close()

    event, sid = s_events.pop(), s_commits.pop()
    assert t_writes == [2]
    assert event == {"sid": sid, "action": "write", "event": {"test": {2}}}
    assert [t_updates, t_deletes, s_rollbacks] == [[]] * 3

    # test multiple writes
    _clear()
    session = Session()
    t_c = Test(data='c')
    t_d = Test(data='d')
    session.add(t_c)
    session.add(t_d)
    session.commit()
    session.close()

    event, sid = s_events.pop(), s_commits.pop()
    assert set(t_writes) == {3, 4}
    assert event == {"sid": sid, "action": "write", "event": {"test": {3, 4}}}
    assert [t_updates, t_deletes, s_rollbacks] == [[]] * 3

    # test single update
    _clear()
    session = Session()
    t_a = session.query(Test).filter(Test.data == 'a').one()
    t_a.data = "aa"
    session.commit()
    session.close()

    event, sid = s_events.pop(), s_commits.pop()
    assert set(t_updates) == {1}
    assert event == {"sid": sid, "action": "update", "event": {"test": {1}}}
    assert [t_writes, t_deletes, s_rollbacks] == [[]] * 3

    # test single flush - update
    _clear()
    session = Session()
    t_a = session.query(Test).filter(Test.data == 'aa').one()
    t_a.data = "a"
    session.flush()
    session.commit()
    session.close()

    event, sid = s_events.pop(), s_commits.pop()
    assert set(t_updates) == {1}
    assert event == {"sid": sid, "action": "update", "event": {"test": {1}}}
    assert [t_writes, t_deletes, s_rollbacks] == [[]] * 3

    # test mixed write, update, delete & multi flush
    _clear()
    session = Session()
    t_b, t_c = session.query(Test).filter(Test.data.in_(('b', 'c'))).all()
    t_e = Test(data='e')
    session.add(t_e)
    t_b.data = "x"
    session.flush()
    session.delete(t_c)
    session.commit()
    session.close()

    # one success commit yields one commit sid
    assert len(s_commits) == 1

    # since the commit include a flush in it, if a flush happened in the middle
    # of transaction, it will cause the same "session_prepare" events to be
    # signaled multiple times.
    assert s_events[:2] == s_events[2:4]

    # test session events
    sid = s_commits.pop()
    assert s_events[2:] == [
        {"sid": sid, "action": "write", "event": {"test": {5}}},
        {"sid": sid, "action": "update", "event": {"test": {2}}},
        {"sid": sid, "action": "delete", "event": {"test": {3}}}
    ]
    assert (t_writes, t_updates, t_deletes) == ([5], [2], [3])

    # test empty rollback
    _clear()
    Session().rollback()
    assert [t_writes, t_updates, t_deletes,
            s_events, s_commits, s_rollbacks] == [[]] * 6

    # test rollback before flush
    _clear()
    session = Session()
    t_e = Test(data='e')
    session.add(t_e)
    session.rollback()
    session.close()

    # rollback happened before flush, nothing recorded.
    assert [t_writes, t_updates, t_deletes,
            s_events, s_commits, s_rollbacks] == [[]] * 6

    # test flush & rollback
    _clear()
    session = Session()
    t_e = Test(data='e')
    session.add(t_e)
    session.flush()
    session.rollback()
    session.close()

    # test session events, since rollback happened after flush, the write
    # have a pk value.
    event, sid = s_events.pop(), s_rollbacks.pop()
    assert event == {"sid": sid, "action": "write", "event": {"test": {6}}}
    assert [t_writes, t_updates, t_deletes, s_commits] == [[]] * 4
