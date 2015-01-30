import logging
import os
import pytest
import tempfile
import uuid

from meepo.apps.eventsourcing.pub import sqlalchemy_es_pub


class sqlalchemy_test(sqlalchemy_es_pub):
    def __init__(self, session):
        self.session = session
        self.tables = set()

    def __call__(self):
        return self


class Column(object):
    def __init__(self, name):
        self.name = name


class Obj(object):
    def __init__(self, tablename, id_):
        self.__table__ = type("Table", (object,), {})
        self.__table__.fullname = tablename
        self.__mapper__ = type("Mapper", (object,), {})
        self.__mapper__.primary_key = (Column("id"),)

        self.id = id_


class Session(object):
    def __init__(self):
        self.meepo_unique_id = str(uuid.uuid4())

        self.pending_write = [Obj("test1", 1), Obj('test2', 2)]
        self.pending_update = [Obj('test1', 4)]
        self.pending_delete = [Obj('test2', 3)]


@pytest.fixture
def logger(request):
    for handler in logging.root.handlers:
        logging.root.removeHandler(handler)

    _, filename = tempfile.mkstemp()
    logging.basicConfig(level=logging.DEBUG, filename=filename)
    logger = logging.getLogger("test")

    def fin():
        for handler in logging.root.handlers:
            logging.root.addHandler(handler)

        try:
            os.remove(filename)
        except Exception:
            pass
    request.addfinalizer(fin)

    def _read():
        with open(filename) as f:
            return f.read()

    return {"instance": logger, "read": _read}


def test_session_prepare_log(logger):
    sqlalchemy_test.logger = logger["instance"]

    session = Session()
    es = sqlalchemy_test(session)
    es.session_prepare(session, None)

    a = logger["read"]().strip().split('\n')

    uid = session.meepo_unique_id

    assert a == [
        'DEBUG:test:{} - session_prepare: test1_write -> 1'.format(uid),
        'DEBUG:test:{} - session_prepare: test2_write -> 2'.format(uid),
        'DEBUG:test:{} - session_prepare: test1_update -> 4'.format(uid),
        'DEBUG:test:{} - session_prepare: test2_delete -> 3'.format(uid)
    ]
