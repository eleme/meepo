# -*- coding: utf-8 -*-

"""
The sqlalchemy pub will hook into SQLAlchemy's event system, shape and publish
events with ``table_action pk`` style.

The events pub flow::

    +------------------+         +-----------------------+
    |                  |         |                       |
    |    meepo.pub     |         |      before_flush     |
    |                  |    +---->                       |
    |  sqlalchemy_pub  |    |    |  record model states  |
    |                  |    |    |                       |
    +---------+--------+    |    +-----------+-----------+
              |             |                |
        hook  |             |                |
              |             |                |
    +---------v--------+    |    +-----------v-----------+
    |                  |    |    |                       |
    |    sqlalchemy    |    |    |     after_commit      |
    |                  +----+---->                       |
    |  session events  |         |  publish model states |
    |                  |         |                       |
    +------------------+         +-----------+-----------+
                                             |
                                       +-----+
                                       |
               +------------------------------------------------+
               |                       |                        |
    +----------+--------+   +----------v---------+   +----------v---------+
    |                   |   |                    |   |                    |
    | table_write event |   | table_update event |   | table_delete event |
    |                   |   |                    |   |                    |
    +-------------------+   +--------------------+   +--------------------+

"""

from __future__ import absolute_import

import logging

import uuid

from sqlalchemy import event

from ..signals import signal


class SQLAlchemyPub(object):
    """SQLAlchemy Pub.

    The install method will add 2 hooks on sqlalchemy events system:

    * ``session_update`` -> sqlalchemy - ``before_flush``
    * ``session_commit`` -> sqlalchemy - ``after_commit``

    The ``session_update`` method need to record the model states in
    sqlalchemy "before_flush" event, when the session records the status
    with ``session.new``, ``session.dirty`` and ``session.deleted``, these
    states will be deleted in "after_commit" event.

    **General Usage**

    Install the sqlalchemy pub hook by calling it on sqlalchemy session::

        sqlalchemy_pub(session)

    Only listen some tables::

        sqlalchemy_pub(session, tables=["test"])

    Then use the session as usual and the events will be available.

    **Signals Illustrate**

    Sometimes you want more info than the pk value, the sqlalchemy_pub expose
    a raw signal which will send the original sqlalchemy objects.

    For example, this code::

        class Test(Base):
            __tablename__ = "test"
            id = Column(Integer, primary_key=True)
            data = Column(String)

        t_1 = Test(id=1, data='a')
        session.add(t_1)
        session.commit()

    Generates signals equal to::

        signal("test_write").send(1)
        signal("test_write_raw").send(t_1)

    .. warning::

        SQLAlchemy bulk operation currently **NOT** supported, so this code
        won't work::

            # bulk updates
            session.query(Test).update({"data": 'x'})

            # bulk deletes
            session.query(Test).filter(Test.data == 'x').delete()
    """

    logger = logging.getLogger("meepo.pub.sqlalchemy_pub")

    @classmethod
    def _pk(cls, obj):
        """Get pk values from object

        :param obj: sqlalchemy object
        """
        pk_values = tuple(getattr(obj, c.name)
                          for c in obj.__mapper__.primary_key)
        if len(pk_values) == 1:
            return pk_values[0]
        return pk_values

    @classmethod
    def _session_init(cls, session):
        if hasattr(session, "meepo_unique_id"):
            cls.logger.debug("skipped - session_init")
            return

        for action in ("write", "update", "delete"):
            attr = "pending_%s" % action
            if not hasattr(session, attr):
                setattr(session, attr, set())
        session.meepo_unique_id = uuid.uuid4().hex
        cls.logger.debug("%s - session_init" % session.meepo_unique_id)

    @classmethod
    def _session_del(cls, session):
        cls.logger.debug("%s - session_del" % session.meepo_unique_id)
        del session.meepo_unique_id
        del session.pending_write
        del session.pending_update
        del session.pending_delete

    @classmethod
    def _session_pub(cls, session):
        def _pub(obj, action):
            """Publish object pk values with action.

            The _pub will trigger 2 signals:
            * normal ``table_action`` signal, sends primary key
            * raw ``table_action_raw`` signal, sends sqlalchemy object

            :param obj: sqlalchemy object
            :param action: action on object
            """
            tables = getattr(session, "_meepo_sqlalchemy_pub_tables", [])
            if tables and obj.__table__.fullname not in tables:
                return

            sg_name = "%s_%s" % (obj.__table__, action)
            sg = signal(sg_name)
            sg_raw = signal("%s_raw" % sg_name)

            pk = cls._pk(obj)
            if pk:
                sg.send(pk)
                sg_raw.send(obj)
                cls.logger.debug("%s - session_pub: %s -> %s" % (
                    session.meepo_unique_id, sg_name, pk))

        for obj in session.pending_write:
            _pub(obj, action="write")
        for obj in session.pending_update:
            _pub(obj, action="update")
        for obj in session.pending_delete:
            _pub(obj, action="delete")

        session.pending_write.clear()
        session.pending_update.clear()
        session.pending_delete.clear()

    @classmethod
    def session_update(cls, session, *_):
        """Record the sqlalchemy object states in the middle of session,
        prepare the events for the final pub in session_commit.
        """
        cls._session_init(session)
        session.pending_write |= set(session.new)
        session.pending_update |= set(session.dirty)
        session.pending_delete |= set(session.deleted)
        cls.logger.debug("%s - session_update" % session.meepo_unique_id)

    @classmethod
    def session_commit(cls, session):
        """Pub the events after the session committed.

        This method should be linked to sqlalchemy "after_commit" event.
        """
        # this may happen when there's nothing to commit
        if not hasattr(session, 'meepo_unique_id'):
            cls.logger.debug("skipped - session_commit")
            return

        cls._session_pub(session)
        cls._session_del(session)

    @classmethod
    def install(cls, session, tables):
        """Install sqlalchemy_pub hooks.

        This method can be called multiple time safely.

        :param session: sqlalchemy session to install the hook
        :param tables: tables to install the hook, leave None to pub all.
        """
        if "meepo_tables" not in session.info:
            # init listen tables
            session.info["meepo_tables"] = set(tables)

            # enable session_update & session_commit hook
            event.listen(session, "before_flush", cls.session_update)
            event.listen(session, "after_commit", cls.session_commit)
        else:
            session.info["meepo_tables"] |= set(tables)


def sqlalchemy_pub(session, tables):
    """Install sqlalchemy pub hook of SQLAlchemyPub instance.

    :param session: sqlalchemy db session.
    :param tables: which tables to enable sqlalchemy_pub.
    """
    # install sqlalchemy_pub hook
    SQLAlchemyPub().install(session, tables)
