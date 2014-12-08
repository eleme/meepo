# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging

import uuid

from blinker import signal
from sqlalchemy import event


class MSQLAlchemyPub(object):
    """SQLAlchemy Pub.

    This publisher don't publish events itself, it will hook on SQLAlchemy's
    event system, and publish the changes automatically.

    You may customize the sqlalchemy pub behavior, refer to
    MSQLAlchemyEventSourcingPub for example.
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
    def session_init(cls, session):
        if hasattr(session, "meepo_unique_id"):
            cls.logger.debug("skipped - session_init")
            return

        for action in ("write", "update", "delete"):
            attr = "pending_{}".format(action)
            if not hasattr(session, attr):
                setattr(session, attr, set())
        session.meepo_unique_id = uuid.uuid4().hex
        cls.logger.debug("%s - session_init" % session.meepo_unique_id)

    @classmethod
    def session_del(cls, session):
        cls.logger.debug("%s - session_del" % session.meepo_unique_id)
        del session.meepo_unique_id
        del session.pending_write
        del session.pending_update
        del session.pending_delete

    @classmethod
    def session_update(cls, session, *_):
        """Record session changes on flush
        """
        cls.session_init(session)
        session.pending_write |= set(session.new)
        session.pending_update |= set(session.dirty)
        session.pending_delete |= set(session.deleted)
        cls.logger.debug("%s - session_update" % session.meepo_unique_id)

    @classmethod
    def session_pub(cls, session):
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

            sg_name = "{}_{}".format(obj.__table__, action)
            sg = signal(sg_name)
            sg_raw = signal("{}_raw".format(sg_name))

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
    def session_commit(cls, session):
        # this may happen when there's nothing to commit
        if not hasattr(session, 'meepo_unique_id'):
            cls.logger.debug("skipped - session_commit")
            return

        cls.session_pub(session)
        cls.session_del(session)

    @classmethod
    def install(cls, session, tables=None):
        """Install hook

        :param session: sqlalchemy session to install the hook
        :param tables: tables to install the hook, leave None to pub all.
        """
        cls.logger.debug("session_install")

        if not hasattr(session, "_meepo_sqlalchemy_pub_tables"):
            # init listen tables
            session._meepo_sqlalchemy_pub_tables = set()

            # enable session_update & session_commit hook
            event.listen(session, "before_flush", cls.session_update)
            event.listen(session, "after_commit", cls.session_commit)

        if tables:
            session._meepo_sqlalchemy_pub_tables |= set(tables)


def sqlalchemy_pub(session, tables=None):
    """Install sqlalchemy eventsourcing hook by MSQLAlchemyPub.

    :param session: sqlalchemy db session.
    :param tables: which tables to enable sqlalchemy_pub.
    """
    # install sqlalchemy_pub hook
    MSQLAlchemyPub().install(session, tables)
