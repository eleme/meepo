===================
Meepo Documentation
===================

Welcome to meepo's documentation. Meepo is a event sourcing and broadcasting
platform for database.

This documentation consists of two parts:

1. Meepo PubSub (`meepo.pub` & `meepo.sub`). This part is enough if you
   only needs a simple solution for your database events.

2. Meepo Apps (`meepo.apps`). This part ships with eventsourcing and
   replicator apps for advanced use. You can refer to examples for demo.

Features
========

Meepo can be used to do lots of things, including replication, eventsourcing,
cache refresh/invalidate, real-time analytics etc. The limit is all the tasks
should be row-based, since meepo only gives ``table_action pk`` events.

* Row-based database replication.

  Meepo can be used to replicate data between databases including
  postgres, sqlite, etc.

  Refer to ``examples/repl_db`` script for demo.

* Replicate RDBMS to NoSQL and search engine.

  Meepo can also be used to replicate data changes from RDBMS to redis,
  elasticsearch etc.

  Refer to ``examples/repl_redis`` and ``examples/repl_elasticsearch`` for
  demo.

* Event Sourcing.

  Meepo can log and replay what has happened since some time using a simple
  event sourcing.

  Refer to ``examples/event_sourcing`` for demo.

.. note::

   Meepo can only replicate row based data, which means it DO NOT replicate
   schema changes, or bulk operations.

Meepo use signal to hook into the events of mysql binlog and sqlalchemy, the
hook is very easy to install.

Hook with MySQL's binlog events:

.. code:: python

    from meepo.pub import mysql_pub
    mysql_pub(mysql_dsn)

Hook with SQLAlchemy's events:

.. code:: python

    from meepo.pub import sqlalchemy_pub
    sqlalchemy_pub(session)

Try out the demo scripts in ``example/tutorial`` for how meepo events works.


Installation
============

.. highlight:: bash

:Requirements: **Python 2.x >= 2.7** or **Python 3.x >= 3.2** or **PyPy**

To install the latest released version of Meepo::

    $ pip install meepo

Meepo Pub
=========

Meepo's core concept is based event pubs, it follows mysql row-based binlog
and sqlalchemy events system and shape them into ``table_action pk``
format signals.

Currently there're 2 pubs implemented: ``mysql_pub`` and ``sqlalchemy_pub``.

.. automodule:: meepo.pub
    :members:


Meepo Sub
=========

.. automodule:: meepo.sub.dummy
    :members:

.. automodule:: meepo.sub.nano
    :members:

.. automodule:: meepo.sub.zmq
    :members:


Applications
============

EventSroucing
-------------

.. automodule:: meepo.apps.eventsourcing
    :members:

.. automodule:: meepo.apps.eventsourcing.pub
    :members:

.. automodule:: meepo.apps.eventsourcing.sub
    :members:

.. automodule:: meepo.apps.eventsourcing.event_store
    :members:

.. automodule:: meepo.apps.eventsourcing.prepare_commit
    :members:


Replicator
----------

.. automodule:: meepo.apps.replicator
    :members:
