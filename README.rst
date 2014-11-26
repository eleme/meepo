=====
Meepo
=====

.. image:: http://img.shields.io/travis/eleme/meepo/master.svg?style=flat
   :target: https://travis-ci.org/eleme/meepo

.. image:: http://img.shields.io/pypi/v/meepo.svg?style=flat
   :target: https://pypi.python.org/pypi/meepo

.. image:: http://img.shields.io/pypi/dm/meepo.svg?style=flat
   :target: https://pypi.python.org/pypi/meepo

Meepo is event sourcing and event broadcasting for databases.

Documentation: https://meepo.readthedocs.org/


Installation
============

Install with pip

.. code:: bash

    $ pip install meepo

To use mysql binlog as event source, install additional requires with

.. code:: bash

    $ pip install meepo[mysqlbinlog]


Features
========

Meepo can publish the table events from database to the outside, so it can
be  used to do a lot of interesting things, including:

- cache invalidation

- replication to RDBS / NoSQL / SearchEngine

- event sourcing

- logging and auditing

- realtime analytics

- notifications pushing


Intro
=====

`meepo` is a pubsub system for databases, it follows database table events
from `sqlalchemy` or `mysql binlog`, and publish them to subscribers.

MySQL Pub
---------

`mysql_pub` event flow::

                                                       +---------------------+
                                                       |                     |
                                                   +--->  table_write event  |
                                                   |   |                     |
                                                   |   +---------------------+
                                                   |
    +--------------------+      +---------------+  |
    |                    |      |               |  |   +---------------------+
    |        mysql       |      |   meepo.pub   |  |   |                     |
    |                    +------>               +--+--->  table_update event |
    |  row-based binlog  |      |   mysql_pub   |  |   |                     |
    |                    |      |               |  |   +---------------------+
    +--------------------+      +---------------+  |
                                                   |
                                                   |   +---------------------+
                                                   |   |                     |
                                                   +--->  table_delete event |
                                                       |                     |
                                                       +---------------------+


MySQL Pub use row-based mysql binlog as events source, and transfer them into
table_action events. `mysql_pub` generates an accurate event stream with one
 pk per event.

For example, the  following sql:

.. code-block:: sql

    INSERT INTO test (data) VALUES ('a');
    INSERT INTO test (data) VALUES ('b'), ('c'), ('d');
    UPDATE test SET data = 'aa' WHERE id = 1;
    UPDATE test SET data = 'bb' WHERE id = 2;
    UPDATE test SET data = 'cc' WHERE id != 1;
    DELETE FROM test WHERE id != 1;
    DELETE FROM test WHERE id = 1;

Generates the following events:

::

    test_write 1
    test_write 2
    test_write 3
    test_write 4
    test_update 1
    test_update 2
    test_update 2
    test_update 3
    test_update 4
    test_delete 2
    test_delete 3
    test_delete 4
    test_delete 1


SQLAlchemy Pub
==============

`sqlalchemy_pub` event flow::

    +------------------+
    |                  |
    |    meepo.pub     |
    |                  |
    |  sqlalchemy_pub  |                                       +---------------------+
    |                  |     +-----------------------+         |                     |
    +---------+--------+     |                       |     +--->  table_write event  |
              |              |      before_flush     |     |   |                     |
        hook  |           +-->                       |     |   +---------------------+
              |           |  |  record model states  |     |
    +---------v--------+  |  |                       |     |
    |                  |  |  +-----------+-----------+     |   +---------------------+
    |    sqlalchemy    |  |              |                 |   |                     |
    |                  +--+              |              +------>  table_update event |
    |  session events  |                 |              |  |   |                     |
    |                  |     +-----------v-----------+  |  |   +---------------------+
    +------------------+     |                       |  |  |
                             |     after_commit      |  |  |
                             |                       +--+  |   +---------------------+
                             |  record model states  |     |   |                     |
                             |                       |     +--->  table_delete event |
                             +-----------------------+         |                     |
                                                               +---------------------+



`SQLAlchemy` is a ORM layer above database, it uses `session` to maintain
model instances states before the data flush to database, and flush them to
database in commit.

So `meepo` will hook into the event system, record all the instances in
`session.new`, `session.dirty`, `session.deleted` in `before_flush` event,
then publish the table_action event after commit issued.

For example, the  following code:

.. code-block:: python

    class Test(Base):
        __tablename__ = "test"
        id = Column(Integer, primary_key=True)
        data = Column(String)

    t_1 = Test(id=1, data='a')
    session.add(t_1)
    session.commit()

    t_2 = Test(id=2, data='b')
    t_3 = Test(id=3, data='c')
    session.add(t_2)
    session.add(t_3)
    session.add(t_4)
    session.commit()

    t_2.data = "x"
    session.commit()

    session.delete(t_3)
    session.commit()

Generates the following events:

::

    test_write 1
    test_write 2
    test_write 3
    test_update 2
    test_delete 3


Examples
========

Dummy prints all database events.

Use :code:`mprint` with mysql dsn with row-based binlog enabled.

.. code:: bash

    $ mprint -m "mysql://user:pwd@mysql_server/"


Contribute
==========

1. Fork the repo and make changes.

2. Write a test which shows a bug was fixed or the feature works as expected.

3. Make sure travis-ci test succeed.

4. Send pull request.
