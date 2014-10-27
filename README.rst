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

Meepo can publish the database events to outside world, so it can be used to
do a lot of interesting things with its pubsub pattern, including:

- cache invalidation

- replication to RDBS / NoSQL / SearchEngine

- event sourcing

- logging and auditing

- realtime analytics

- notifications pushing


Intro
=====

Meepo use a pubsub pattern to follow database events from sqlalchemy or mysql
binlog then publish them to outside.

Meepo uses blinker to connect its PUBs and SUBs, which will transform database
events to :code:`{table}_{action}` signals with primary keys info.

Events demo:

- when a user make an order with id 1234, :code:`order_write` signal
  with :code:`1234` will be triggered.

- when the status of order#1234 changed, :code:`order_update` signal
  with :code:`1234` will be triggered.

- when user deleted the order, :code:`order_delete` signal with :code:`1234`
  will be triggered.

For every signals, you can add multiple subscribers or customize your own.

Refer to :code:`meepo/apps/` for more examples.


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
