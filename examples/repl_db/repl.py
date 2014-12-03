# -*- coding: utf-8 -*-


import logging

import click
from blinker import signal
import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import SQLAlchemyError

from meepo.pub import mysql_pub


def repl_db_sub(master_dsn, slave_dsn, tables):
    """Database replication subscriber.

    The function will subscribe to the event sourcing pk stream, retrive rows
    from master based pk and then update the slave.
    """
    logger = logging.getLogger("meepo.sub.replicate_sub")

    # sqlalchemy reflection
    logger.info("reflecting master database: {}".format(master_dsn))
    master_engine = sa.create_engine(master_dsn)
    master_base = automap_base()
    master_base.prepare(engine=master_engine, reflect=True)
    MasterSession = scoped_session(sessionmaker(bind=master_engine))

    logger.info("reflecting slave database: {}".format(slave_dsn))
    slave_engine = sa.create_engine(slave_dsn)
    slave_base = automap_base()
    slave_base.prepare(engine=slave_engine, reflect=True)
    SlaveSession = scoped_session(sessionmaker(bind=slave_engine))

    def _write_by_pk(name, pk):
        """Copy row from master to slave based on pk
        """
        MasterModel = master_base.classes[name]
        obj = MasterSession.query(MasterModel).get(pk)
        if not obj:
            logger.error("pk for {} not found in master: {}".format(name, pk))
            return

        SlaveModel = slave_base.classes[name]
        columns = [c.name for c in SlaveModel.__table__.columns]
        s_obj = SlaveModel(**{k: v
                              for k, v in obj.__dict__.items()
                              if k in columns})
        SlaveSession.add(s_obj)

        try:
            SlaveSession.commit()
        except SQLAlchemyError as e:
            SlaveSession.rollback()
            logger.exception(e)

        # cleanup
        MasterSession.close()
        SlaveSession.close()

    def _update_by_pk(name, pk):
        """Update row from master to slave based on pk
        """
        MasterModel = master_base.classes[name]
        obj = MasterSession.query(MasterModel).get(pk)

        SlaveModel = slave_base.classes[name]
        s_obj = SlaveSession.query(SlaveModel).get(pk)
        if not s_obj:
            return _write_by_pk(name, pk)

        columns = [c.name for c in SlaveModel.__table__.columns]
        for col in columns:
            try:
                val = getattr(obj, col)
            except AttributeError as e:
                continue
            setattr(s_obj, col, val)

        try:
            SlaveSession.commit()
        except SQLAlchemyError as e:
            SlaveSession.rollback()
            logger.exception(e)

        # cleanup
        MasterSession.close()
        SlaveSession.close()

    def _delete_by_pk(name, pk):
        """Copy row from slave based on pk
        """
        Model = slave_base.classes[name]
        obj = SlaveSession.query(Model).get(pk)
        if obj:
            SlaveSession.delete(obj)
        SlaveSession.commit()

        # cleanup
        SlaveSession.close()

    def _sub(table):

        def _sub_write(pk):
            logger.info("repl_db {}_write: {}".format(table, pk))
            _write_by_pk(table, pk)
        signal("%s_write" % table).connect(_sub_write, weak=False)

        def _sub_update(pk):
            logger.info("repl_db {}_update: {}".format(table, pk))
            _update_by_pk(table, pk)
        signal("%s_update" % table).connect(_sub_update, weak=False)

        def _sub_delete(pk):
            logger.info("repl_db {}_delete: {}".format(table, pk))
            _delete_by_pk(table, pk)
        signal("%s_delete" % table).connect(_sub_delete, weak=False)

    tables = (t for t in tables if t in slave_base.classes.keys())
    for table in tables:
        _sub(table)


@click.command()
@click.option("-b", "--blocking", is_flag=True, default=False)
@click.option('-m', '--master_dsn')
@click.option('-s', '--slave_dsn')
@click.argument('tables', nargs=-1)
def main(master_dsn, slave_dsn, tables, blocking=False):
    """DB Replication app.

    This script will replicate data from mysql master to other databases(
    including mysql, postgres, sqlite).

    This script only support a very limited replication:
    1. data only. The script only replicates data, so you have to make sure
    the  tables already exists in slave db.
    2. pk only. The script replicate data by pk, when a row_pk changed, it
    retrieve it from master and write in to slave.

    :param master_dsn: mysql dsn with row-based binlog enabled.
    :param slave_dsn: slave dsn, most databases supported including mysql,
    postgres, sqlite etc.
    :param tables: the tables need to be replicated
    :param blocking: by default, the script only reads existing binlog,
    replicate them and exit. if  set to True,  this script will run as a
    daemon and wait for more mysql binlog and do replicates.
    """
    # currently only supports mysql master
    assert master_dsn.startswith("mysql")

    logger = logging.getLogger(__name__)
    logger.info("replicating tables: %s" % ", ".join(tables))

    repl_db_sub(master_dsn, slave_dsn, tables)
    mysql_pub(master_dsn, blocking=blocking)


if __name__ == '__main__':
    main()
