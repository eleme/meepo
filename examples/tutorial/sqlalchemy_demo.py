# -*- coding: utf-8 -*-

"""
meepo_examples.tutorial.sqlalchemy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A demo script on how to use meepo with sqlalchemy.
"""

import logging

import click

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

from meepo.logutils import setup_logger
setup_logger()
logger = logging.getLogger("meepo_examples.tutorial.sqlalchemy")

Base = declarative_base()


class Test(Base):
    __tablename__ = "test"

    id = sa.Column(sa.Integer, primary_key=True)
    data = sa.Column(sa.String)


def session_prepare(dsn):
    engine = sa.create_engine(dsn)
    session = scoped_session(sessionmaker(bind=engine))

    engine.execute("DROP TABLE IF EXISTS test;")
    engine.execute("""
    CREATE TABLE test (
        id INT NOT NULL,
        data VARCHAR (256) NOT NULL,
        PRIMARY KEY (id)
    );""")
    logger.info("table created.")

    return session


def sa_demo(session):
    t_1 = Test(id=1, data='a')
    session.add(t_1)
    session.commit()

    t_2 = Test(id=2, data='b')
    t_3 = Test(id=3, data='c')
    session.add(t_2)
    session.add(t_3)
    session.commit()

    t_2.data = "x"
    session.commit()

    session.delete(t_3)
    session.commit()


def main():
    dsn = "sqlite:///sa_demo.db"
    session = session_prepare(dsn)

    from meepo.sub import print_sub
    print_sub(["test"])

    from meepo.pub import sqlalchemy_pub
    sqlalchemy_pub(session)

    sa_demo(session)


if __name__ == "__main__":
    main()
