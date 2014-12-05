#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

# requirements
install_requires = [
    "SQLAlchemy>=0.9.0,<1.0.0",
    "blinker>=1.3,<2.0",
    "mysql-replication>=0.5,<0.6.0",
    "pyketama>=0.2.0",
    "pyzmq>=14.4.1,<15.0.0",
    "redis>=2.10.3,<2.11.0",
]

dev_requires = [
    "flake8>=2.2",
    "pytest>=2.6",
    "sphinx-rtd-theme>=0.1.6",
    "sphinx>=1.2",
    "tox>=1.8",
] + install_requires


setup(name="meepo",
      version=__import__("meepo").__version__,
      description="event sourcing for databases.",
      keywords="eventsourcing event sourcing replication cache elasticsearch",
      author="Lx Yu",
      author_email="i@lxyu.net",
      packages=find_packages(exclude=['docs', 'tests']),
      url="https://github.com/eleme/meepo",
      license="MIT",
      zip_safe=False,
      long_description=open("README.rst").read(),
      install_requires=install_requires,
      extras_require={
          "dev": dev_requires,
      },
      classifiers=[
          "Topic :: Software Development",
          "Development Status :: 3 - Alpha",
          "Intended Audience :: Developers",
          "License :: OSI Approved :: MIT License",
          "Programming Language :: Python :: 2.7",
          "Programming Language :: Python :: 3.3",
          "Programming Language :: Python :: 3.4",
          "Programming Language :: Python :: Implementation :: CPython",
          "Programming Language :: Python :: Implementation :: PyPy",
      ])
