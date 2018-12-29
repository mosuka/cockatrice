.. cockatrice documentation master file, created by
   sphinx-quickstart on Tue Oct  2 13:26:53 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Cockatrice |release| documentation
==================================

Cockatrice is the open source search and indexing server written in `Python <https://www.python.org>`_ that provides scalable indexing, search and advanced analysis/tokenization capabilities.

Features
--------

Cockatrice provides Indexing and search functionality implemented by `Whoosh <https://whoosh.readthedocs.io/en/latest/index.html>`_ via the `RESTful <https://en.wikipedia.org/wiki/Representational_state_transfer>`_ API based on `Flask <http://flask.pocoo.org/docs/>`_ and it could bring up the cluster with `Raft Consensus Algorithm <https://raft.github.io>`_ by `PySyncObj <https://pysyncobj.readthedocs.io/en/latest/>`_.

* Easy deployment
* Full-text search and indexing
* Per field similarity (scoring/ranking model) definition.
* Bringing up cluster
* Index replication
* Create indices snapshot
* Recover from indices snapshot
* Synchronize indices from leader node
* An easy-to-use RESTful API


Source Codes
------------

https://github.com/mosuka/cockatrice


Requirements
------------

Python 3.x interpreter


Contents
--------

.. toctree::
   :maxdepth: 2

   getting_started/index
   schema/index
   scoring/index
   cluster/index
   monitor/index
   health_check/index
   api_reference/index


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
