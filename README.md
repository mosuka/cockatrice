Cockatrice
==========

# What's Cockatrice?

Cockatrice is the open source search and indexing server written in `Python <https://www.python.org>`_ that provides scalable indexing and search, faceting, hit highlighting and advanced analysis/tokenization capabilities.
Indexing and search are implemented by `Whoosh <https://whoosh.readthedocs.io/en/latest/index.html>`_. Cockatrice provides it via the `RESTful <https://en.wikipedia.org/wiki/Representational_state_transfer>`_ API using `Flask <http://flask.pocoo.org/docs/>`_.
In cluster mode, Cockatrice uses `Raft Consensus Algorithm <https://raft.github.io>`_ by `PySyncObj <https://pysyncobj.readthedocs.io/en/latest/>`_ to achieve consensus across all the instances of the nodes, ensuring that every change made to the system is made to a quorum of nodes.


# Document

https://cockatrice.readthedocs.io/en/latest/


# Requirements

Python 3.x interpreter


# Features

- Full-text search and indexing
- Faceting
- Result highlighting
- Easy deployment
- Bringing up cluster
- Index replication
- An easy-to-use RESTful API
