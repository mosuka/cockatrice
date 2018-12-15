Getting Started
===============

Installation of Cockatrice on Unix-compatible or Windows servers generally requires `Python <https://www.python.org>`_ interpreter and `pip <https://pip.pypa.io>`_ command.


Installing Cockatrice
---------------------

Cockatrice is registered to `PyPi <https://pypi.org/project/cockatrice/>`_ now, so you can just run following command:

.. code-block:: bash

    $ pip install cockatrice


Starting Cockatrice
-------------------

Cockatrice includes a command line interface tool called bin/cockatrice. This tool allows you to start Cockatrice in your system.

To use it to start Cockatrice you can simply enter:

.. code-block:: bash

    $ cockatrice server

This will start Cockatrice, listening on default port (8080).

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/

You can see the result in plain text format. The result of the above command is:

.. code-block:: json

    {
      "node": {
        "commit_idx": 2,
        "enabled_code_version": 0,
        "last_applied": 2,
        "leader": "127.0.0.1:7070",
        "leader_commit_idx": 2,
        "log_len": 2,
        "match_idx_count": 0,
        "next_node_idx_count": 0,
        "partner_nodes_count": 0,
        "raft_term": 1,
        "readonly_nodes_count": 0,
        "revision": "2c8a3263d0dbe3f8d7b8a03e93e86d385c1de558",
        "self": "127.0.0.1:7070",
        "self_code_version": 0,
        "state": 2,
        "unknown_connections_count": 0,
        "uptime": 17,
        "version": "0.3.4"
      },
      "status": {
        "code": 200,
        "description": "Request fulfilled, document follows",
        "phrase": "OK"
      },
      "time": 0.00010395050048828125
    }
