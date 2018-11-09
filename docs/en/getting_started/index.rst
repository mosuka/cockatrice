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

    $ cockatrice server --index-dir=/tmp/cockatrice/node1/index

This will start Cockatrice, listening on default port (8080).

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/

You can see the result in plain text format. The result of the above command is:

.. code-block:: text

    cockatrice 0.3.0 is running.
