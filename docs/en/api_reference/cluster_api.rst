Cluster APIs
============

Get Cluster API
---------------

.. code-block:: text

    GET /cluster?output=<OUTPUT>

* ``<OUTPUT>``: The output format. ``json`` or ``yaml``. Default is ``json``.


Add Node API
------------

.. code-block:: text

    PUT /cluster/<NODE_NAME>?output=<OUTPUT>

* ``<NODE_NAME>``: The node name.
* ``<OUTPUT>``: The output format. ``json`` or ``yaml``. Default is ``json``.


Delete Node API
---------------

.. code-block:: text

    DELETE /cluster/<NODE_NAME>?output=<OUTPUT>

* ``<NODE_NAME>``: The node name.
* ``<OUTPUT>``: The output format. ``json`` or ``yaml``. Default is ``json``.
