Index APIs
==========

The Index API is used to manage individual indices.


Put Index API
-------------

The Create Index API is used to manually create an index in Cockatrice.
The most basic usage is the following:

.. code-block:: text

    PUT /indices/<INDEX_NAME>?sync=<SYNC>&output=<OUTPUT>
    ---
    schema:
      id:
        field_type: id
        args:
          unique: true
          stored: true
    ...

* ``<INDEX_NAME>``: The index name.
* ``<SYNC>``: Specifies whether to execute the command synchronously or asynchronously. If ``True`` is specified, command will execute synchronously. Default is ``False``, command will execute asynchronously.
* ``<OUTPUT>``: The output format. ``json`` or ``yaml``. Default is ``json``.
* Request Body: JSON or YAML formatted schema definition.


Get Index API
-------------

The Get Index API allows to retrieve information about the index.
The most basic usage is the following:

.. code-block:: text

    GET /indices/<INDEX_NAME>?output=<OUTPUT>

* ``<INDEX_NAME>``: The index name.
* ``<OUTPUT>``: The output format. ``json`` or ``yaml``. Default is ``json``.


Delete Index API
----------------

The Delete Index API allows to delete an existing index.
The most basic usage is the following:

.. code-block:: text

    DELETE /indices/<INDEX_NAME>?sync=<SYNC>&output=<OUTPUT>

* ``<INDEX_NAME>``: The index name.
* ``<SYNC>``: Specifies whether to execute the command synchronously or asynchronously. If ``True`` is specified, command will execute synchronously. Default is ``False``, command will execute asynchronously.
* ``<OUTPUT>``: The output format. ``json`` or ``yaml``. Default is ``json``.
