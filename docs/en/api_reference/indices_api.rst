Index APIs
==========

The Index API is used to manage individual indices.


Create Index API
----------------

The Create Index API is used to manually create an index in Cockatrice.
The most basic usage is the following:

.. code-block:: text

    PUT /<INDEX_NAME>?sync=<SYNC>
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
* Request Body: YAML formatted schema definition.


Get Index API
-------------

The Get Index API allows to retrieve information about the index.
The most basic usage is the following:

.. code-block:: text

    GET /<INDEX_NAME>

* ``<INDEX_NAME>``: The index name.


Delete Index API
----------------

The Delete Index API allows to delete an existing index.
The most basic usage is the following:

.. code-block:: text

    DELETE /<INDEX_NAME>?sync=<SYNC>

* ``<INDEX_NAME>``: The index name.
* ``<SYNC>``: Specifies whether to execute the command synchronously or asynchronously. If ``True`` is specified, command will execute synchronously. Default is ``False``, command will execute asynchronously.
