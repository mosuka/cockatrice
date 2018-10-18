Document APIs
=============


Get Document API
----------------

.. code-block:: text

    GET /rest/<INDEX_NAME>/_doc/<DOC_ID>

* ``<INDEX_NAME>``: The index name.
* ``<DOC_ID>``: The document ID to retrieve.


Index Document API
------------------

.. code-block:: text

    PUT /rest/<INDEX_NAME>/_doc/<DOC_ID>?sync=<SYNC>
    {
      "name": "Cockatrice",
      ...
    }

* ``<INDEX_NAME>``: The index name.
* ``<DOC_ID>``: The document ID to index.
* ``<SYNC>``: Specifies whether to execute the command synchronously or asynchronously. If ``True`` is specified, command will execute synchronously. Default is ``False``, command will execute asynchronously.
* Request Body: JSON formatted fields definition.


Delete Document API
-------------------

.. code-block:: text

    DELETE /rest/<INDEX_NAME>/_doc/<DOC_ID>?sync=<SYNC>

* ``<INDEX_NAME>``: The index name.
* ``<DOC_ID>``: The document ID to delete.
* ``<SYNC>``: Specifies whether to execute the command synchronously or asynchronously. If ``True`` is specified, command will execute synchronously. Default is ``False``, command will execute asynchronously.


Index Documents API
-----------------

.. code-block:: text

    PUT /rest/<INDEX_NAME>/_docs?sync=<SYNC>
    [
      {
        "id": "1",
        "name": "Cockatrice"
      },
      {
        "id": "2",
      ...
    ]

* ``<INDEX_NAME>``: The index name.
* ``<SYNC>``: Specifies whether to execute the command synchronously or asynchronously. If ``True`` is specified, command will execute synchronously. Default is ``False``, command will execute asynchronously.
* Request Body: JSON formatted documents definition.


Delete Documents API
--------------------

.. code-block:: text

    DELETE /rest/<INDEX_NAME>/_docs?sync=<SYNC>
    [
      "1",
      "2",
      ...
    ]

* ``<INDEX_NAME>``: The index name.
* ``<SYNC>``: Specifies whether to execute the command synchronously or asynchronously. If ``True`` is specified, command will execute synchronously. Default is ``False``, command will execute asynchronously.
* Request Body: JSON formatted document ids definition.

