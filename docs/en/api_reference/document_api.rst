Document APIs
=============


Get Document API
----------------

.. code-block:: text

    GET /indices/<INDEX_NAME>/documents/<DOC_ID>?output=<OUTPUT>

* ``<INDEX_NAME>``: The index name.
* ``<DOC_ID>``: The document ID to retrieve.
* ``<OUTPUT>``: The output format. ``json`` or ``yaml``. Default is ``json``.


Put Document API
------------------

.. code-block:: text

    PUT /indices/<INDEX_NAME>/documents/<DOC_ID>?sync=<SYNC>&output=<OUTPUT>
    {
      "name": "Cockatrice",
      ...
    }

* ``<INDEX_NAME>``: The index name.
* ``<DOC_ID>``: The document ID to index.
* ``<SYNC>``: Specifies whether to execute the command synchronously or asynchronously. If ``True`` is specified, command will execute synchronously. Default is ``False``, command will execute asynchronously.
* ``<OUTPUT>``: The output format. ``json`` or ``yaml``. Default is ``json``.
* Request Body: JSON or YAML formatted fields definition.


Delete Document API
-------------------

.. code-block:: text

    DELETE /indices/<INDEX_NAME>/documents/<DOC_ID>?sync=<SYNC>&output=<OUTPUT>

* ``<INDEX_NAME>``: The index name.
* ``<DOC_ID>``: The document ID to delete.
* ``<SYNC>``: Specifies whether to execute the command synchronously or asynchronously. If ``True`` is specified, command will execute synchronously. Default is ``False``, command will execute asynchronously.
* ``<OUTPUT>``: The output format. ``json`` or ``yaml``. Default is ``json``.


Put Documents API
-------------------

.. code-block:: text

    PUT /indices/<INDEX_NAME>/documents?sync=<SYNC>&output=<OUTPUT>
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
* ``<OUTPUT>``: The output format. ``json`` or ``yaml``. Default is ``json``.
* Request Body: JSON or YAML formatted documents definition.


Delete Documents API
--------------------

.. code-block:: text

    DELETE /indices/<INDEX_NAME>/documents?sync=<SYNC>&output=<OUTPUT>
    [
      "1",
      "2",
      ...
    ]

* ``<INDEX_NAME>``: The index name.
* ``<SYNC>``: Specifies whether to execute the command synchronously or asynchronously. If ``True`` is specified, command will execute synchronously. Default is ``False``, command will execute asynchronously.
* ``<OUTPUT>``: The output format. ``json`` or ``yaml``. Default is ``json``.
* Request Body: JSON or YAML formatted document ids definition.

