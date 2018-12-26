Document management
===================

Once indices are created, you can update indices.

Index a document
----------------

If you already created an index named ``myindex``, indexing a document by the following command:

.. code-block:: bash

    $ curl -s -X PUT -H "Content-Type:application/json" http://localhost:8080/indices/myindex/documents/1 --data-binary @./example/doc1.json

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "time": 0.0008089542388916016,
      "status": {
        "code": 202,
        "phrase": "Accepted",
        "description": "Request accepted, processing continues off-line"
      }
    }


Get a document
--------------

If you already indexed a document ID ``1`` in ``myindex``, getting a document that specifying ID from ``myindex`` by the following command:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/indices/myindex/documents/1

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "fields": {
        "contributor": "43.225.167.166",
        "id": "1",
        "text": "A search engine is an information retrieval system designed to help find information stored on a computer system. The search results are usually presented in a list and are commonly called hits. Search engines help to minimize the time required to find information and the amount of information which must be consulted, akin to other techniques for managing information overload.\nThe most public, visible form of a search engine is a Web search engine which searches for information on the World Wide Web.",
        "timestamp": "20180704054100",
        "title": "Search engine (computing)"
      },
      "time": 0.014967918395996094,
      "status": {
        "code": 200,
        "phrase": "OK",
        "description": "Request fulfilled, document follows"
      }
    }


Delete a document
-----------------

Deleting a document from ``myindex`` by the following command:

.. code-block:: bash

    $ curl -s -X DELETE http://localhost:8080/indices/myindex/documents/1

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "time": 0.00019788742065429688,
      "status": {
        "code": 202,
        "phrase": "Accepted",
        "description": "Request accepted, processing continues off-line"
      }
    }


Index documents in bulk
-----------------------

Indexing documents in bulk by the following command:

.. code-block:: bash

    $ curl -s -X PUT -H "Content-Type:application/json" http://localhost:8080/indices/myindex/documents --data-binary @./example/bulk_index.json

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "time": 0.05237007141113281,
      "status": {
        "code": 202,
        "phrase": "Accepted",
        "description": "Request accepted, processing continues off-line"
      }
    }


Delete documents in bulk
------------------------

Deleting documents in bulk by the following command:

.. code-block:: bash

    $ curl -s -X DELETE -H "Content-Type:application/json" http://localhost:8080/indices/myindex/documents --data-binary @./example/bulk_delete.json

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "status": {
        "code": 202,
        "description": "Request accepted, processing continues off-line",
        "phrase": "Accepted"
      },
      "time": 0.0012569427490234375
    }
