Document management
===================

Once indices are created, you can update indices.

Index a document
----------------

If you already created an index named ``myindex``, indexing a document by the following command:

.. code-block:: bash

    $ curl -s -X PUT -H "Content-Type:application/json" http://localhost:8080/myindex/_doc/1 -d @./example/doc1.json | jq .

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "status": {
        "code": 202,
        "description": "Request accepted, processing continues off-line",
        "phrase": "Accepted"
      },
      "time": 0.00015020370483398438
    }


Get a document
--------------

If you already indexed a document ID ``1`` in ``myindex``, getting a document that specifying ID from ``myindex`` by the following command:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/myindex/_doc/1 | jq .

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "doc": {
        "fields": {
          "contributor": "43.225.167.166",
          "id": "1",
          "text": "A search engine is an information retrieval system designed to help find information stored on a computer system. The search results are usually presented in a list and are commonly called hits. Search engines help to minimize the time required to find information and the amount of information which must be consulted, akin to other techniques for managing information overload. The most public, visible form of a search engine is a Web search engine which searches for information on the World Wide Web.",
          "timestamp": "20180704054100",
          "title": "Search engine (computing)"
        }
      },
      "status": {
        "code": 200,
        "description": "Request fulfilled, document follows",
        "phrase": "OK"
      },
      "time": 0.011947870254516602
    }


Delete a document
-----------------

Deleting a document from ``myindex`` by the following command:

.. code-block:: bash

    $ curl -s -X DELETE http://localhost:8080/myindex/_doc/1 | jq .

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "status": {
        "code": 202,
        "description": "Request accepted, processing continues off-line",
        "phrase": "Accepted"
      },
      "time": 6.699562072753906e-05
    }


Index documents in bulk
-----------------------

Indexing documents in bulk by the following command:

.. code-block:: bash

    $ curl -s -X PUT -H "Content-Type:application/json" http://localhost:8080/myindex/_docs -d @./example/bulk_index.json | jq .

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "status": {
        "code": 202,
        "description": "Request accepted, processing continues off-line",
        "phrase": "Accepted"
      },
      "time": 0.00018596649169921875
    }


Delete documents in bulk
------------------------

Deleting documents in bulk by the following command:

.. code-block:: bash

    $ curl -s -X DELETE -H "Content-Type:application/json" http://localhost:8080/myindex/_docs -d @./example/bulk_delete.json | jq .

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "status": {
        "code": 202,
        "description": "Request accepted, processing continues off-line",
        "phrase": "Accepted"
      },
      "time": 0.00232696533203125
    }
