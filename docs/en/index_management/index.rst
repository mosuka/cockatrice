Index management
================

You need to create an index after starting Cockatrice. Also you can delete indexes that are no longer needed.


Create an index
---------------

A schema is required to create an index, you need to put the schema in the request. Create an index by the following command:

.. code-block:: bash

    $ curl -s -X PUT -H "Content-type: text/x-yaml" --data-binary @./conf/schema.yaml http://localhost:8080/myindex | jq .

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "status": {
        "code": 202,
        "description": "Request accepted, processing continues off-line",
        "phrase": "Accepted"
      },
      "time": 0.08018112182617188
    }


Get an index
------------

If you created an index, you can retrieve index information by the following command:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/myindex | jq .

The result of the above command is:

.. code-block:: json

    {
      "index": {
        "doc_count": 0,
        "doc_count_all": 0,
        "last_modified": 1541768662.8663597,
        "latest_generation": 0,
        "name": "myindex",
        "storage": {
          "files": [
            "_myindex_0.toc"
          ],
          "folder": "/tmp/cockatrice/node1/index",
          "readonly": false,
          "supports_mmap": true
        },
        "version": -111
      },
      "status": {
        "code": 200,
        "description": "Request fulfilled, document follows",
        "phrase": "OK"
      },
      "time": 0.0016748905181884766
    }


Delete an index
---------------

You can delete indexes that are no longer needed. Delete an index by the following command:

.. code-block:: bash

    $ curl -s -X DELETE http://localhost:8080/myindex | jq .

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "status": {
        "code": 202,
        "description": "Request accepted, processing continues off-line",
        "phrase": "Accepted"
      },
      "time": 0.0006439685821533203
    }
