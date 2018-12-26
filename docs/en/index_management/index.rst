Index management
================

You need to create an index after starting Cockatrice. Also you can delete indexes that are no longer needed.


Create an index
---------------

Creating an index needs to put the schema in the request like the following command:

.. code-block:: bash

    $ curl -s -X PUT -H 'Content-type: application/yaml' --data-binary @./example/schema.yaml http://localhost:8080/indices/myindex

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "time": 0.30895185470581055,
      "status": {
        "code": 202,
        "phrase": "Accepted",
        "description": "Request accepted, processing continues off-line"
      }
    }


Get an index
------------

If you created an index, you can retrieve an index information by the following command:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/indices/myindex

The result of the above command is:

.. code-block:: json

    {
      "index": {
        "name": "myindex",
        "doc_count": 0,
        "doc_count_all": 0,
        "last_modified": 1545792828.5970383,
        "latest_generation": 0,
        "version": -111,
        "storage": {
          "folder": "/tmp/cockatrice/index",
          "supports_mmap": true,
          "readonly": false,
          "files": [
            "_myindex_0.toc"
          ]
        }
      },
      "time": 0.0013620853424072266,
      "status": {
        "code": 200,
        "phrase": "OK",
        "description": "Request fulfilled, document follows"
      }
    }


Delete an index
---------------

You can delete indexes that are no longer needed. Delete an index by the following command:

.. code-block:: bash

    $ curl -s -X DELETE http://localhost:8080/indices/myindex

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "time": 0.0001461505889892578,
      "status": {
        "code": 202,
        "phrase": "Accepted",
        "description": "Request accepted, processing continues off-line"
      }
    }
