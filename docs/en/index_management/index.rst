Index management
================

| First of all, you need to create an index after starting Cockatrice.
| You can delete indexes that are no longer needed.

Create an index
---------------

A schema is required to create an index, you need to put the schema in the request. Create an index by the following command:

.. code-block:: bash

    $ curl -s -X PUT -H "Content-type: text/x-yaml" --data-binary @./conf/schema.yaml http://localhost:8080/rest/myindex | jq .

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "status": {
        "code": 202,
        "description": "Request accepted, processing continues off-line",
        "phrase": "Accepted"
      },
      "time": 21.88803505897522
    }


Delete an index
---------------

You can delete indexes that are no longer needed. Delete an index by the following command:

.. code-block:: bash

    $ curl -s -X DELETE http://localhost:8080/rest/myindex | jq .

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
