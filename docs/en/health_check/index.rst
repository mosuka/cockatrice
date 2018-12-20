Health check
============

Cockatrice provides a health endpoint which returns 200 if Cockatrice is live or ready to response to queries.


Liveness probe
--------------

To get the current liveness probe is following:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/health/liveness | jq .

You can see the result in JSON format. The result of the above command is:

.. code-block:: text

    {
      "liveness": true,
      "status": {
        "code": 200,
        "description": "Request fulfilled, document follows",
        "phrase": "OK"
      },
      "time": 3.123283386230469e-05
    }


Readiness probe
---------------

To get the current readiness probe is following:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/health/readiness | jq .

You can see the result in JSON format. The result of the above command is:

.. code-block:: text

    {
      "readiness": true,
      "status": {
        "code": 200,
        "description": "Request fulfilled, document follows",
        "phrase": "OK"
      },
      "time": 4.124641418457031e-05
    }
