Health check
============

Cockatrice provides a health check endpoint which returns 200 if Cockatrice is live or ready to response to queries.


Liveness probe
--------------

To get the current liveness probe is following:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/liveness

You can see the result in JSON format. The result of the above command is:

.. code-block:: text

    {
      "liveness": true,
      "time": 7.152557373046875e-06,
      "status": {
        "code": 200,
        "phrase": "OK",
        "description": "Request fulfilled, document follows"
      }
    }


Readiness probe
---------------

To get the current readiness probe is following:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/readiness

You can see the result in JSON format. The result of the above command is:

.. code-block:: text

    {
      "readiness": true,
      "time": 1.6927719116210938e-05,
      "status": {
        "code": 200,
        "phrase": "OK",
        "description": "Request fulfilled, document follows"
      }
    }
