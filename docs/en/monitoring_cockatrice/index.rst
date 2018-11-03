Monitoring Cockatrice
=====================

The ``/metrics`` endpoint provides access to all the metrics. Cockatrice outputs metrics in `Prometheus <https://prometheus.io>`_ exposition format.


Get metrics
-----------

If you already started a cockatrice, you can get metrics by the following command:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/metrics

You can see the result in Prometheus exposition format. The result of the above command is:

.. code-block:: text

    # HELP cockatrice_indexhttpserver_requests_total The number of requests.
    # TYPE cockatrice_indexhttpserver_requests_total counter
    cockatrice_indexhttpserver_requests_total{endpoint="/metrics",method="GET",status_code="200"} 1.0
    # HELP cockatrice_indexhttpserver_requests_bytes_total A summary of the invocation requests bytes.
    # TYPE cockatrice_indexhttpserver_requests_bytes_total counter
    cockatrice_indexhttpserver_requests_bytes_total{endpoint="/metrics",method="GET"} 0.0
    # HELP cockatrice_indexhttpserver_responses_bytes_total A summary of the invocation responses bytes.
    # TYPE cockatrice_indexhttpserver_responses_bytes_total counter
    cockatrice_indexhttpserver_responses_bytes_total{endpoint="/metrics",method="GET"} 874.0
    # HELP cockatrice_indexhttpserver_requests_duration_seconds The invocation duration in seconds.
    # TYPE cockatrice_indexhttpserver_requests_duration_seconds histogram
    cockatrice_indexhttpserver_requests_duration_seconds_bucket{endpoint="/metrics",le="0.005",method="GET"} 0.0
    cockatrice_indexhttpserver_requests_duration_seconds_bucket{endpoint="/metrics",le="0.01",method="GET"} 0.0
    cockatrice_indexhttpserver_requests_duration_seconds_bucket{endpoint="/metrics",le="0.025",method="GET"} 0.0
    cockatrice_indexhttpserver_requests_duration_seconds_bucket{endpoint="/metrics",le="0.05",method="GET"} 1.0
    cockatrice_indexhttpserver_requests_duration_seconds_bucket{endpoint="/metrics",le="0.075",method="GET"} 1.0
    cockatrice_indexhttpserver_requests_duration_seconds_bucket{endpoint="/metrics",le="0.1",method="GET"} 1.0
    cockatrice_indexhttpserver_requests_duration_seconds_bucket{endpoint="/metrics",le="0.25",method="GET"} 1.0
    cockatrice_indexhttpserver_requests_duration_seconds_bucket{endpoint="/metrics",le="0.5",method="GET"} 1.0
    cockatrice_indexhttpserver_requests_duration_seconds_bucket{endpoint="/metrics",le="0.75",method="GET"} 1.0
    cockatrice_indexhttpserver_requests_duration_seconds_bucket{endpoint="/metrics",le="1.0",method="GET"} 1.0
    cockatrice_indexhttpserver_requests_duration_seconds_bucket{endpoint="/metrics",le="2.5",method="GET"} 1.0
    cockatrice_indexhttpserver_requests_duration_seconds_bucket{endpoint="/metrics",le="5.0",method="GET"} 1.0
    cockatrice_indexhttpserver_requests_duration_seconds_bucket{endpoint="/metrics",le="7.5",method="GET"} 1.0
    cockatrice_indexhttpserver_requests_duration_seconds_bucket{endpoint="/metrics",le="10.0",method="GET"} 1.0
    cockatrice_indexhttpserver_requests_duration_seconds_bucket{endpoint="/metrics",le="+Inf",method="GET"} 1.0
    cockatrice_indexhttpserver_requests_duration_seconds_count{endpoint="/metrics",method="GET"} 1.0
    cockatrice_indexhttpserver_requests_duration_seconds_sum{endpoint="/metrics",method="GET"} 0.041642189025878906
