Monitoring
=====================

The ``/metrics`` endpoint provides access to all the metrics. Cockatrice outputs metrics in `Prometheus exposition format <https://prometheus.io/docs/instrumenting/exposition_formats/>`_.


Get metrics
-----------

You can get metrics by the following command:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/metrics

You can see the result in Prometheus exposition format. The result of the above command is:

.. code-block:: text

    # HELP cockatrice_http_requests_total The number of requests.
    # TYPE cockatrice_http_requests_total counter
    cockatrice_http_requests_total{endpoint="/myindex",method="PUT",status_code="202"} 1.0
    cockatrice_http_requests_total{endpoint="/myindex/_docs",method="PUT",status_code="202"} 1.0
    # HELP cockatrice_http_requests_bytes_total A summary of the invocation requests bytes.
    # TYPE cockatrice_http_requests_bytes_total counter
    cockatrice_http_requests_bytes_total{endpoint="/myindex",method="PUT"} 7376.0
    cockatrice_http_requests_bytes_total{endpoint="/myindex/_docs",method="PUT"} 3909.0
    # HELP cockatrice_http_responses_bytes_total A summary of the invocation responses bytes.
    # TYPE cockatrice_http_responses_bytes_total counter
    cockatrice_http_responses_bytes_total{endpoint="/myindex",method="PUT"} 135.0
    cockatrice_http_responses_bytes_total{endpoint="/myindex/_docs",method="PUT"} 137.0
    # HELP cockatrice_http_requests_duration_seconds The invocation duration in seconds.
    # TYPE cockatrice_http_requests_duration_seconds histogram
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex",le="0.005",method="PUT"} 0.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex",le="0.01",method="PUT"} 0.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex",le="0.025",method="PUT"} 0.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex",le="0.05",method="PUT"} 0.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex",le="0.075",method="PUT"} 0.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex",le="0.1",method="PUT"} 0.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex",le="0.25",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex",le="0.5",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex",le="0.75",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex",le="1.0",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex",le="2.5",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex",le="5.0",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex",le="7.5",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex",le="10.0",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex",le="+Inf",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_count{endpoint="/myindex",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_sum{endpoint="/myindex",method="PUT"} 0.22063422203063965
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex/_docs",le="0.005",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex/_docs",le="0.01",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex/_docs",le="0.025",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex/_docs",le="0.05",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex/_docs",le="0.075",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex/_docs",le="0.1",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex/_docs",le="0.25",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex/_docs",le="0.5",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex/_docs",le="0.75",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex/_docs",le="1.0",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex/_docs",le="2.5",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex/_docs",le="5.0",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex/_docs",le="7.5",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex/_docs",le="10.0",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_bucket{endpoint="/myindex/_docs",le="+Inf",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_count{endpoint="/myindex/_docs",method="PUT"} 1.0
    cockatrice_http_requests_duration_seconds_sum{endpoint="/myindex/_docs",method="PUT"} 0.0020329952239990234
    # HELP cockatrice_index_documents The number of documents.
    # TYPE cockatrice_index_documents gauge
    cockatrice_index_documents{index_name="myindex"} 5.0
