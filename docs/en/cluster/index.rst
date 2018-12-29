Cluster
=======

Cockatrice includes the ability to set up a cluster of servers that combines fault tolerance and high availability.


Bring up a cluster
------------------

You already know how to start Cockatrice in standalone mode, but that is not fault tolerant. If you need to increase the fault tolerance, bring up a cluster.

You can easily bring up 3-node cluster with dynamic membership by following commands:

.. code-block:: bash

    $ cockatrice server --port=7070 --snapshot-file=/tmp/cockatrice/node1/snapshot.zip --index-dir=/tmp/cockatrice/node1/index --http-port=8080
    $ cockatrice server --port=7071 --snapshot-file=/tmp/cockatrice/node2/snapshot.zip --index-dir=/tmp/cockatrice/node2/index --http-port=8081 --seed-addr=127.0.0.1:7070
    $ cockatrice server --port=7072 --snapshot-file=/tmp/cockatrice/node3/snapshot.zip --index-dir=/tmp/cockatrice/node3/index --http-port=8082 --seed-addr=127.0.0.1:7070

Start by specifying the existing node in the cluster with the ``--seed-addr`` parameter.

Now you have a 3-nodes cluster. Then you can tolerate the failure of 1 node.

Above example shows each Cockatrice node running on the same host, so each node must listen on different ports. This would not be necessary if each node ran on a different host.


Get Cluster state
-----------------

You will be wondering if the cluster is working properly. In such a case you can retrieve the cluster state with the following command;

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/cluster

The result of the above command can be seen in the JSON format as follows:

.. code-block:: json

    {
      "cluster": {
        "version": "0.3.4",
        "revision": "2c8a3263d0dbe3f8d7b8a03e93e86d385c1de558",
        "self": "localhost:7070",
        "state": 2,
        "leader": "localhost:7070",
        "partner_nodes_count": 2,
        "partner_node_status_server_localhost:7071": 2,
        "partner_node_status_server_localhost:7072": 2,
        "readonly_nodes_count": 0,
        "unknown_connections_count": 0,
        "log_len": 4,
        "last_applied": 4,
        "commit_idx": 4,
        "raft_term": 1,
        "next_node_idx_count": 2,
        "next_node_idx_server_localhost:7071": 5,
        "next_node_idx_server_localhost:7072": 5,
        "match_idx_count": 2,
        "match_idx_server_localhost:7071": 4,
        "match_idx_server_localhost:7072": 4,
        "leader_commit_idx": 4,
        "uptime": 29,
        "self_code_version": 0,
        "enabled_code_version": 0
      },
      "time": 5.91278076171875e-05,
      "status": {
        "code": 200,
        "phrase": "OK",
        "description": "Request fulfilled, document follows"
      }
    }


It is recommended to set an odd number of 3 or more for the number of nodes to bring up the cluster. In failure scenarios, data loss is inevitable, so avoid deploying single node.

Once the cluster is created, you can request that any node in the cluster be created index. The following command request to create index named ``myindex`` to ``localhost:8080``:

.. code-block:: bash

    $ curl -s -X PUT -H "Content-type: application/yaml" --data-binary @./conf/schema.yaml http://localhost:8080/indices/myindex

If the above command succeeds, same index will be created on all the nodes in the cluster. Check your index on each nodes like follows:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/indices/myindex
    $ curl -s -X GET http://localhost:8081/indices/myindex
    $ curl -s -X GET http://localhost:8082/indices/myindex

Similarly, you can request to add any document to any node in the cluster. The following command requests to index documents in the index named ``myindex`` via ``localhost:8080``:

.. code-block:: bash

    $ curl -s -X PUT -H "Content-Type:application/json" http://localhost:8080/indices/myindex/documents/1 -d @./example/doc1.json

If the above command succeeds, same document will be indexed on all the nodes in the cluster. Check your document on each nodes like follows:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/indices/myindex/documents/1
    $ curl -s -X GET http://localhost:8081/indices/myindex/documents/1
    $ curl -s -X GET http://localhost:8082/indices/myindex/documents/1
