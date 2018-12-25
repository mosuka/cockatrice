Cluster management
==================

You already know how to start Cockatrice in standalone mode, but that is not fault tolerant. If you need to increase the fault tolerance, bring up a cluster.


Create a cluster
----------------

Cockatrice is easy to bring up the cluster. You can bring up 3-node cluster with static membership by following commands:

.. code-block:: bash

    $ cockatrice server --port=7070 --snapshot-file=/tmp/cockatrice/node1/snapshot.zip --index-dir=/tmp/cockatrice/node1/index --http-port=8080
    $ cockatrice server --port=7071 --snapshot-file=/tmp/cockatrice/node2/snapshot.zip --index-dir=/tmp/cockatrice/node2/index --http-port=8081 --seed-addr=127.0.0.1:7070
    $ cockatrice server --port=7072 --snapshot-file=/tmp/cockatrice/node3/snapshot.zip --index-dir=/tmp/cockatrice/node3/index --http-port=8082 --seed-addr=127.0.0.1:7070

Just add ``--seed-addr`` parameter and start it.

Above example shows each Cockatrice node running on the same host, so each node must listen on different ports. This would not be necessary if each node ran on a different host.

So you have a 3-node cluster. That way you can tolerate the failure of 1 node.

You can check the cluster with the following command:

.. code-block:: bash

    $ cockatrice status --bind-addr=127.0.0.1:7070 | jq .

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "version": "0.3.4",
      "revision": "2c8a3263d0dbe3f8d7b8a03e93e86d385c1de558",
      "self": "127.0.0.1:7070",
      "state": 2,
      "leader": "127.0.0.1:7070",
      "partner_nodes_count": 2,
      "partner_node_status_server_127.0.0.1:7071": 2,
      "partner_node_status_server_127.0.0.1:7072": 2,
      "readonly_nodes_count": 0,
      "unknown_connections_count": 1,
      "log_len": 4,
      "last_applied": 4,
      "commit_idx": 4,
      "raft_term": 1,
      "next_node_idx_count": 2,
      "next_node_idx_server_127.0.0.1:7071": 5,
      "next_node_idx_server_127.0.0.1:7072": 5,
      "match_idx_count": 2,
      "match_idx_server_127.0.0.1:7071": 4,
      "match_idx_server_127.0.0.1:7072": 4,
      "leader_commit_idx": 4,
      "uptime": 281,
      "self_code_version": 0,
      "enabled_code_version": 0
    }



Recommend 3 or more odd number of nodes in the cluster. In failure scenarios, data loss is inevitable, so avoid deploying single nodes.

Once cluster is created, you can create indices. let's create an index to 127.0.0.1:8080 by the following command:

.. code-block:: bash

    $ curl -s -X PUT -H "Content-type: text/x-yaml" --data-binary @./conf/schema.yaml http://localhost:8080/indices/myindex | jq .

If the above command succeeds, same index will be created on all the nodes in the cluster. Check your index on each nodes.

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/indices/myindex | jq .
    $ curl -s -X GET http://localhost:8081/indices/myindex | jq .
    $ curl -s -X GET http://localhost:8082/indices/myindex | jq .

Let's index a document to 127.0.0.1:8080 by the following command:

.. code-block:: bash

    $ curl -s -X PUT -H "Content-Type:application/json" http://localhost:8080/indices/myindex/documents/1 -d @./example/doc1.json | jq .

If the above command succeeds, same document will be indexed on all the nodes in the cluster. Check your document on each nodes.

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/indices/myindex/documents/1 | jq .
    $ curl -s -X GET http://localhost:8081/indices/myindex/documents/1 | jq .
    $ curl -s -X GET http://localhost:8082/indices/myindex/documents/1 | jq .
