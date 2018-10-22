Cluster management
==================

You already know to start Cockatrice in standalone mode, but that is not fault tolerant. If you need to increase the fault tolerance, bring up Cockatrice cluster.


Create a cluster
----------------

Cockatrice is easy to bring up the cluster. You can bring up 3-node cluster with static membership by following commands:

.. code-block:: bash

    $ cockatrice server --http-port=8080 --bind-addr=127.0.0.1:7070 --peer-addr=127.0.0.1:7071 --peer-addr=127.0.0.1:7072 --index-dir=/tmp/cockatrice/node1/index --dump-file=/tmp/cockatrice/node1/raft/data.dump
    $ cockatrice server --http-port=8081 --bind-addr=127.0.0.1:7071 --peer-addr=127.0.0.1:7070 --peer-addr=127.0.0.1:7072 --index-dir=/tmp/cockatrice/node2/index --dump-file=/tmp/cockatrice/node2/raft/data.dump
    $ cockatrice server --http-port=8082 --bind-addr=127.0.0.1:7072 --peer-addr=127.0.0.1:7070 --peer-addr=127.0.0.1:7071 --index-dir=/tmp/cockatrice/node3/index --dump-file=/tmp/cockatrice/node3/raft/data.dump

Above example shows each Cockatrice node running on the same host, so each node must listen on different ports. This would not be necessary if each node ran on a different host.

So you have a 3-node cluster. That way you can tolerate the failure of 1 node.

You can check the cluster with the following command:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/rest/_cluster | jq .

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "cluster_status": {
        "commit_idx": 4,
        "enabled_code_version": 0,
        "last_applied": 4,
        "leader": "127.0.0.1:7070",
        "leader_commit_idx": 4,
        "log_len": 3,
        "match_idx_count": 2,
        "match_idx_server_127.0.0.1:7071": 4,
        "match_idx_server_127.0.0.1:7072": 4,
        "next_node_idx_count": 2,
        "next_node_idx_server_127.0.0.1:7071": 5,
        "next_node_idx_server_127.0.0.1:7072": 5,
        "partner_node_status_server_127.0.0.1:7071": 2,
        "partner_node_status_server_127.0.0.1:7072": 2,
        "partner_nodes_count": 2,
        "raft_term": 1,
        "readonly_nodes_count": 0,
        "revision": "2c8a3263d0dbe3f8d7b8a03e93e86d385c1de558",
        "self": "127.0.0.1:7070",
        "self_code_version": 0,
        "state": 2,
        "unknown_connections_count": 0,
        "uptime": 159,
        "version": "0.3.4"
      },
      "status": {
        "code": 200,
        "description": "Request fulfilled, document follows",
        "phrase": "OK"
      },
      "time": 0.0001499652862548828
    }

Recommend 3 or more odd number of nodes in the cluster. In failure scenarios, data loss is inevitable, so avoid deploying single nodes.

Once cluster is created, you can create indices. let's create an index to 127.0.0.1:8080 by the following command:

.. code-block:: bash

    $ curl -s -X PUT -H "Content-type: text/x-yaml" --data-binary @./conf/schema.yaml http://localhost:8080/rest/myindex | jq .

If the above command succeeds, same index will be created on all the nodes in the cluster. Check your index on each nodes.

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/rest/myindex | jq .
    $ curl -s -X GET http://localhost:8081/rest/myindex | jq .
    $ curl -s -X GET http://localhost:8082/rest/myindex | jq .

Let's index a document to 127.0.0.1:8080 by the following command:

.. code-block:: bash

    $ curl -s -X PUT -H "Content-Type:application/json" http://localhost:8080/rest/myindex/_doc/1 -d @./example/doc1.json | jq .

If the above command succeeds, same document will be indexed on all the nodes in the cluster. Check your document on each nodes.

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/rest/myindex/_doc/1 | jq .
    $ curl -s -X GET http://localhost:8081/rest/myindex/_doc/1 | jq .
    $ curl -s -X GET http://localhost:8082/rest/myindex/_doc/1 | jq .


Dynamic Membership change
-------------------------

Dynamic membership change allows you to add or remove nodes from your cluster without cluster restart.
This section describes how to scale the cluster. Let's start first node by the following command:

.. code-block:: bash

    $ cockatrice server --http-port=8080 --bind-addr=127.0.0.1:7070 --index-dir=/tmp/cockatrice/node1/index --dump-file=/tmp/cockatrice/node1/raft/data.dump

Then, call Node API with new node name on one of the existing nodes.

.. code-block:: bash

    $ curl -s -X PUT http://localhost:8080/rest/_node?node=127.0.0.1:7071 | jq .

If the above command succeeds, you can launch new node with correct initial peers:

.. code-block:: bash

    $ cockatrice server --http-port=8081 --bind-addr=127.0.0.1:7071 --index-dir=/tmp/cockatrice/node2/index --dump-file=/tmp/cockatrice/node2/raft/data.dump --peer-addr=127.0.0.1:7070

Recommend 3 or more odd number of nodes in the cluster due to avoid split brain. You should launch one more new node with correct initial peers:

.. code-block:: bash

    $ curl -s -X PUT http://localhost:8080/rest/_node?node=127.0.0.1:7072 | jq .
    $ cockatrice server --http-port=8082 --bind-addr=127.0.0.1:7072 --index-dir=/tmp/cockatrice/node3/index --dump-file=/tmp/cockatrice/node3/raft/data.dump --peer-addr=127.0.0.1:7070 --peer-addr=127.0.0.1:7071
