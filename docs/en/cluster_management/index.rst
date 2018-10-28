Cluster management
==================

You already know how to start Cockatrice in standalone mode, but that is not fault tolerant. If you need to increase the fault tolerance, bring up a cluster.


Create a cluster with static membership
---------------------------------------

Cockatrice is easy to bring up the cluster. You can bring up 3-node cluster with static membership by following commands:

.. code-block:: bash

    $ cockatrice server --bind-addr=127.0.0.1:7070 --peer-addr=127.0.0.1:7071 --peer-addr=127.0.0.1:7072 --dump-file=/tmp/cockatrice/node1/raft/data.dump --index-dir=/tmp/cockatrice/node1/index --http-port=8080
    $ cockatrice server --bind-addr=127.0.0.1:7071 --peer-addr=127.0.0.1:7070 --peer-addr=127.0.0.1:7072 --dump-file=/tmp/cockatrice/node2/raft/data.dump --index-dir=/tmp/cockatrice/node2/index --http-port=8081
    $ cockatrice server --bind-addr=127.0.0.1:7072 --peer-addr=127.0.0.1:7070 --peer-addr=127.0.0.1:7071 --dump-file=/tmp/cockatrice/node3/raft/data.dump --index-dir=/tmp/cockatrice/node3/index --http-port=8082

Above example shows each Cockatrice node running on the same host, so each node must listen on different ports. This would not be necessary if each node ran on a different host.

So you have a 3-node cluster. That way you can tolerate the failure of 1 node.

You can check the cluster with the following command:

.. code-block:: bash

    $ cockatrice status --bind-addr=127.0.0.1:7070 | jq .

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "message": "SUCCESS",
      "data": {
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
        "log_len": 56,
        "last_applied": 59,
        "commit_idx": 59,
        "raft_term": 110,
        "next_node_idx_count": 2,
        "next_node_idx_server_127.0.0.1:7071": 60,
        "next_node_idx_server_127.0.0.1:7072": 60,
        "match_idx_count": 2,
        "match_idx_server_127.0.0.1:7071": 59,
        "match_idx_server_127.0.0.1:7072": 59,
        "leader_commit_idx": 59,
        "uptime": 10860,
        "self_code_version": 0,
        "enabled_code_version": 0
      }
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


Create a cluster with dynamic membership by manual operation
------------------------------------------------------------

Dynamic membership change allows you to add or remove nodes from your cluster without cluster restart.
This section describes how to scale the cluster. Let's start first node by the following command:

.. code-block:: bash

    $ cockatrice server --bind-addr=127.0.0.1:7070 --dump-file=/tmp/cockatrice/node1/raft/data.dump --index-dir=/tmp/cockatrice/node1/index --http-port=8080

Then, execute join command with new node on one of the existing nodes.

.. code-block:: bash

    $ cockatrice join --bind-addr=127.0.0.1:7070 --join-addr=127.0.0.1:7071

``127.0.0.1:7070`` is one of the existing cluster nodes, and ``127.0.0.1:7071`` is the node you want to add.
The above command will wait until the new node starts up. You need to launch new node with correct initial peers on the other terminal window by following:

.. code-block:: bash

    $ cockatrice server --bind-addr=127.0.0.1:7071 --dump-file=/tmp/cockatrice/node2/raft/data.dump --index-dir=/tmp/cockatrice/node2/index --peer-addr=127.0.0.1:7070 --http-port=8081

Also, recommend 3 or more odd number of nodes in the cluster due to avoid split brain. You should launch one more new node with correct initial peers like following:

.. code-block:: bash

    $ cockatrice join --bind-addr=127.0.0.1:7070 --join-addr=127.0.0.1:7072
    $ cockatrice server --bind-addr=127.0.0.1:7072 --dump-file=/tmp/cockatrice/node3/raft/data.dump --index-dir=/tmp/cockatrice/node3/index --peer-addr=127.0.0.1:7070 --peer-addr=127.0.0.1:7071 --http-port=8082


Create a cluster with dynamic membership without manual operation
-----------------------------------------------------------------

The above section described how to create a cluster with dynamic membership by manual operation. Although it is a method that is used when the administrator needs accurate operation, it provides easier way to create a cluster with dynamic membership without manual operations.
Start first node in standalone mode by following command:

.. code-block:: bash

    $ cockatrice server --bind-addr=127.0.0.1:7070 --dump-file=/tmp/cockatrice/node1/raft/data.dump --index-dir=/tmp/cockatrice/node1/index --http-port=8080
    $ cockatrice server --bind-addr=127.0.0.1:7071 --seed-addr=127.0.0.1:7070 --dump-file=/tmp/cockatrice/node2/raft/data.dump --index-dir=/tmp/cockatrice/node2/index --http-port=8081
    $ cockatrice server --bind-addr=127.0.0.1:7072 --seed-addr=127.0.0.1:7070 --dump-file=/tmp/cockatrice/node3/raft/data.dump --index-dir=/tmp/cockatrice/node3/index --http-port=8082

Just add ``--seed-addr`` parameter and start it. These are the same as that create a cluster with dynamic membership by manual operation. The above command performs register a new node and starts one at the same time.
