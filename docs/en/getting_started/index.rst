Getting Started
===============

Cockatrice makes it easy for programmers to develop search applications with advanced features. This section introduces you to the basic features to help you get up and running quickly.


Installing Cockatrice
---------------------

Installation of Cockatrice on Unix-compatible or Windows servers generally requires `Python <https://www.python.org>`_ interpreter and `pip <https://pip.pypa.io>`_ command.

Since Cockatrice is registered in `PyPi <https://pypi.org/project/cockatrice/>`_, you can install it only by executing the following command.

.. code-block:: bash

    $ pip install cockatrice


Starting Cockatrice
-------------------

Cockatrice includes a command line interface tool called ``cockatrice``. This tool allows you to start Cockatrice in your system.

You can easily start Cockatrice like the following command:

.. code-block:: bash

    $ cockatrice server

The above command starts Cockatrice in the default state. ``cockatrice`` has many startup flags, so please refer to the help for details.

You can display the help by specifying the following:

.. code-block:: bash

    $ cockatrice server --help

When Cockatrice started, following URL available:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/

You can see the result in plain text format. The result of the above command is:

.. code-block:: text

    cockatrice <VERSION> is running.


Create an index
---------------

You can not index documents yet just by starting Cockatrice. You need to create an index with a schema that tells how to index the documents.
Creating an index needs to put the schema in the request. The following command creates an index named ``myindex``:

.. code-block:: bash

    $ curl -s -X PUT -H 'Content-type: application/yaml' --data-binary @./example/schema.yaml http://localhost:8080/indices/myindex

The result of the above command can be seen in the JSON format as follows:

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

Information on the created index can be retrieve. The following command retrieves information on the index named ``myindex``:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/indices/myindex

The result of the above command can be seen in the JSON format as follows:

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

Index that are no longer needed can be deleted. The following command deletes the index named ``myindex``:

.. code-block:: bash

    $ curl -s -X DELETE http://localhost:8080/indices/myindex

The result of the above command can be seen in the JSON format as follows:

.. code-block:: json

    {
      "time": 0.0001461505889892578,
      "status": {
        "code": 202,
        "phrase": "Accepted",
        "description": "Request accepted, processing continues off-line"
      }
    }


Index a document
----------------

Indexing a document needs to put a document in the request that contains fields and its values. The following command indexes the document that id is ``1`` to the index named ``myindex``:

.. code-block:: bash

    $ curl -s -X PUT -H "Content-Type:application/json" http://localhost:8080/indices/myindex/documents/1 --data-binary @./example/doc1.json

The result of the above command can be seen in the JSON format as follows:

.. code-block:: json

    {
      "time": 0.0008089542388916016,
      "status": {
        "code": 202,
        "phrase": "Accepted",
        "description": "Request accepted, processing continues off-line"
      }
    }


Get a document
--------------

Information on the indexed document can be retrieve. The following command retrieves information on the document that id is ``1`` in the index named ``myindex``:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/indices/myindex/documents/1

The result of the above command can be seen in the JSON format as follows:

.. code-block:: json

    {
      "fields": {
        "contributor": "43.225.167.166",
        "id": "1",
        "text": "A search engine is an information retrieval system designed to help find information stored on a computer system. The search results are usually presented in a list and are commonly called hits. Search engines help to minimize the time required to find information and the amount of information which must be consulted, akin to other techniques for managing information overload.\nThe most public, visible form of a search engine is a Web search engine which searches for information on the World Wide Web.",
        "timestamp": "20180704054100",
        "title": "Search engine (computing)"
      },
      "time": 0.014967918395996094,
      "status": {
        "code": 200,
        "phrase": "OK",
        "description": "Request fulfilled, document follows"
      }
    }


Delete a document
-----------------

Document that are no longer needed can be deleted. The following command deletes the document that id is ``1`` in the index named ``myindex``:

.. code-block:: bash

    $ curl -s -X DELETE http://localhost:8080/indices/myindex/documents/1

The result of the above command can be seen in the JSON format as follows:

.. code-block:: json

    {
      "time": 0.00019788742065429688,
      "status": {
        "code": 202,
        "phrase": "Accepted",
        "description": "Request accepted, processing continues off-line"
      }
    }


Index documents in bulk
-----------------------

Include multiple documents in the request, you can index documents at once. The following command puts the documents in bulk into the index called ``myindex``.

.. code-block:: bash

    $ curl -s -X PUT -H "Content-Type:application/json" http://localhost:8080/indices/myindex/documents --data-binary @./example/bulk_index.json

The result of the above command can be seen in the JSON format as follows:

.. code-block:: json

    {
      "time": 0.05237007141113281,
      "status": {
        "code": 202,
        "phrase": "Accepted",
        "description": "Request accepted, processing continues off-line"
      }
    }


Delete documents in bulk
------------------------

Include multiple document IDs in the request, you can delete documents at once. The following command deletes the documents in bulk from an index named ``myindex``.

.. code-block:: bash

    $ curl -s -X DELETE -H "Content-Type:application/json" http://localhost:8080/indices/myindex/documents --data-binary @./example/bulk_delete.json

The result of the above command can be seen in the JSON format as follows:

.. code-block:: json

    {
      "status": {
        "code": 202,
        "description": "Request accepted, processing continues off-line",
        "phrase": "Accepted"
      },
      "time": 0.0012569427490234375
    }


Searching documents
-------------------

You can specify the search parameters to search the index under various conditions. The following command searches documents containing the keyword ``search`` from an index named ``myindex``.

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/indices/myindex/search?query=search

The result of the above command can be seen in the JSON format as follows:

.. code-block:: json

    {
      "results": {
        "is_last_page": true,
        "page_count": 1,
        "page_len": 5,
        "page_num": 1,
        "total": 5,
        "hits": [
          {
            "doc": {
              "fields": {
                "contributor": "KolbertBot",
                "id": "3",
                "text": "Enterprise search is the practice of making content from multiple enterprise-type sources, such as databases and intranets, searchable to a defined audience.\n\"Enterprise search\" is used to describe the software of search information within an enterprise (though the search function and its results may still be public). Enterprise search can be contrasted with web search, which applies search technology to documents on the open web, and desktop search, which applies search technology to the content on a single computer.\nEnterprise search systems index data and documents from a variety of sources such as: file systems, intranets, document management systems, e-mail, and databases. Many enterprise search systems integrate structured and unstructured data in their collections.[3] Enterprise search systems also use access controls to enforce a security policy on their users.\nEnterprise search can be seen as a type of vertical search of an enterprise.",
                "timestamp": "20180129125400",
                "title": "Enterprise search"
              }
            },
            "score": 1.8455226333928205,
            "rank": 0,
            "pos": 0
          },
          {
            "doc": {
              "fields": {
                "contributor": "Nurg",
                "id": "5",
                "text": "Federated search is an information retrieval technology that allows the simultaneous search of multiple searchable resources. A user makes a single query request which is distributed to the search engines, databases or other query engines participating in the federation. The federated search then aggregates the results that are received from the search engines for presentation to the user. Federated search can be used to integrate disparate information resources within a single large organization (\"enterprise\") or for the entire web. Federated search, unlike distributed search, requires centralized coordination of the searchable resources. This involves both coordination of the queries transmitted to the individual search engines and fusion of the search results returned by each of them.",
                "timestamp": "20180716000600",
                "title": "Federated search"
              }
            },
            "score": 1.8252014574100586,
            "rank": 1,
            "pos": 1
          },
          {
            "doc": {
              "fields": {
                "contributor": "Aistoff",
                "id": "2",
                "text": "A web search engine is a software system that is designed to search for information on the World Wide Web. The search results are generally presented in a line of results often referred to as search engine results pages (SERPs). The information may be a mix of web pages, images, and other types of files. Some search engines also mine data available in databases or open directories. Unlike web directories, which are maintained only by human editors, search engines also maintain real-time information by running an algorithm on a web crawler. Internet content that is not capable of being searched by a web search engine is generally described as the deep web.",
                "timestamp": "20181005132100",
                "title": "Web search engine"
              }
            },
            "score": 1.7381779253336536,
            "rank": 2,
            "pos": 2
          },
          {
            "doc": {
              "fields": {
                "contributor": "43.225.167.166",
                "id": "1",
                "text": "A search engine is an information retrieval system designed to help find information stored on a computer system. The search results are usually presented in a list and are commonly called hits. Search engines help to minimize the time required to find information and the amount of information which must be consulted, akin to other techniques for managing information overload.\nThe most public, visible form of a search engine is a Web search engine which searches for information on the World Wide Web.",
                "timestamp": "20180704054100",
                "title": "Search engine (computing)"
              }
            },
            "score": 1.7118135656658342,
            "rank": 3,
            "pos": 3
          },
          {
            "doc": {
              "fields": {
                "contributor": "Citation bot",
                "id": "4",
                "text": "A distributed search engine is a search engine where there is no central server. Unlike traditional centralized search engines, work such as crawling, data mining, indexing, and query processing is distributed among several peers in a decentralized manner where there is no single point of control.",
                "timestamp": "20180930171400",
                "title": "Distributed search engine"
              }
            },
            "score": 1.635459291513833,
            "rank": 4,
            "pos": 4
          }
        ]
      },
      "time": 0.015053987503051758,
      "status": {
        "code": 200,
        "phrase": "OK",
        "description": "Request fulfilled, document follows"
      }
    }
