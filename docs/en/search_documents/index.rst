Search documents
================

Once created an index and added documents to it, you can search for those documents.


Searching documents
-------------------

Searching documents by the following command:

.. code-block:: bash

    $ curl -s -X GET http://localhost:8080/indices/myindex/search?query=search | jq .

You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "results": {
        "hits": [
          {
            "doc": {
              "fields": {
                "contributor": "KolbertBot",
                "id": "3",
                "text": "Enterprise search is the practice of making content from multiple enterprise-type sources, such as databases and intranets, searchable to a defined audience. \"Enterprise search\" is used to describe the software of search information within an enterprise (though the search function and its results may still be public). Enterprise search can be contrasted with web search, which applies search technology to documents on the open web, and desktop search, which applies search technology to the content on a single computer. Enterprise search systems index data and documents from a variety of sources such as: file systems, intranets, document management systems, e-mail, and databases. Many enterprise search systems integrate structured and unstructured data in their collections.[3] Enterprise search systems also use access controls to enforce a security policy on their users. Enterprise search can be seen as a type of vertical search of an enterprise.",
                "timestamp": "20180129125400",
                "title": "Enterprise search"
              }
            },
            "pos": 0,
            "rank": 0,
            "score": 1.7928099079920008
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
            "pos": 1,
            "rank": 1,
            "score": 1.7730448689827392
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
            "pos": 2,
            "rank": 2,
            "score": 1.6882037387583286
          },
          {
            "doc": {
              "fields": {
                "contributor": "43.225.167.166",
                "id": "1",
                "text": "A search engine is an information retrieval system designed to help find information stored on a computer system. The search results are usually presented in a list and are commonly called hits. Search engines help to minimize the time required to find information and the amount of information which must be consulted, akin to other techniques for managing information overload. The most public, visible form of a search engine is a Web search engine which searches for information on the World Wide Web.",
                "timestamp": "20180704054100",
                "title": "Search engine (computing)"
              }
            },
            "pos": 3,
            "rank": 3,
            "score": 1.6626056232111253
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
            "pos": 4,
            "rank": 4,
            "score": 1.5883802067794877
          }
        ],
        "is_last_page": true,
        "page_count": 1,
        "page_len": 5,
        "page_num": 1,
        "total": 5
      },
      "status": {
        "code": 200,
        "description": "Request fulfilled, document follows",
        "phrase": "OK"
      },
      "time": 0.021579980850219727
    }


Searching documents with weighting model
----------------------------------------

You can specify the weighting model for scoring. Searching documents by the following command:

.. code-block:: bash

    $ curl -s -X POST -H "Content-type: text/x-yaml" --data-binary @./conf/weighting.yaml http://localhost:8080/indices/myindex/search?query=search | jq .


You can see the result in JSON format. The result of the above command is:

.. code-block:: json

    {
      "results": {
        "hits": [
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
            "pos": 0,
            "rank": 0,
            "score": 1.245740159716303
          },
          {
            "doc": {
              "fields": {
                "contributor": "43.225.167.166",
                "id": "1",
                "text": "A search engine is an information retrieval system designed to help find information stored on a computer system. The search results are usually presented in a list and are commonly called hits. Search engines help to minimize the time required to find information and the amount of information which must be consulted, akin to other techniques for managing information overload. The most public, visible form of a search engine is a Web search engine which searches for information on the World Wide Web.",
                "timestamp": "20180704054100",
                "title": "Search engine (computing)"
              }
            },
            "pos": 1,
            "rank": 1,
            "score": 0.8458503364028286
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
            "pos": 2,
            "rank": 2,
            "score": 0.7080130633767222
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
            "pos": 3,
            "rank": 3,
            "score": 0.34399698713879023
          },
          {
            "doc": {
              "fields": {
                "contributor": "KolbertBot",
                "id": "3",
                "text": "Enterprise search is the practice of making content from multiple enterprise-type sources, such as databases and intranets, searchable to a defined audience. \"Enterprise search\" is used to describe the software of search information within an enterprise (though the search function and its results may still be public). Enterprise search can be contrasted with web search, which applies search technology to documents on the open web, and desktop search, which applies search technology to the content on a single computer. Enterprise search systems index data and documents from a variety of sources such as: file systems, intranets, document management systems, e-mail, and databases. Many enterprise search systems integrate structured and unstructured data in their collections.[3] Enterprise search systems also use access controls to enforce a security policy on their users. Enterprise search can be seen as a type of vertical search of an enterprise.",
                "timestamp": "20180129125400",
                "title": "Enterprise search"
              }
            },
            "pos": 4,
            "rank": 4,
            "score": 0.2683300227447003
          }
        ],
        "is_last_page": true,
        "page_count": 1,
        "page_len": 5,
        "page_num": 1,
        "total": 5
      },
      "status": {
        "code": 200,
        "description": "Request fulfilled, document follows",
        "phrase": "OK"
      },
      "time": 0.043231964111328125
    }
