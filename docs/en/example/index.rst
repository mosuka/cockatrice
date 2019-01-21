Wipepedia example
=================

This section discusses how to index Wikipedia dump.


Downloading Wikipedia dump
--------------------------

.. code-block:: bash

    $ curl -o ~/tmp/enwiki-20190101-pages-articles.xml.bz2 https://dumps.wikimedia.org/enwiki/20190101/enwiki-20190101-pages-articles.xml.bz2


Installing Wikiextractor
------------------------

.. code-block:: bash

    $ git clone git@github.com:attardi/wikiextractor.git
    $ cd wikiextractor


Extracting Wikipedia data
-------------------------

.. code-block:: bash

    $ ./WikiExtractor.py -o ~/tmp/enwiki --json ~/tmp/enwiki-20190101-pages-articles.xml.bz2


Starting Cockatrice
-------------------

.. code-block:: bash

    $ cockatrice start indexer


Creating index
--------------

.. code-block:: bash

    $ cockatrice create index --schema-file ~/github.com/mosuka/cockatrice/example/enwiki_schema.yaml enwiki


Indexing Wikipedia
------------------

.. code-block:: bash

    $ cockatrice put documents --documents-file ~/github.com/mosuka/cockatrice/example/enwiki_schema.yaml enwiki

