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

    $ ./WikiExtractor.py --output ~/tmp/enwiki --bytes 200K --json ~/tmp/enwiki-20190101-pages-articles.xml.bz2


Starting Cockatrice
-------------------

.. code-block:: bash

    $ cockatrice start indexer


Creating index
--------------

.. code-block:: bash

    $ curl -s -X GET https://raw.githubusercontent.com/mosuka/cockatrice/master/example/enwiki_schema.yaml | xargs -0 cockatrice create index enwiki


Indexing Wikipedia
------------------

.. code-block:: bash

    $ for FILE in $(find ./tmp/enwiki -type f -name '*' | sort)
      do
        echo ${FILE}
        cat ${FILE} | jq  . | jq -s '.' | xargs -0 cockatrice put documents enwiki
      done
