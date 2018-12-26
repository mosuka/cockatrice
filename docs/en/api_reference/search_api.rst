Search APIs
===========

Search API
----------

.. code-block:: text

    GET /indices/<INDEX_NAME>/search?query=<QUERY>&search_field=<SEARCH_FIELD>&page_num=<PAGE_NUM>&page_len=<PAGE_LEN>&output=<OUTPUT>

* ``<INDEX_NAME>``: The index name to search.
* ``<QUERY>``: The unicode string to search index.
* ``<SEARCH_FIELD>``: Uses this as the field for any terms without an explicit field.
* ``<PAGE_NUM>``: The page number to retrieve, starting at ``1`` for the first page.
* ``<PAGE_LEN>``: The number of results per page.
* ``<OUTPUT>``: The output format. ``json`` or ``yaml``. Default is ``json``.
