Schema
======

Cockatrice fully supports the field types, analyzers, tokenizers and filters provided by Whoosh.

This section discusses how Cockatrice organizes its data into documents and fields, as well as how to work with a schema in Cockatrice.


Schema Design
-------------

Cockatrice defines the schema in `YAML <https://yaml.org>`_ or `JSON <https://json.org>`_ format.

The following items are defined in configuration:

* schema
* default_search_field
* field_types
* analyzers
* tokenizers
* filters


Schema
------

The schema is the place where you tell Cockatrice how it should build indexes from input documents.

.. code-block:: text

    schema:
      <FIELD_NAME>:
        field_type: <FIELD_TYPE>
        args:
          <ARG_NAME>: <ARG_VALUE>
          ...

.. code-block:: text

    {
      "schema": {
        <FIELD_NAME>: {
          "field_type": <FIELD_TYPE>,
          "args": {
            <ARG_NAME>: <ARG_VALUE>,
            ...
          }
        }
      }
    }

* ``<FIELD_NAME>``: The field name in the document.
* ``<FIELD_TYPE>``: The field type used in this field.
* ``<ARG_NAME>``: The argument name to use constructing the field.
* ``<ARG_VALUE>``: The argument value to use constructing the field.

For example, ``id`` field used as a unique key is defined as following:

.. code-block:: yaml

    schema:
      id:
        field_type: id
        args:
          unique: true
          stored: true

.. code-block:: json

    {
      "schema": {
        "id": {
          "field_type": "id",
          "args": {
            "unique": true,
            "stored": true
          }
        }
      }
    }


Default Search Field
--------------------

The query parser uses this as the field for any terms without an explicit field.

.. code-block:: text

    default_search_field: <FIELD_NAME>

.. code-block:: text

    {
      "default_search_field": <FIELD_NAME>
    }

* ``<FIELD_NAME>``: Uses this as the field name for any terms without an explicit field name.

For example, uses ``text`` field as default search field as following:

.. code-block:: yaml

    default_search_field: text

.. code-block:: json

    {
      "default_search_field": "text"
    }


Field Types
-----------

The field type defines how Cockatrice should interpret data in a field and how the field can be queried. There are many field types included with Whoosh by default, and they can also be defined directly in YAML or JSON.

.. code-block:: text

    field_types:
      <FIELD_TYPE>:
        class: <FIELD_TYPE_CLASS>
        args:
          <ARG_NAME>: <ARG_VALUE>
          ...

.. code-block:: text

    {
      "field_types": {
        <FIELD_TYPE>: {
          "class": <FIELD_TYPE_CLASS>,
          "args": {
            <ARG_NAME>: <ARG_VALUE>,
            ...
          }
        }
      }
    }

* ``<FIELD_TYPE>``: The field type name.
* ``<FIELD_TYPE_CLASS>``: The field type class.
* ``<ARG_NAME>``: The argument name to use constructing the field type.
* ``<ARG_VALUE>``: The argument value to use constructing the field type.

For example, defines ``text`` field type as following:

.. code-block:: yaml

    field_types:
      text:
        class: whoosh.fields.TEXT
        args:
          analyzer:
          phrase: true
          chars: false
          stored: false
          field_boost: 1.0
          multitoken_query: default
          spelling: false
          sortable: false
          lang: null
          vector: null
          spelling_prefix: spell_

.. code-block:: json

    {
      "field_types": {
        "text": {
          "class": "whoosh.fields.TEXT",
          "args": {
            "analyzer": null,
            "phrase": true,
            "chars": false,
            "stored": false,
            "field_boost": 1.0,
            "multitoken_query": "default",
            "spelling": false,
            "sortable": false,
            "lang": null,
            "vector": null,
            "spelling_prefix": "spell_"
          }
        }
      }
    }


Analyzers
---------

An analyzer examines the text of fields and generates a token stream. The simplest way to configure an analyzer is with a single ``class`` element whose class attribute is a fully qualified Python class name.

Even the most complex analysis requirements can usually be decomposed into a series of discrete, relatively simple processing steps. Cockatrice comes with a large selection of tokenizers and filters. Setting up an analyzer chain is very straightforward; you specify a ``tokenizer`` and ``filters`` to use, in the order you want them to run.

.. code-block:: text

    analyzers:
      <ANALYZER_NAME>:
        class: <ANALYZER_CLASS>
        args:
          <ARG_NAME>: <ARG_VALUE>
          ...
      <ANALYZER_NAME>:
        tokenizer: <TOKENIZER_NAME>
        filters:
          - <FILTER_NAME>
          ...

.. code-block:: text

    {
      "analyzers": {
        <ANALYZER_NAME>: {
          "class": <ANALYZER_CLASS>,
          "args": {
            <ARG_NAME>: <ARG_VALUE>,
            ...
          }
        },
        <ANALYZER_NAME>: {
          "tokenizer": <TOKENIZER_NAME>,
          "filters": [
            <FILTER_NAME>,
            ...
          ]
        }
      }
    }

* ``<ANALYZER_NAME>``: The analyzer name.
* ``<ANALYZER_CLASS>``: The analyzer class.
* ``<ARG_NAME>``: The argument name to use constructing the analyzer.
* ``<ARG_VALUE>``: The argument value to use constructing the analyzer.
* ``<TOKENIZER_NAME>``: The tokenizer name to use in the analyzer chain.
* ``<FILTER_NAME>``: The filter name to use in the analyzer chain.

For example, defines analyzers using ``class``, ``tokenizer`` and ``filters`` as follows:

.. code-block:: yaml

    analyzers:
      simple:
        class: whoosh.analysis.SimpleAnalyzer
        args:
          expression: "\\w+(\\.?\\w+)*"
          gaps: false
      ngramword:
        tokenizer: regex
        filters:
          - lowercase
          - ngram

.. code-block:: yaml

    {
      "analyzers": {
        "simple": {
          "class": "whoosh.analysis.SimpleAnalyzer",
          "args": {
            "expression": "\\w+(\\.?\\w+)*",
            "gaps": false
          }
        },
        "ngramword": {
          "tokenizer": "regex",
          "filters": [
            "lowercase",
            "ngram"
          ]
        }
      }
    }


Tokenizers
----------

The job of a tokenizer is to break up a stream of text into tokens, where each token is (usually) a sub-sequence of the characters in the text.

.. code-block:: text

    tokenizers:
      <TOKENIZER_NAME>:
        class: <TOKENIZER_CLASS>
        args:
          <ARG_NAME>: <ARG_VALUE>
          ...

.. code-block:: text

    {
      "tokenizers": {
        <TOKENIZER_NAME>: {
          "class": <TOKENIZER_CLASS>,
          "args": {
            <ARG_NAME>: ARG_VALUE>,
            ...
          }
        }
      }
    }

* ``<TOKENIZER_NAME>``: The tokenizer name.
* ``<TOKENIZER_CLASS>``: The tokenizer class.
* ``<ARG_NAME>``: The argument name to use constructing the tokenizer.
* ``<ARG_VALUE>``: The argument value to use constructing the tokenizer.

For example, defines tokenizer as follows:

.. code-block:: yaml

    tokenizers:
      ngram:
        class: whoosh.analysis.NgramTokenizer
        args:
          minsize: 2
          maxsize: null

.. code-block:: json

    {
      "tokenizers": {
        "ngram": {
          "class": "whoosh.analysis.NgramTokenizer",
          "args": {
            "minsize": 2,
            "maxsize": null
          }
        }
      }
    }


Filters
-------

The job of a filter is usually easier than that of a tokenizer since in most cases a filter looks at each token in the stream sequentially and decides whether to pass it along, replace it or discard it.

.. code-block:: text

    filters:
      <FILTER_NAME>:
        class: <FILTER_CLASS>
        args:
          <ARG_NAME>: <ARG_VALUE>
          ...

.. code-block:: text

    {
      "filters": {
        <FILTER_NAME>: {
          "class": <FILTER_CLASS>,
          "args": {
            <ARG_NAME>: <ARG_VALUE>,
            ...
          }
        }
      }
    }

* ``<FILTER_NAME>``: The filter name.
* ``<FILTER_CLASS>``: The filter class.
* ``<ARG_NAME>``: The argument name to use constructing the filter.
* ``<ARG_VALUE>``: The argument value to use constructing the filter.

For example, defines filter as follows:

.. code-block:: yaml

    filters:
      stem:
        class: whoosh.analysis.StemFilter
        args:
          lang: en
          ignore: null
          cachesize: 50000

.. code-block:: json

    {
      "filters": {
        "stem": {
          "class": "whoosh.analysis.StemFilter",
          "args": {
            "lang": "en",
            "ignore": null,
            "cachesize": 50000
          }
        }
      }
    }


Example
-------

Refer to the example for how to define schema.

YAML example: https://github.com/mosuka/cockatrice/blob/master/example/schema.yaml
JSON example: https://github.com/mosuka/cockatrice/blob/master/example/schema.json

More information
----------------

See documents for more information.

* https://whoosh.readthedocs.io/en/latest/schema.html
* https://whoosh.readthedocs.io/en/latest/api/fields.html
* https://whoosh.readthedocs.io/en/latest/api/analysis.html
