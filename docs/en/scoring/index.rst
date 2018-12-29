Scoring
=======

Cockatrice fully supports the weighting module (scoring/ranking model) provided by Whoosh.

This section discusses how to work with a weighting in Cockatrice.


Weighting Design
----------------

Cockatrice defines the weighting in `YAML <https://yaml.org>`_ or `JSON <https://json.org>`_ format.

The following items are defined in configuration:

* weighting


Weighting
---------

The weighting is the place where you tell Cockatrice how it should weighting documents in search from input queries.

.. code-block:: text

    weighting:
      default:
        class: <WEIGHTING_MODEL_CLASS>
        args:
          <ARG_NAME>: <ARG_VALUE>
          ...
      <FIELD_NAME>:
        class: <WEIGHTING_MODEL_CLASS>
        args:
          <ARG_NAME>: <ARG_VALUE>
          ...

.. code-block:: text

    {
      "weighting": {
        "default": {
          "class": <WEIGHTING_MODEL_CLASS>,
          "args": {
            <ARG_NAME>: <ARG_VALUE>,
            ...
          }
        },
        <FIELD_NAME>: {
          "class": <WEIGHTING_MODEL_CLASS>
          "args": {
            <ARG_NAME>: <ARG_VALUE>,
            ...
          }
        }
      }
    }

``default`` is the weighting instance to use for fields not specified in the field names.

* ``<FIELD_NAME>``: The field name.
* ``<WEIGHTING_MODEL_CLASS>``: The weighting model class.
* ``<ARG_NAME>``: The argument name to use constructing the weighting model.
* ``<ARG_VALUE>``: The argument value to use constructing the weighting model.

For example, defines weighting model as following:

.. code-block:: yaml

    weighting:
      default:
        class: whoosh.scoring.BM25F
        args:
          B: 0.75
          K1: 1.2
      title:
        class: whoosh.scoring.TF_IDF
      text:
        class: whoosh.scoring.PL2
        args:
          c: 1.0

.. code-block:: json

    {
      "weighting": {
        "default": {
          "class": "whoosh.scoring.BM25F",
          "args": {
            "B": 0.75,
            "K1": 1.2
          }
        },
        "title": {
          "class": "whoosh.scoring.TF_IDF"
        },
        "text": {
          "class": "whoosh.scoring.PL2",
          "args": {
            "c": 1.0
          }
        }
      }
    }


Example
-------

Refer to the example for how to define schema.

* YAML example: https://github.com/mosuka/cockatrice/blob/master/example/weighting.yaml
* JSON example: https://github.com/mosuka/cockatrice/blob/master/example/weighting.json


More information
----------------

See documents for more information.

* https://whoosh.readthedocs.io/en/latest/api/scoring.html
