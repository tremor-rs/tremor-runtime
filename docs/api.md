
## Document status

Work In Progress

## Well known API endpoints

This document summarises the tremor REST API

|Url|Description|
|---|---|
|http://localhost:9898/	|	The default ( development ) endpoint on a local ( development ) host|


## Paths 

The endpoint paths support by the Tremor REST API

###  __GET__ /binding

Lists bindings

*Description:*

Returns a list of identifiers for each binding stored in the repository

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  find_bindings

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/registry_set|
> |200|application/yaml|#/components/schemas/registry_set|

###  __POST__ /binding

Publish a new binding to the tremor artefact repository

*Description:*

Publishes a new binding to the tremor artefact repository if the artefact id
is unique.

Returns artefact data, on success.

If an arterfact of the same name already exists, a conflict error is returned.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  publish_binding

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |201|application/json|#/components/schemas/binding|
> |201|application/yaml|#/components/schemas/binding|
> |409|empty|no content|

###  __DELETE__ /binding/{artefact-id}

Remove binding from tremor artefact repository

*Description:*

Given a valid artefact identifier of an artefact stored in the tremor artefact repository

Returns old artefact data, on success.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  delete_binding_by_id

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/binding|
> |200|application/yaml|#/components/schemas/binding|
> |404|empty|no content|

###  __GET__ /binding/{artefact-id}

Get binding data from tremor artefact repository

*Description:*

Given a valid artefact identifier of an artefact stored in the tremor artefact repository

Returns artefact data, on success.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  get_binding_by_id

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/binding_state|
> |200|application/yaml|#/components/schemas/binding_state|
> |404|empty|no content|

###  __GET__ /offramp

Lists oframps

*Description:*

Returns a list of identifiers for each offramp stored in the repository

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  find_offramps

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/registry_set|
> |200|application/yaml|#/components/schemas/registry_set|

###  __POST__ /offramp

Publish a new offramp to the tremor artefact repository

*Description:*

Publishes a new offramp to the tremor artefact repository if the artefact id
is unique.

Returns artefact data, on success.

If an arterfact of the same name already exists, a conflict error is returned.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  publish_offramp

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |201|application/json|#/components/schemas/offramp|
> |201|application/yaml|#/components/schemas/offramp|
> |409|empty|no content|

###  __DELETE__ /offramp/{artefact-id}

Remove artefact from tremor artefact repository

*Description:*

Given a valid artefact identifier of an artefact stored in the tremor artefact repository

Returns old artefact data, on success.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  delete_offramp_by_id

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/offramp|
> |200|application/yaml|#/components/schemas/offramp|
> |404|empty|no content|
> |409|empty|no content|

###  __GET__ /offramp/{artefact-id}

Get offramp data from tremor artefact repository

*Description:*

Given a valid artefact identifier of an artefact stored in the tremor artefact repository

Returns artefact data, on success.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  get_offramp_by_id

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/offramp_state|
> |200|application/yaml|#/components/schemas/offramp_state|
> |404|empty|no content|

###  __GET__ /onramp

Lists onramps

*Description:*

Returns a list of identifiers for each onramp stored in the repository

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  find_onramps

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/registry_set|
> |200|application/yaml|#/components/schemas/registry_set|

###  __POST__ /onramp

Publish a new onramp to the tremor artefact repository

*Description:*

Publishes a new onramp to the tremor artefact repository if the artefact id
is unique.

Returns artefact data, on success.

If an onramp of the same name already exists, a conflict error is returned.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  publish_onramp

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |201|application/json|#/components/schemas/onramp|
> |201|application/yaml|#/components/schemas/onramp|
> |409|empty|no content|

###  __DELETE__ /onramp/{artefact-id}

Remove an onramp from tremor artefact repository

*Description:*


Given a valid artefact identifier of an artefact stored in the tremor artefact repository

Returns old artefact data, on success.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  delete_onramp_by_id

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/onramp|
> |200|application/yaml|#/components/schemas/onramp|
> |404|empty|no content|
> |409|empty|no content|

###  __GET__ /onramp/{artefact-id}

Finds onramp data from tremor artefact repository

*Description:*

Given a valid artefact identifier of an artefact stored in the tremor artefact repository

Returns artefact data, on success.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  get_onramp_by_id

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/onramp_state|
> |200|application/yaml|#/components/schemas/onramp_state|
> |404|empty|no content|

###  __GET__ /pipeline

Lists pipelines

*Description:*

Returns a list of identifiers for each pipeline stored in the repository

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  find_pipelines

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/registry_set|
> |200|application/yaml|#/components/schemas/registry_set|

###  __POST__ /pipeline

Publish a new pipeline to the tremor artefact repository

*Description:*

Publishes a new pipeline to the tremor artefact repository if the artefact id
is unique.

Returns artefact data, on success.

If an pipeline of the same name already exists, a conflict error is returned.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  publish_pipeline

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |201|application/json|#/components/schemas/pipeline|
> |201|application/yaml|#/components/schemas/pipeline|
> |409|empty|no content|

###  __DELETE__ /pipeline/{artefact-id}

Remove pipeline from tremor artefact repository

*Description:*

Given a valid artefact identifier of an artefact stored in the tremor artefact repository

Returns old artefact data, on success.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  delete_pipeline_by_id

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/pipeline|
> |200|application/yaml|#/components/schemas/pipeline|
> |404|empty|no content|
> |409|empty|no content|

###  __GET__ /pipeline/{artefact-id}

Get pipeline data from tremor artefact repository

*Description:*

Given a valid artefact identifier of an artefact stored in the tremor artefact repository

Returns artefact data, on success.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  get_pipeline_by_id

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/pipeline_state|
> |200|application/yaml|#/components/schemas/pipeline_state|
> |404|empty|no content|

###  __GET__ /version

Get's the current version

*Description:*


This endpoint returns version information for the current
version of tremor. Versioning policy follows [Semantic Versioning](https://semver.org/)


*OperationId:*

>  get_version

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/version|

###  __DELETE__ /{artefact-kind}/{artefact-id}/{instance-id}

Deactivate and unpublish deployed instances

*Description:*

Given a valid artefact kind parameter of:

* pipeline
* onramp
* offramp
* binding

Given a valid artefact identifier of an artefact stored in the tremor artefact repository

Given a valid instance identifier for a deployed and running instance of the artefact deployed
and accesible via the tremor instance registry

Deactivates, stops and unpublishes the target instances and any
dependant instances that are no longer referenced by the runtime.

Returns old instance data, on success.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  deactivate

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/artefact|
> |200|application/yaml|#/components/schemas/artefact|
> |404|empty|no content|

###  __GET__ /{artefact-kind}/{artefact-id}/{instance-id}

Get deployed artefact servant data from tremor artefact registry

*Description:*

Given a valid artefact kind parameter of:

* pipeline
* onramp
* offramp
* binding

Given a valid artefact identifier of an artefact stored in the tremor artefact repository

Given a valid instance identifier for a deployed and running instance of the artefact deployed
and accesible via the tremor instance registry

Returns instance data, on success.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  get_artefact_instance_by_id

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/artefact|
> |200|application/yaml|#/components/schemas/artefact|
> |404|empty|no content|

###  __POST__ /{artefact-kind}/{artefact-id}/{instance-id}

Publish, deploy and activate instances

*Description:*

Given a valid artefact kind parameter of:

* pipeline
* onramp
* offramp
* binding

Given a valid artefact identifier of an artefact stored in the tremor artefact repository

Given a valid instance identifier for a deployed and running instance of the artefact deployed
and accesible via the tremor instance registry

Creates new instances of artefacts ( if required ), publishes instances
to the tremor instance registry. If instances are onramps, offramps or
pipelines new registry values will be created. In the case of onramps
and offramps these are deployed *after* any dependant pipeline instances
and then they are interaconnected.

Returns instance data, on success.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  activate

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |201|application/json|#/components/schemas/artefact|
> |201|application/yaml|#/components/schemas/artefact|
> |404|empty|no content|

## Schemas 

JSON Schema for types defined in the Tremor REST API

### Schema for type: __artefact__

null

```json
{
  "schema": {
    "oneOf": [
      {
        "$ref": "#/components/schemas/pipeline"
      },
      {
        "$ref": "#/components/schemas/onramp"
      },
      {
        "$ref": "#/components/schemas/offramp"
      },
      {
        "$ref": "#/components/schemas/binding"
      }
    ]
  }
}
```
### Schema for type: __artefact_id__

null

```json
{
  "schema": {
    "pattern": "^[a-z][a-zA-Z_:]*$",
    "type": "string"
  }
}
```
### Schema for type: __binding__

"A tremor binding specification"

```json
{
  "schema": {
    "additionalProperties": false,
    "description": "A tremor binding specification",
    "properties": {
      "description": {
        "type": "string"
      },
      "id": {
        "$ref": "#/components/schemas/artefact_id"
      },
      "links": {
        "$ref": "#/components/schemas/binding_map"
      }
    },
    "required": [
      "id",
      "links"
    ],
    "type": "object"
  }
}
```
### Schema for type: __binding_dst__

null

```json
{
  "schema": {
    "pattern": "^(tremor://)?/(pipeline|offramp)/[a-zA-Z][A-Za-z0-9_]*/[a-zA-Z][A-Za-z0-9_]*$",
    "type": "string"
  }
}
```
### Schema for type: __binding_map__

"A map of binding specification links"

```json
{
  "schema": {
    "additionalProperties": false,
    "description": "A map of binding specification links",
    "patternProperties": {
      "^(tremor://)?/(onramp|pipeline)/[a-zA-Z][A-Za-z0-9_]*/[a-zA-Z][A-Za-z0-9_]*$": {
        "additionalItems": false,
        "items": {
          "$ref": "#/components/schemas/binding_dst"
        },
        "type": "array"
      }
    },
    "type": "object"
  }
}
```
### Schema for type: __binding_state__

"State of an binding, including specification and instances"

```json
{
  "schema": {
    "additionalProperties": false,
    "description": "State of an binding, including specification and instances",
    "properties": {
      "artefact": {
        "$ref": "#/components/schemas/binding"
      },
      "instances": {
        "$ref": "#/components/schemas/instance_set"
      }
    },
    "type": "object"
  }
}
```
### Schema for type: __codec__

"The data format supported for encoding/decoding to/from tremor types"

```json
{
  "schema": {
    "description": "The data format supported for encoding/decoding to/from tremor types",
    "enum": [
      "json",
      "msgpack",
      "string",
      "null",
      "influx"
    ],
    "type": "string"
  }
}
```
### Schema for type: __instance__

null

```json
{
  "schema": {
    "oneOf": [
      {
        "$ref": "#/components/schemas/mapping"
      }
    ]
  }
}
```
### Schema for type: __instance_id__

null

```json
{
  "schema": {
    "$ref": "#/components/schemas/artefact_id"
  }
}
```
### Schema for type: __instance_set__

"A list of artefact instances"

```json
{
  "schema": {
    "description": "A list of artefact instances",
    "items": {
      "$ref": "#/components/schemas/instance_id"
    },
    "type": "array"
  }
}
```
### Schema for type: __interface__

null

```json
{
  "schema": {
    "additionalProperties": false,
    "properties": {
      "inputs": {
        "$ref": "#/components/schemas/stream_names"
      },
      "outputs": {
        "$ref": "#/components/schemas/stream_names"
      }
    },
    "type": "object"
  }
}
```
### Schema for type: __links__

"The set of connections between nodes/vertices/operators in a pipeline DAG"

```json
{
  "schema": {
    "additionalProperties": false,
    "description": "The set of connections between nodes/vertices/operators in a pipeline DAG",
    "patternProperties": {
      "^[a-zA-Z][A-Za-z0-9_/]*$": {
        "additionalItems": false,
        "items": {
          "pattern": "^[a-zA-Z][A-Za-z0-9_/]*$",
          "type": "string"
        },
        "type": "array"
      }
    },
    "type": "object"
  }
}
```
### Schema for type: __mapping__

"A tremor mapping specification"

```json
{
  "schema": {
    "description": "A tremor mapping specification",
    "type": "object"
  }
}
```
### Schema for type: __nodes__

"The set of operator nodes this pipeline DAG is formed from"

```json
{
  "schema": {
    "additionalItems": false,
    "description": "The set of operator nodes this pipeline DAG is formed from",
    "items": {
      "$ref": "#/components/schemas/operator"
    },
    "type": "array"
  }
}
```
### Schema for type: __offramp__

"A tremor offramp specification"

```json
{
  "schema": {
    "additionalProperties": false,
    "description": "A tremor offramp specification",
    "properties": {
      "codec": {
        "$ref": "#/components/schemas/codec"
      },
      "config": {
        "description": "A map of key/value pairs used to configure this onramp",
        "type": "object"
      },
      "description": {
        "description": "Documentation for this type",
        "type": "string"
      },
      "id": {
        "$ref": "#/components/schemas/artefact_id"
      },
      "type": {
        "type": "string"
      }
    },
    "required": [
      "type",
      "id"
    ],
    "type": "object"
  }
}
```
### Schema for type: __offramp_state__

"State of an offramp, including specification and instances"

```json
{
  "schema": {
    "additionalProperties": false,
    "description": "State of an offramp, including specification and instances",
    "properties": {
      "artefact": {
        "$ref": "#/components/schemas/offramp"
      },
      "instances": {
        "$ref": "#/components/schemas/instance_set"
      }
    },
    "type": "object"
  }
}
```
### Schema for type: __onramp__

"A tremor onramp specification"

```json
{
  "schema": {
    "additionalProperties": false,
    "description": "A tremor onramp specification",
    "properties": {
      "codec": {
        "$ref": "#/components/schemas/codec"
      },
      "config": {
        "description": "A map of key/value pairs used to configure this onramp",
        "type": "object"
      },
      "description": {
        "description": "Documentation for this type",
        "type": "string"
      },
      "id": {
        "$ref": "#/components/schemas/artefact_id"
      },
      "preprocessors": {
        "additionalItems": false,
        "items": {
          "$ref": "#/components/schemas/preprocessor"
        },
        "type": "array"
      },
      "type": {
        "description": "Rust native type for this onramp specification",
        "type": "string"
      }
    },
    "required": [
      "type",
      "id"
    ],
    "type": "object"
  }
}
```
### Schema for type: __onramp_state__

"State of an onramp, including specification and instances"

```json
{
  "schema": {
    "additionalProperties": false,
    "description": "State of an onramp, including specification and instances",
    "properties": {
      "artefact": {
        "$ref": "#/components/schemas/onramp"
      },
      "instances": {
        "$ref": "#/components/schemas/instance_set"
      }
    },
    "type": "object"
  }
}
```
### Schema for type: __operator__

"An operator node in a pipeline or vertex in the pipeline DAG"

```json
{
  "schema": {
    "additionalProperties": false,
    "description": "An operator node in a pipeline or vertex in the pipeline DAG",
    "properties": {
      "config": {
        "description": "A map of key/value pairs used to configure this operator",
        "type": "object"
      },
      "description": {
        "description": "Documentation for this type",
        "type": "string"
      },
      "id": {
        "$ref": "#/components/schemas/artefact_id",
        "description": "A pipeline unique identifier for this operator"
      },
      "type": {
        "description": "Rust native type for this operator specification",
        "type": "string"
      }
    },
    "required": [
      "id",
      "type"
    ],
    "type": "object"
  }
}
```
### Schema for type: __pipeline__

"A tremor pipeline specification"

```json
{
  "schema": {
    "additionalProperties": false,
    "description": "A tremor pipeline specification",
    "properties": {
      "description": {
        "type": "string"
      },
      "id": {
        "$ref": "#/components/schemas/artefact_id"
      },
      "interface": {
        "$ref": "#/components/schemas/interface"
      },
      "links": {
        "$ref": "#/components/schemas/links"
      },
      "metrics_interval_s": {
        "minimum": 1,
        "type": "integer"
      },
      "nodes": {
        "$ref": "#/components/schemas/nodes"
      }
    },
    "required": [
      "id",
      "interface",
      "nodes",
      "links"
    ],
    "type": "object"
  }
}
```
### Schema for type: __pipeline_state__

"State of an pipeline, including specification and instances"

```json
{
  "schema": {
    "additionalProperties": false,
    "description": "State of an pipeline, including specification and instances",
    "properties": {
      "artefact": {
        "$ref": "#/components/schemas/pipeline"
      },
      "instances": {
        "$ref": "#/components/schemas/instance_set"
      }
    },
    "type": "object"
  }
}
```
### Schema for type: __port_id__

null

```json
{
  "schema": {
    "$ref": "#/components/schemas/artefact_id"
  }
}
```
### Schema for type: __preprocessor__

"Supported preprocessors"

```json
{
  "schema": {
    "description": "Supported preprocessors",
    "enum": [
      "lines",
      "base64",
      "decompress",
      "gelf-chunking"
    ],
    "type": "string"
  }
}
```
### Schema for type: __publish_ok__

"Response when a registry publish was succesful"

```json
{
  "schema": {
    "description": "Response when a registry publish was succesful",
    "properties": {
      "id": {
        "$ref": "#/components/schemas/artefact_id",
        "description": "The id of the pubished artefact"
      }
    },
    "required": [
      "id"
    ]
  }
}
```
### Schema for type: __registry_set__

"A list of registry artefacts"

```json
{
  "schema": {
    "description": "A list of registry artefacts",
    "items": {
      "$ref": "#/components/schemas/artefact_id"
    },
    "type": "array"
  }
}
```
### Schema for type: __stream_names__

"The set of input or output stream names"

```json
{
  "schema": {
    "additionalItems": false,
    "description": "The set of input or output stream names",
    "items": {
      "$ref": "#/components/schemas/port_id"
    },
    "type": "array"
  }
}
```
### Schema for type: __version__

"Version information"

```json
{
  "schema": {
    "description": "Version information",
    "properties": {
      "version": {
        "description": "The semantic version code",
        "type": "string"
      }
    },
    "required": [
      "version"
    ]
  }
}
```
