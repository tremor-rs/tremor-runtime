
## Document status

Work In Progress

## Well known API endpoints

This document summarises the tremor REST API

|Url|Description|
|---|---|
|http://localhost:9898/	|	The default ( development ) endpoint on a local ( development ) host|


## Paths 

The endpoint paths support by the Tremor REST API

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

###  __GET__ /{artefact-kind}

Lists artefacts of a given type

*Description:*

Given a valid artefact kind parameter of:

* pipeline
* onramp
* offramp
* binding

Returns a list of identifiers for each element stored in the repository

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  find_artefacts

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/registry_set|
> |200|application/yaml|#/components/schemas/registry_set|

###  __POST__ /{artefact-kind}

Publish a new artefact of a given type to the tremor artefact repository

*Description:*

Given a valid artefact kind parameter of:

* pipeline
* onramp
* offramp
* binding

Publishes a new artefact to the tremor artefact repository if the artefact id
is unique.

Returns artefact data, on success.

If an arterfact of the same name already exists, a conflict error is returned.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  publish_artefact

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |201|application/json|#/components/schemas/publish_ok|
> |201|application/yaml|#/components/schemas/publish_ok|
> |409|empty|no content|

###  __DELETE__ /{artefact-kind}/{artefact-id}

Remove artefact from tremor artefact repository

*Description:*

Given a valid artefact kind parameter of:

* pipeline
* onramp
* offramp
* binding

Given a valid artefact identifier of an artefact stored in the tremor artefact repository

Returns old artefact data, on success.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  delete_artefact_by_id

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/artefact|
> |200|application/yaml|#/components/schemas/artefact|
> |404|empty|no content|

###  __GET__ /{artefact-kind}/{artefact-id}

Get artefact data from tremor artefact repository

*Description:*

Given a valid artefact kind parameter of:

* pipeline
* onramp
* offramp
* binding

Given a valid artefact identifier of an artefact stored in the tremor artefact repository

Returns artefact data, on success.

Response data may be either JSON or YAML formatted ( defaults to JSON ).


*OperationId:*

>  get_artefact_by_id

*Returns:*

> |Status Code|Content Type|Schema Type|
> |---|---|---|
> |200|application/json|#/components/schemas/artefact|
> |200|application/yaml|#/components/schemas/artefact|
> |404|empty|no content|

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
### Schema for type: __binding__

"A tremor binding specification"

```json
{
  "schema": {
    "description": "A tremor binding specification",
    "properties": {
      "description": {
        "description": "Documentation for this type",
        "type": "string"
      },
      "id": {
        "description": "A unique identifier for this binding specification",
        "type": "string"
      },
      "links": {
        "$ref": "#/components/schemas/binding_map"
      }
    },
    "required": [
      "id"
    ],
    "type": "object"
  }
}
```
### Schema for type: __binding_map__

"A map of binding specification links"

```json
{
  "schema": {
    "description": "A map of binding specification links",
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
      "raw",
      "influx"
    ],
    "type": "string"
  }
}
```
### Schema for type: __edges__

"The set of connections between nodes/vertices/operators in a pipeline DAG"

```json
{
  "schema": {
    "description": "The set of connections between nodes/vertices/operators in a pipeline DAG",
    "type": "object"
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
### Schema for type: __interface__

null

```json
{
  "schema": {
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
### Schema for type: __offramp__

"A tremor offramp specification"

```json
{
  "schema": {
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
        "description": "A unique identifier for this offramp specification",
        "type": "string"
      },
      "type": {
        "description": "Rust native type for this offramp specification",
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
### Schema for type: __onramp__

"A tremor onramp specification"

```json
{
  "schema": {
    "description": "A tremor onramp specification",
    "properties": {
      "codec": {
        "$ref": "#/components/schemas/codec",
        "description": "The codec supported by this offramp"
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
        "description": "A unique identifier for this onramp specification",
        "type": "string"
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
### Schema for type: __operator__

"An operator node in a pipeline or vertex in the pipeline DAG"

```json
{
  "schema": {
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
        "description": "A pipeline unique identifier for this operator",
        "type": "string"
      },
      "type": {
        "description": "Rust native type for this operator specification",
        "type": "string"
      }
    },
    "type": "object"
  }
}
```
### Schema for type: __pipeline__

"A tremor pipeline specification"

```json
{
  "schema": {
    "description": "A tremor pipeline specification",
    "properties": {
      "description": {
        "description": "Documentation for this type",
        "type": "string"
      },
      "id": {
        "description": "A unique identifier for this pipeline specification",
        "type": "string"
      },
      "interface": {
        "$ref": "#/components/schemas/interface"
      },
      "links": {
        "$ref": "#/components/schemas/edges"
      },
      "nodes": {
        "$ref": "#/components/schemas/vertices"
      }
    },
    "required": [
      "id"
    ],
    "type": "object"
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
        "description": "The id of the pubished artefact",
        "type": "string"
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
      "$ref": "#/components/schemas/artefact"
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
    "description": "The set of input or output stream names",
    "items": {
      "type": "string"
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
### Schema for type: __vertices__

"The set of operator nodes this pipeline DAG is formed from"

```json
{
  "schema": {
    "description": "The set of operator nodes this pipeline DAG is formed from",
    "items": {
      "$ref": "#/components/schemas/operator"
    },
    "type": "array"
  }
}
```
