# Tremor tool

Tremor command line interface tool

## Scope

This document summarises tremor tool commands

## Audience

Tremor operators and developers


## Usage



### General flags and switches


|Name|Switch|Kind|Multiple|Description|
|---|---|---|---|---|
|config|c|takes value|yes|Sets a custom config file|
|verbose|v|switch/flag|yes|Sets the level of verbosity|
|format|f|takes value|yes|Sets the output format ( json | yaml )|


### Commands

Top level command summary

|Command|Description|
|---|---|
|script|Tremor scripting language tool|
|grok|Tremor support for logstash grok patterns|
|pipe|Tremor pipeline tool|
|api|Tremor API client|


## Command Details

Details for each supported command


### Command: **script**

Tremor scripting language tool

#### Subcommand: run






__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|SCRIPT|yes|tremor script filename|
|DATA|no|JSON-per-line data log to replay|



### Command: **grok**

Tremor support for logstash grok patterns

#### Subcommand: run






__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|patterns|no|Extra patterns ( or alternative builtins if ignoring builtin patterns )|
|TEST_PATTERN|yes|The grok pattern under test for this run|
|DATA|no|line by line data log to replay, or stdin otherwise|



### Command: **pipe**

Tremor pipeline tool

#### Subcommand: run






__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|CONFIG|yes|tremor pipeline configuration|
|DATA|no|JSON-per-line data log to replay|

#### Subcommand: dot






__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|CONFIG|yes|tremor pipeline configuration|



### Command: **api**

Tremor API client

#### Subcommand: version







#### Subcommand: target


|Name|Description|
|---|---|
|list|List registered targets|
|create|Create a new API target|
|delete|Delete an existing API target|


##### target list






##### target create





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|TARGET_ID|yes|The unique target id for the targetted tremor servers|

##### target delete





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|TARGET_ID|yes|The unique target id for the targetted tremor servers|

#### Subcommand: binding


|Name|Description|
|---|---|
|list|List registered binding specifications|
|fetch|Fetch a binding by artefact id|
|delete|Delete a binding by artefact id|
|create|Create and register a binding specification|
|instance|Fetch an binding instance by artefact id and instance id|
|activate|Activate a binding by artefact id and servant instance id|
|deactivate|Activate a binding by artefact id and servant instance id|


##### binding list






##### binding fetch





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the binding specification|

##### binding delete





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the binding specification|

##### binding create





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|SOURCE|yes|JSON or YAML file request body|

##### binding instance





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the binding specification|

##### binding activate





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the binding specification|

##### binding deactivate





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the binding specification|

#### Subcommand: pipeline


|Name|Description|
|---|---|
|list|List registered pipeline specifications|
|fetch|Fetch a pipeline by artefact id|
|delete|Delete a pipeline by artefact id|
|create|Create and register a pipeline specification|
|instance|Fetch an pipeline instance by artefact id and instance id|


##### pipeline list






##### pipeline fetch





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the pipeline specification|

##### pipeline delete





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the pipeline specification|

##### pipeline create





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|SOURCE|no|JSON or YAML file request body|

##### pipeline instance





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the pipeline specification|

#### Subcommand: onramp


|Name|Description|
|---|---|
|list|List registered onramp specifications|
|fetch|Fetch an onramp by artefact id|
|delete|Delete an onramp by artefact id|
|create|Create and register an onramp specification|
|instance|Fetch an onramp instance by artefact id and instance id|


##### onramp list






##### onramp fetch





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the onramp specification|

##### onramp delete





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the onramp specification|

##### onramp create





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|SOURCE|no|JSON or YAML file request body|

##### onramp instance





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the onramp specification|

#### Subcommand: offramp


|Name|Description|
|---|---|
|list|List registered offramp specifications|
|fetch|Fetch an offramp by artefact id|
|delete|Delete an offramp by artefact id|
|create|Create and register an offramp specification|
|instance|Fetch an offramp instance by artefact id and instance id|


##### offramp list






##### offramp fetch





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the offramp specification|

##### offramp delete





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the offramp specification|

##### offramp create





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|SOURCE|no|JSON or YAML file request body|

##### offramp instance





__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the offramp specification|

