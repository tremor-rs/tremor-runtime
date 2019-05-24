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



Run tremor script against stdin or a json data archive

USAGE:



```

    tremor-tool script run <SCRIPT> [DATA]

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;SCRIPT>    tremor script filename

  &lt;DATA>      JSON-per-line data log to replay


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|SCRIPT|yes|tremor script filename|
|DATA|no|JSON-per-line data log to replay|



### Command: **grok**

Tremor support for logstash grok patterns

#### Subcommand: run



Run tremor grok matcher against stdin or a json data archive

USAGE:



```

    tremor-tool grok run [OPTIONS] <TEST_PATTERN> [DATA]

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information

OPTIONS:

   __p__,__patterns__ <patterns>    Extra patterns ( or alternative builtins if ignoring builtin patterns )



ARGS:

  &lt;TEST_PATTERN>    The grok pattern under test for this run

  &lt;DATA>            line by line data log to replay, or stdin otherwise


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|patterns|no|Extra patterns ( or alternative builtins if ignoring builtin patterns )|
|TEST_PATTERN|yes|The grok pattern under test for this run|
|DATA|no|line by line data log to replay, or stdin otherwise|



### Command: **pipe**

Tremor pipeline tool

#### Subcommand: run



Run pipeline against stdin or a json data archive

USAGE:



```

    tremor-tool pipe run <CONFIG> [DATA]

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;CONFIG>    tremor pipeline configuration

  &lt;DATA>      JSON-per-line data log to replay


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|CONFIG|yes|tremor pipeline configuration|
|DATA|no|JSON-per-line data log to replay|

#### Subcommand: dot



Generate a dot ( graphviz ) graph from the pipeline

USAGE:



```

    tremor-tool pipe dot <CONFIG>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;CONFIG>    tremor pipeline configuration


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|CONFIG|yes|tremor pipeline configuration|



### Command: **api**

Tremor API client

#### Subcommand: version




Get tremor version

USAGE:



```

    tremor-tool api version

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information


#### Subcommand: target


|Name|Description|
|---|---|
|list|List registered targets|
|create|Create a new API target|
|delete|Delete an existing API target|


##### target list


List registered targets

USAGE:



```

    tremor-tool api target list

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



##### target create


Create a new API target

USAGE:



```

    tremor-tool api target create <TARGET_ID> <SOURCE>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;TARGET_ID>    The unique target id for the targetted tremor servers

  &lt;SOURCE>       JSON or YAML file request body


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|TARGET_ID|yes|The unique target id for the targetted tremor servers|

##### target delete


Delete an existing API target

USAGE:



```

    tremor-tool api target delete <TARGET_ID>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;TARGET_ID>    The unique target id for the targetted tremor servers


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


List registered binding specifications

USAGE:



```

    tremor-tool api binding list

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



##### binding fetch


Fetch a binding by artefact id

USAGE:



```

    tremor-tool api binding fetch <ARTEFACT_ID>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;ARTEFACT_ID>    The unique artefact id for the binding specification


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the binding specification|

##### binding delete


Delete a binding by artefact id

USAGE:



```

    tremor-tool api binding delete <ARTEFACT_ID>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;ARTEFACT_ID>    The unique artefact id for the binding specification


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the binding specification|

##### binding create


Create and register a binding specification

USAGE:



```

    tremor-tool api binding create <SOURCE>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;SOURCE>    JSON or YAML file request body


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|SOURCE|yes|JSON or YAML file request body|

##### binding instance


Fetch an binding instance by artefact id and instance id

USAGE:



```

    tremor-tool api binding instance <ARTEFACT_ID> <INSTANCE_ID>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;ARTEFACT_ID>    The unique artefact id for the binding specification

  &lt;INSTANCE_ID>    The unique instance id for the binding specification


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the binding specification|

##### binding activate


Activate a binding by artefact id and servant instance id

USAGE:



```

    tremor-tool api binding activate <ARTEFACT_ID> <INSTANCE_ID> <SOURCE>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;ARTEFACT_ID>    The unique artefact id for the binding specification

  &lt;INSTANCE_ID>    The unique instance id for the binding specification

  &lt;SOURCE>         JSON__r__ YAML file request body


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the binding specification|

##### binding deactivate


Activate a binding by artefact id and servant instance id

USAGE:



```

    tremor-tool api binding deactivate <ARTEFACT_ID> <INSTANCE_ID>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;ARTEFACT_ID>    The unique artefact id for the binding specification

  &lt;INSTANCE_ID>    The unique instance id for the binding specification


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


List registered pipeline specifications

USAGE:



```

    tremor-tool api pipeline list

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



##### pipeline fetch


Fetch a pipeline by artefact id

USAGE:



```

    tremor-tool api pipeline fetch <ARTEFACT_ID>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;ARTEFACT_ID>    The unique artefact id for the pipeline specification


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the pipeline specification|

##### pipeline delete


Delete a pipeline by artefact id

USAGE:



```

    tremor-tool api pipeline delete <ARTEFACT_ID>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;ARTEFACT_ID>    The unique artefact id for the pipeline specification


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the pipeline specification|

##### pipeline create


Create and register a pipeline specification

USAGE:



```

    tremor-tool api pipeline create [SOURCE]

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;SOURCE>    JSON or YAML file request body


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|SOURCE|no|JSON or YAML file request body|

##### pipeline instance


Fetch an pipeline instance by artefact id and instance id

USAGE:



```

    tremor-tool api pipeline instance <ARTEFACT_ID> <INSTANCE_ID>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;ARTEFACT_ID>    The unique artefact id for the pipeline specification

  &lt;INSTANCE_ID>    The unique instance id for the pipeline specification


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


List registered onramp specifications

USAGE:



```

    tremor-tool api onramp list

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



##### onramp fetch


Fetch an onramp by artefact id

USAGE:



```

    tremor-tool api onramp fetch <ARTEFACT_ID>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;ARTEFACT_ID>    The unique artefact id for the onramp specification


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the onramp specification|

##### onramp delete


Delete an onramp by artefact id

USAGE:



```

    tremor-tool api onramp delete <ARTEFACT_ID>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;ARTEFACT_ID>    The unique artefact id for the onramp specification


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the onramp specification|

##### onramp create


Create and register an onramp specification

USAGE:



```

    tremor-tool api onramp create [SOURCE]

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;SOURCE>    JSON or YAML file request body


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|SOURCE|no|JSON or YAML file request body|

##### onramp instance


Fetch an onramp instance by artefact id and instance id

USAGE:



```

    tremor-tool api onramp instance <ARTEFACT_ID> <INSTANCE_ID>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;ARTEFACT_ID>    The unique artefact id for the onramp specification

  &lt;INSTANCE_ID>    The unique instance id for the onramp specification


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


List registered offramp specifications

USAGE:



```

    tremor-tool api offramp list

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



##### offramp fetch


Fetch an offramp by artefact id

USAGE:



```

    tremor-tool api offramp fetch <ARTEFACT_ID>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;ARTEFACT_ID>    The unique artefact id for the offramp specification


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the offramp specification|

##### offramp delete


Delete an offramp by artefact id

USAGE:



```

    tremor-tool api offramp delete <ARTEFACT_ID>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;ARTEFACT_ID>    The unique artefact id for the offramp specification


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the offramp specification|

##### offramp create


Create and register an offramp specification

USAGE:



```

    tremor-tool api offramp create [SOURCE]

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;SOURCE>    JSON or YAML file request body


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|SOURCE|no|JSON or YAML file request body|

##### offramp instance


Fetch an offramp instance by artefact id and instance id

USAGE:



```

    tremor-tool api offramp instance <ARTEFACT_ID> <INSTANCE_ID>

```



FLAGS:

   __h__,__help__       Prints help information

   __V__,__version__    Prints version information



ARGS:

  &lt;ARTEFACT_ID>    The unique artefact id for the offramp specification

  &lt;INSTANCE_ID>    The unique instance id for the offramp specification


__Arguments:__


|Argument|Required?|Description|
|---|---|---|
|ARTEFACT_ID|yes|The unique artefact id for the offramp specification|

