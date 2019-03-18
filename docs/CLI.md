# tremor-tool v1.0

Tremor command line interface tool

## Flags

|flag|short|description|
|---|---|---|
|config|-c|Sets a custom config file|
|format|-f|Sets the output format ( json or yaml )|
|verbose|-v|Sets the level of verbosity|

## General Commands

Generate a dot graph of the pipelines in a configuration (validates the configuration.) The output can be visualized using http://viz-js.com/

```bash
$ tremor-tool pipe dot path/to/tremor.yaml
```

## Exploratory Commands

Run tremor script against an archived data file ( json.xz )

```bash
$ tremor-tool script run path/to/tremor.script path/to/data.json.xz
```

Run pipeline against an archived data file ( json.xz )

```bash
$ tremor-tool pipe run path/to/tremor.yaml path/to/data.json.xz
```

## Repository API Commands

General form for listing, fetching and publishing onramps, offramps and pipelines

```bash
$ tremor-tool api <ARTEFACT_KIND> list # List all artefacts for type
$ tremor-tool api <ARTEFACT_KIND> fetch <ID> # Get artefact specification
$ tremor-tool api <ARTEFACT_KIND> create path/to/artefact.yaml # Publish an artefact
```

Where:

* ARTEFACT_KIND can be
  * one of:
    - pipeline - A pipeline specification
    - onramp - An onramp specification
    - offramp - An offramp specification
    - binding - A binding specification

## Registry API Commands

**TBD FIXME WIP**
