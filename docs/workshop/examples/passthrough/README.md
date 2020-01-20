# Passthrough

The `passthrough` example is the simplest possible configuration of thremor. It shows the very basic building blocks: Onramp, Offramp, Binding, Mapping and Pipeline.

## Environment

The [onramp](etc/tremor/config/00_ramps.yaml) we use is a websocket onramp listening on port 4242, it receives `JSON` formatted messages.

```yaml
onramp:
  - id: ws-input          # A unique id for binding/mapping
    type: ws              # The unique type descriptor for the onramp ( websocket server here)
    codec: json           # The underlying data format expected for application payload data
    config:
      port: 4242          # The TCP port to listen on
      host: '0.0.0.0'     # The IP address to bind on ( all interfaces in this case )
```

It connects to the pipeline `example` in the [`example.trickle`](etc/tremor/config/example.trickle) file using the trickle query language to express its logic.

The [offramp](etc/tremor/config/00_ramps.yaml) we used is a console offramp producing to standard output. It receives `JSON` formatted messages.

```yaml
offramp:
  - id: stdout-output     # The unique id for binding/mapping
    type: stdout          # The unique type descriptor for the offramp ( stdout here )
    codec: json           # The underlying data format expected for application payload data
    config:
      prefix: '>> '       # A prefix for data emitted on standard output by this offramp
```

The [binding](./etc/tremor/config/01_binding.yaml) expresses those relations and gives the graph of onramp, pipeline and offramp.

```yaml
binding:
  - id: passthrough                                 # The unique name of this binding template
    links:
      '/onramp/ws-input/{instance}/out':            # Connect the inpunt to the pipeline
       - '/pipeline/example/{instance}/in'
      '/pipeline/example/{instance}/out':           # Connect the pipeline to the output
       - '/offramp/stdout-output/{instance}/in'
```

Finally the [mapping](./etc/tremor/config/02_mapping.yaml) instanciates the binding with the given name and instance variable to activate the elements of the binding.

## Business Logic

```trickle
select event from in into out
```

## Command line testing during logic development

Execute a the passthrough query against a sample input.json

```bash
$ tremor-query -e input.json example.trickle
>> {"hello": "world"}
```

Deploy the solution into a docker environment

```bash
$ docker-compose up
>> {"hello": "world"}
...
>> {"snot": "badger"}
```

Inject test messages via [websocat](https://github.com/vi/websocat)

> Note: Can be installed via `cargo install websocat` for the lazy/impatient amongst us

