# Transform

The `validate` exmaple adds the concept of scripts to the trickle file. In this script we validate the schema of the input json against some requirements and only let events through that do satisfy them. Other evetns are dropped. Those changes are entirely inside the [`example.trickle`](etc/tremor/config/example.trickle).

## Environment

In the [`example.trickle`](etc/tremor/config/example.trickle) we define a script that validates the schema that the field `hello` is a string and the field `selected` is a boolean. If both conditions are true we pass it on, otherwise it'll drop.

All other configuration is the same as per the previous example, and is elided here for brevity.

## Business Logic

```tremor
define script validate                                                          # define the script
script
  match event of
    case %{ present hello, present selected }                                   # Record pattern, validating field presence
        when type::is_string(event.hello) and type::is_bool(event.selected)     # guards
            => emit event                                                       # propagate if valid
    default => drop                                                             # discard or reroute 
  end
end;
```

## Command line testing during logic development

Execute a the query against a sample input.json

```bash
$ tremor-query -e input.json example.trickle
>> {"hello": "world"}
```

Deploy the solution into a docker environment

```bash
$ docker-compose up
>> {"hello": "again", "selected": true}
```

Inject test messages via [websocat](https://github.com/vi/websocat)

> Note: Can be installed via `cargo install websocat` for the lazy/impatient amongst us

```bash
$ cat inputs.txt | websocat ws://localhost:4242
...
```

Injecting invalid messages to verify validation.

```bash
$ cat invalids.txt | websocat ws://localhost:4242
...
```

### Discussion

We introduce the `declare script` and `create script` query language features. `delcare script` lets declare a template for a script to be executed while `create script` instanciates it as a part of the graph. `create script` takes an additional `as <name>` argument if it is omitted the operator will have the same name as the declaration.

### Attention

Scripts themselfs can not connect to elements inside the graph, a `select` statement is needed to glue scripts and other logic together.