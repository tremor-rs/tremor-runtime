# Filter

The `filter` builds on the [passthrough example](../00_passthrough/README.md) and extends the [`example.trickle`](etc/tremor/config/example.trickle) by adding a filter
on the field `selected`. Only if this field is set to true the event will pass.

## Environment

It connects to the pipeline `example` in the [`example.trickle`](etc/tremor/config/example.trickle) file using the trickle query language to express the filtering logic.

All other configuration is the same as per the passthrough example, and is elided here for brevity.

## Business Logic

```trickle
select event from in into out where event.selected
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
>> {"hello": "again", "selected": true}
```

Inject test messages via [websocat](https://github.com/vi/websocat)

> Note: Can be installed via `cargo install websocat` for the lazy/impatient amongst us

```bash
$ cat inputs.txt | websocat ws://localhost:4242
...
```

### Discussion

Filters in tremor query ( `trickle` ) can be any legal predicate expression ( boolean returning
expression or function call ). For example:

#### Where clause

Events are selected on the inbound event if the `numeric` field on the inbound event is less than or equal to `10` or
greater than or equal to `100`.

```trickle
select event
from in
where event.numeric <= 10 or >= 100
into out
```

#### Having clause

Events are selected after processing them if the `selected` field on the outbound event is true.

```trickle
select event
from in
into out
having event.selected
```

#### Where and Having clauses

Events are selected on the inbound event if the `numeric` field on the inbound event is less than or equal to `10` or
greater than or equal to `100` and after procerssing them the `selected` field on the outbound event is true.

```trickle
select event
from in
where event.numeric_filed <= 10 or >= 100
into out
having event.selected
```

### Attention

The `where` clause has to be located after the `from` section in a trickle select expression! The `where` clause is evaluated on the incoming event.

The `having` clause can be used to filter events, but it has to appear after the `into` expression and will be evaluated on the resulting produced event prior to passing it on.

The `where` and `having` clauses are optional in trickle select query statements.
