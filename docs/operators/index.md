# Operators

Operators are part of the pipeline configuration.

Operators process events and signals in the context of a pipeline. An operator, upon receiving an event from an upstream operator or stream, MAY produce one or many events to one or many downstream directly connected operators. An operator MAY drop events which halts any further processing.

Operators allow the data processing capabilities of tremor to be extended or specialized without changes to runtime behavior, concurrency, event ordering or other aspects of a running tremor system.

Operators are created in the context of a pipeline and configured as part of `tremor-query` [statements](../../language/pipelines#custom-operator-definitions). An operator MUST have an identifier that is unique for its owning pipeline.

Configuration is of the general form:

```tremor
define operator my_custom_operator from module::operator_name
args
  required_argument,
  optional_argument = 42
with
  param1 = "foo",
  param2 = [1, args.required_argument, args.optional_argument]
end;

# create - short form
create operator my_custom_operator;

# create - full form
create operator my_custom_operator_instance from my_custom_operator
with
  param1 = "bar",
  param2 = [true, false, {}]
end;

```