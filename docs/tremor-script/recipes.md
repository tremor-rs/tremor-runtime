This document provides a few recipes for common patterns in tremor script. Please not however that it neither is exhaustive nor should those patterns considered the 'only way' to perform certain tasks.



## Extracting a raw message

If the event is a unstructured / raw message parsing it can be tricky since we can not match over a string. The following code offers a simple solution to it:

```tremor
# event = "John Doe"

let event =  match {"message": event} of
  case r = %{ message ~= dissect|%{first} %{last}| } => r.message
  default => drop
end;
# event = {"first" : "John", "last": "Doe"}
```



## Appending to an array

When appending to an array we can use the `array::push` function

```tremor
# event = {"key": "value", "tags": ["tag1"]}
let event.tags = array::push(event.tags, "tag2");
# event = {"key": "value", "tags": ["tag1", "tag2"]}
```



## Validating over extracted data

Sometimes we want to validate over extracted data without forcing the extraction to be a regular expression. For simple validations like the one below this pattern can be used.

```tremor
match event of
  # ...
  case r = %{message ~= dissect|%{log_level} %{log_timestamp}: %{logger}: %{message}|} when array::contains(["ERROR", "WARN", "INFO", "DEBUG"], result.message.log_level) => let event = merge event of r.message end
  # ...
end
```

Here we extract the `log_level`  and validate of that the it is one of `ERROR`, `WARN`, `INFO` or `DEBUG` by moving the check into the when guard we don't need to use a regular expression for this validation instead can use array membership.



## Replacing a field with an extraction

When extracting a field to merge with with the event and wanting to remove  the extracted field we can take advantage of the `merge` expressions behaviour that it will treat `null` in merged objects as a command to delete the data by setting the field to replace to `null` before merging.

```tremor
# event = %{"message": "John Doe"}
let event = merge event of match event of
  case r = %{message ~= dissect|%{first} %{last}| } => let r = r.message, let r.message = null, r
  default => {}
end;
# event = %{"first": "John", "last": "Doe"}
```



## No effect on non matching case

If we use merge with match we can make the `default` case to have no effect by using `{}`. This is possible since `merge` on `{}` is a identity function.

```tremor
# event = %{"message": "John Doe"}
let event = merge event of match event of
  case r = %{message ~= dissect|%{first} %{midle} %{last}| } => let r = r.message, let r.message = null, r
  default => {}
end;
# event = %{"message": "John Doe"}
```



## Boolean decisions

To make boolean decisions we can match on `true` or `false`. 

```tremor
match type::is_object(event) of
  case true => let event_type = "object"
  case false => let event_type = "other"
end
```



## Diverting an event to a different channel

By default the tremor-script operator forwards all events that are not dropped to the `out` channel for further processing. However it is possible to route events to different channels using the `emit` keyword. This allows, for example, diverting certain events to reserve bandwidth for a more important subset.

```tremor
match event.importance of
  case "high" => emit # this is the same as emit event => "out"
  case "medium" => emit event => "divert"
  default => drop # deletes the event
end
```



## The 'null default'

When the result of a match statement isn't needed - as in we use it purely for it's side effects - and we want the `default` to have no effect we can simply use `null` here.

```tremor
match event of
  case %{ tags ~= ["high-priority"]} => let event.importance = "high"
  default => null
end
```



## Testing against the type of a field

Sometimes we want to know if a field has a certain type. The `type` module provides help here but common types such as `record` or `array` can be checked using their patterns.

```tremor
match event of
  case %{field ~= %{}} => emit "event.field is a record"
  case %{field ~= %[]} => emit "event.field is a array"
  case %{} when type::is_record(event.field) => emit "event.field is a record"
  case %{} when type::is_array(event.field) => emit "event.field is a array"
  case %{} when type::is_number(event.field) => emit "event.field is a number (float or integer)"
  case %{} when type::is_integer(event.field) => emit "event.field is an integer"
  case %{} when type::is_float(event.field) => emit "event.field is a float"
  case %{} when type::is_null(event.field) => emit "event.field is null (but set)"
  case %{absent field} => emit "event.field is not set"
  # ...
end
```

## Routing messages

Tremor script can be used to route messages by combining the `emit` feature and the fact that the tremor runtime operator allows different outputs.

To route to doing a  `blue` / `green` split based on a field in a record we could use the following code:



```
match event of
  case %{key == "blue"} => emit event => "blue"
  case %{key == "green"} => emit event => "green"
  default => drop
end
```



And then in the pipeline configuration instead use parts such as:

```yaml

pipeline:
  - id: one
    outputs:
      - blue
      - green
    # ...
    links:
     script/blue: ["blue"]
     script/green: ["green"]     
```

