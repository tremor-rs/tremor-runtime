# seperation

Tremor uses two seperators `,` and `;`. The pattern when which is used is as following:

## commands

Commands are separated with a `;`. This can be seen in top-level troy, trickle, and tremor script, in `patch`, `match`, and `for`.

## data

Data is seperated by `,` this can be seen in arrays and records as well as in the window declerators for select and the with and args part of troy and trickle defines and creates


# order of arguments in commands
A general pattern that repeats in troy/trickle/tremor-script is:

```
<action> <target> <source>
```

## query / troy syntax:


### Definitional statements
    
```tremor
define connector <my_alias> from <build_in_connector>;
define window <my_alias> from <window_type>;
define operator <my_alias> from <operator_type>;
define flow <my_alias>;
define script <my_alias>;
define pipeline <my_alias>;
```

**CHAGE**:

it was:

```
define <build_in_connector> connector <my_alias>;
define <window_type> window <my_alias>;
define <operator_type> operator <my_alias>;
```

### Creational statements

```tremor
create stream <my_alias>
create script <my_definition>;
create script <my_alias> from <my_definition>;
create operator <my_definition>;
create operator <my_alias> from <my_definition>;
create pipeline <my_definition>;
create pipeline <my_alias> from <my_definition>;
create connector <my_definition>;
create connector <my_alias> from <my_definition>;
deploy flow <my_alias> from <my_definition>;
```


### args and with

```
args
   <target>[ = <source>]
with
   <target> = <source>
end
```

## in the script syntax

### let
```tremor
let <target> = <srouce>;
```

### merge and patch
```tremor
merge <target> from <source> end;
patch <target> from <source> end;
```

### records

```tremor
{
    "<name>": <source-data>
}

## non conforming

### connect

`connect` switches target and source, while it would be possible to swap those around to follow the same `target` first `source` second pattern, it is counter-intuitive to the fact that tremor uses the English language and pipelining here is commonly thought of as something going from left to right.

```tremor
connect <source> to <target>;
```

### match and for

`match` and `for` both do not follow the pattern of `target` first `source` second. The reasoning for this is twofold. Again with English being the foundation of the language for control statements, it is more idiomatic to put the condition before the consequence. This pattern is also widespread in other programming languages.

The second factor is that we have multiple possible targets, and moving them upfront would lead to a very hard-to-read construct.

```tremor
match <source> of case <target> => ... end;
```

```tremor
for <source> in
  case <target> => ...
end;
```

### select

`select is a unique case as it conforms and does not conform to this pattern in different places. We look at the different parts and explain why.


The non-conforming part is the `from` / `to` section. The reasoning here follows the same reasoning in `connect` we have a dataflow, and in the English language, this typically moves left to right.


```tremor
... from <source> to <target>
```

The conforming part is the `select` / `from` section. Here we have the target (`script`) on the left and the source (`from`) on the right.  We choose This to conform with SQL.


```tremor
select <target> from <source> ...
```