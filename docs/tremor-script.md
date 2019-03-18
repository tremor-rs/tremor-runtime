# Tremor Script

Tremor script is the scripting language that is used inside of tremor to mutate and inspect events. It is provided by the [tremor::runtime](../operators/#runtimetremor) operator.

Note: This document looks purely at the script not how YAML wraps the configuration.

There are two primary use cases for tremor script, inspection, and mutation.

## Overview

### Inspection

We talk about inspection when we want to look at the event data and make decisions on its content. tremor-script uses 'conditions' for this.

### Mutation

With mutation, the goal is slightly different, for mutation we want to either add, remove or change data in the event we're processing. Usually, mutations happen after the inspection found out that an event matches specific criteria.

### Metadata variables

Lastly, metadata variables are another vital concept that all operators in tremor share, but is especially crucial for tremor-script.

Every event carries metadata as it passes through the pipeline. This metadata is used to communicate information on the event that is not part of the event's data.

For example the [grouper::bucket](../operators/#grouperbucket) operator uses the metadata metadata variables `$class`, `$rate` and `$dimensions` to perform it's work.

Tremor script can be used to both set and read these variables.

## Syntax

The tremor-script syntax consists of comments, an import/export block and a set of statements.

#### Import/Export

The script can start with an `import` and `export` statement. This method is used to define what variables in tremor script leak to the event and what variables in the event leak into the script.

Let's talk about imports first. An imported variable initially has the value of the corresponding metadata variable. So if the metadata variable `$foo` is  `5` the following script would match its first rule:

```tremor-script
import foo;

$foo=5 {  }  // this rule matches
```

However, import does not imply an export! So if imported variables change inside the script without exporting it again, those changes are script local! So in the example below:

```tremor-script
import foo;

$foo=5 { $foo := 6; }  // this rule matches
$foo=6 { $foo := 7; }  // this rule matches too
```

The metadata variable `$foo` remains `5` while the local variable `$foo` changes from `5` to `6` to `7`.

To make this change propagate to the outside world, we introduce our second keyword: `export`. Export tells the engine that we are not dealing with a local variable but rather with an event scoped metadata variable. Taking the example above we can add an export statement so that the changes to `$foo` become not locally scoped but event scoped:

```tremor-script
import foo;
export foo;

$foo=5 { $foo := 6; }  // this rule matches
$foo=6 { $foo := 7; }  // this rule matches too
```

In the same sense that `import` doesn't imply `export`, `export` doesn't imply `import`. If we were to remove the `import` statement from the example above none of the rules would match:

```tremor-script
export foo;

$foo=5 { $foo := 6; }  // this rule does not matches as $foo isn't set
$foo=6 { $foo := 7; }  // this rule does not matches as $foo isn't set
```

#### comments

Tremor script allows comments after the import/export block. Comments are single line comments that go from `//` to the end of the line.

#### Statement

Statements consist of two elements a condition and an action. The action executes if the condition evaluations to true. If a script consists of multiple statements they are tested and executed in order; however, it is possible to return early from execution by using the `return` keyword in the action section.

#### conditions

Each statement begins with a condition. This condition is used to perform some test against the content of the event or the event metadata (when [imported](#importexport)).

There is a special condition `_` that always matches. This can be used to perform mutations that always are supposed to occur.

Otherwise, conditions consist of tests that can be combined by the logical operators `and`/`&&` and  `or`/`||` or be negated by `not`/`!`.

Tests consist of a * left-hand side* that denotes what data to look at, a *comparison operator* and a * right-hand side* that is compared against.

There is a particular case where a test only consists of a * left-hand side* in which only the existence of it is tested but not the content. It should be noted that we are testing existence, not truthfulness! So if `$foo` is set to `false` then the test `$foo` would still succeed, as `$foo` exists.

##### left hand side

The left-hand side can either be a variable as in `$something` either local or data path into the event as in `key.subkey.subsubkey`. If the key is not set the comparison fails automatically and return a non-match.


##### right hand side

The right-hand side can either be a variable, a datapath, a constant, a glob, a regexp, function call or a CIDR style network range.

We won't discuss the datapath or variable further here as they are already covered in many other places.

###### constants
Tremor script has a number of possible constants:

* Integers - such as `7`
* Floating Point numbers (floats) - such as `4.2`
* Strings - such as `"squirrel"`
* Array - such as `[1, "hello", 42, [7, 8]]` where arrays can contain both constants or function calls.

###### globs


Globs are a cheap and simple way to search the content of a string. They follow the same rules as standard UNIX shell globs patterns.

Globs are written as: `g"<glob here>"`, so for example `g"*INFO*"` matches every string that contains the characters `INFO`.

* `?` matches any single character.

* `*` matches any (possibly empty) sequence of characters.

* `**` matches the current directory and arbitrary subdirectories. This sequence must form a single path component, so both `**a` and `b**` are invalid and result in an error. A sequence of more than two consecutive * characters is also invalid.

* `[...]` matches any character inside the brackets. Character sequences can also specify ranges of characters, as ordered by Unicode, so, e.g. `[0-9]` specifies any character between 0 and 9 inclusive. An unclosed bracket is invalid.

* `[!...]` is the negation of `[...]`, i.e., it matches any characters, not in the brackets.

* The metacharacters `?`, `*`, `[`, `]` can be matched by using brackets (e.g. `[?]`). When a `]` occurs immediately following `[` or `[!` then it is interpreted as being part of, rather than ending, the character set, so `]` and NOT `]` can be matched by `[]]` and `[!]]` respectively. The `-` character can be specified inside a character sequence pattern by placing it at the start or the end, e.g. `[abc-]`.

###### regexp

Regular expressions allow for more complex dissections of string values. They are however very expensive. If possible, they should be avoided in favor of globs. We are using a [PCRE2](https://www.pcre.org/) compatible regular expression engine.

Regular expressions are written as: `r"<expression here"` so `r".*INFO.*"` would match any string that contains the characters `INFO` (note: this is an example! A Glob should be used for this test!).

###### CIDR

CIDRs are used to match IP v4 addresses. A CIDR won't match of the left-hand side isn't a string that can be translated to an IP v4 address.

CIDRs are written as `<ip>/<bits>`, the special form `<ip>` is allowed as a shorthand for `<ip>/32`.

A CIDR should only be used when:

1) A range is going to be matched.
2) The data field might contain an IP with leading zeros (i.e. `010.10.10.10` instead of always `10.10.10.10`)

If a specific IP string should be matched a simple string comparison is cheaper and should be used instead as deconstructing the IP string into an IP isn't required in that case.

###### function calls

When the right-hand side is a function call tremor compares against the result of the function call. A list of functions is provided in it's [own section](functions/).

##### Compairison operators

Tremor script has a few comparison operators: `=`, `>`, `<`, `>=`, `<=` and `:`.

The comparison operator `=` is used for every value type to itself as well as glob and regex values. Note, *Float* and *Integer* are considered different types and can't be directly compared using `=`.

The `:` operator is used for arrays to check inclusion, so `tags:"hello"` translates to "The array `tags` includes an element `"hello"`".


## Patterns

The following sections contain useful patterns that can be used for a given effect.

### default values

It often is helpful to set default values for local variables. When either these values are used in most other cases, or there is a good chance they are not going to be set due to no case matching.

```tremor-script
import something;
export var;

// set a default value
_ { $var := "default value"; }

// ... rest of the script
```

### early returns

From a performance perspective it is an advantage to return as early as possible and by that reduce the number of tests needed to execute. This can be achieved by adding a `return` statement to rules when we are sure that no other rule will or should match.

```tremor-script
export class;

// instead of
app="app1" { $class := "class1";}
app="app2" { $class := "class2";}
app="app3" { $class := "class3";}

// add return statements as we know that when app1="app1" the other cases can never match

app="app1" { $class := "class1"; return;}
app="app2" { $class := "class2"; return;}
app="app3" { $class := "class3"; return;}
```
### breaking after a set of rules

At times we have groups of rules that we know if one group matches the next one can never match and they share some common elements. The code can be made less repetitive by matching on a variable and setting the common element at the end. However, this comes at a performance penalty, and it should be considered not to write some more and use an early return instead - we still provide this pattern for completeness.

```tremor-script
export class;

// the two groups:
app="app1" { $class := "class1"; $dimensions := level; return;}
app="app2" { $class := "class2"; $dimensions := level; return;}
app="app3" { $class := "class2"; $dimensions := level; return;}

app="app4" { $class := "class1"; $dimensions := host; return;}
app="app5" { $class := "class2"; $dimensions := host; return;}
app="app6" { $class := "class2"; $dimensions := host; return;}

/// can be re-written with less typing but slower performance as:

app="app1" { $class := "class1";}
app="app2" { $class := "class2";}
app="app3" { $class := "class2";}
// If class was set at this point we know one of the earlier rules matched!
$class {$dimensions := level; return;}

app="app4" { $class := "class1";}
app="app5" { $class := "class2";}
app="app6" { $class := "class2";}
$class {$dimensions := host; return;}
```