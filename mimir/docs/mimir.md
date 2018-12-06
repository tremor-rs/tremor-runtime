# mimir Cheatsheet

Ivan Monov

https://git.csnzoo.com/data-engineering/mimir

# About

mimir is a programmable rules engine

# Basic queries

mimir is case-sensitive.  The following queries will return different results based on the case sensitivity of the rule and the data.

```
foo="bar"

foo:"bar"

FOO="BAR"

Foo="BaR"
```

Data example matching the term `bar` all lowercase:

```
data = {
  ...
  "foo": "bar",
  ...
}
```

This document will match the first 2 queries from above (`foo=bar` and `foo:bar`)

Note: colon (`:`) is a contains the word operator while equals (`=`) is a match exact operator.

## Joins

Default operator between multiple terms is an AND

```
pet:"cat" pet:"dog"
pet:"cat" AND pet:"dog"

```

Data example for matching queries using `AND`:

```
data = {
  ...
  "pet": "cats, dogs, tigers",
  ...
}
```

Data example which doesn't match (`dog` is missing):

```
data = {
  ...
  "pet": "cats, tigers",
  ...
}
```


## Union

```
pet="cat" OR pet="dog"

pet:["cat", "dog"]
```

Matching example:

```
data = {
  ...
  "pet": "cats, tigers",
  ...
}
```

Another matching example.

```
data = {
  ...
  "pet": "dogs",
  ...
}
```

## Nesting

```
(pet:"cat" pet:"dog") (food:"peanut butter" food:"jelly")
```

Data example for nested query match:

```
data = {
  ...
  "pet": "dogs",
  "food": "peanut butter and jelly",
  ...
}
```

Data example for non-matching nested query:

```
data = {
  ...
  "pet": "dogs",
  "food": "peanut butter",
  ...
}
```

## Negation

WARNING: Not yet implemented

Excludes specified terms from the query using `NOT` or `!` operators.

```
pet:"cat" AND NOT pet:"dog"
pet:"cat" AND !pet:"dog"
```

Data example for matching negation query:

```
data = {
  ...
  "pet": "cats are great",
  ...
}
```

Data example for non-matching negation query:

```
data = {
  ...
  "pet": "cats and dogs are great",
  ...
}
```

## Wildcards

WARNING: Not yet implemented

Question mark (`?`) matches a single, arbitrary character.

Asterisk (`*`) matches any word or phrase.

```
message:g"error"

message:g"err*"

message:g"err?r"

```

Data example for wildcard query matching the search:

```
data = {
  ...
  "message": "error",
  ...
}
```

```
data = {
  ...
  "message": "err0r",
  ...
}
```

```
data = {
  ...
  "message": "erratic",
  ...
}
```

Data example for wildcard query which doesn't match the search:

```
data = {
  ...
  "message": "eradicate",
  ...
}
```

## Regex

Any characters surrounded by slashes (`/`). PCRE compatible expressions are allowed.

Backslash is used to escape a slash in the expression.

```
message:/(?i).*error.*/
```

Data examples for regex query which matches the above search:

```
data = {
  ...
  "message": "error",
  ...
}
```

```
data = {
  ...
  "message": "eRRoR",
  ...
}
```

# Operators

## IPs

WARNING: Not yet implemented

```
wf_host_ip:10.228.75.76

```

Data example for IP query which matches the search:

```
data = {
  ...
  "wf_host_ip": "10.228.75.76",
  ...
}
```

Using wildcard in the IP search
```
wf_host_ip:g"10.228.*"
```

Data example for IP query which matches the search:

```
data = {
  ...
  "wf_host_ip": "10.228.0.0",
  ...
}
```


## CIDRs

WARNING: Not yet implemented

```
wf_host_ip:10.228.0.0/16
```

Data example for CIDR query which matches the search:

```
data = {
  ...
  "wf_host_ip": "10.228.1.1",
  ...
}
```

## List references

WARNING: Not yet implemented

List references can be initialized using the colon equals operator (`:=`)

An initialized list might be used within a query by prepending the dollar sign character (`$`) to it's name.

For example the following declares a list named `bar` and instantiates a query which uses the declared list:

```
bar := ["item1", "item2", "item3"]

foo:$bar
```

Data example for lists by reference query which matches the search:

```
data = {
  ...
  "foo": "item2",
  ...
}
```

Data example for list by reference query which doesn't match the search:

```
data = {
  ...
  "foo": "item4",
  ...
}
```

## Inline lists

WARNING: Not yet implemented

If foo is a single string item, and if it is either item1, item2 or item2, it is a positive match.

If foo is an array containing all three elements (item1, item2, and item3), it is also a match.

If foo is an array which doesn't contain all three items, then the specified query below isn't a match.

```
foo:["item1", "item2", "item3"]

```

Data example for inline list query which matches the search:

```
data = {
  ...
  "foo": "item2",
  ...
}
```

Data example for inline list query which matches the search:

```
data = {
  ...
  "foo": ["item1", "item2", "item3", "item4"],
  ...
}
```

Data example for inline list query which matches the search:

```
data = {
  ...
  "foo": ["item1", "item2", item4"],
  ...
}
```

## Greater and Less than

Ints or floats

```
level>3 level<5 measure>2.5 measure<3.0

```

Data example where the search matches:

```
data = {
  ...
  "level": 4,
  "measure": 2.75,
  ...
}
```

Data example where the search doesn't match:

```
data = {
  ...
  "level": 1,
  "measure": 3.75,
  ...
}
```

## Exists and missing

WARNING: Not yet implemented

Looks for `foo` to be defined and `bar` to be undefined.

```
_exists_:"foo" _missing_:"bar"

```

Data example where the search matches:

```
data = {
  ...
  "foo": "cats",
  ...
}
```

Data example where the search doesn't match:

```
data = {
  ...
  "foo": "cats",
  "bar": "dogs",
  ...
}
```

## Nested queries

Query nested `subkey1` for `value`
```
.key1.subkey1:\"value\"
```

Data example:
```
data = {
  ...
  "key1": {
    "subkey1":"value"
  }
  ...
}
```

Query nested `subkey1` in `arrkey`

WARNING: Not yet implemented

```
.key1.arrkey[0].subkey1:\"value1\"
```

Data example:
```
data = {
  ...
  "key1": {
    "arrkey": [{"subkey1":"value1", "subkey2":"value2"}]
  }
  ...
}
```

## Mutating data

WARNING: Not yet implemented

Query nested `subkey1` for `value`. If true, set field `index` to `index-value`.

```
.key1.index:"value" {.index := "index-value"}
```
Data example:

Before mutation:
```
data = {
  ...
  "key1": {
    "subkey1":"value"
  }
  ...
}
```

After mutation:

```
data = {
  ...
  "key1": {
    "subkey1":"value"
  },
  "index":"index-value"
  ...
}
```





