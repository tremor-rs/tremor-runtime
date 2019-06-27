# Base64

The `base64` extractor decodes content encoded with the Base64 binary-to-text encoding scheme into the corresponding string value.

## Predicate

When used as a predicate test with `~`, and the referent target is a valid string and is base64 encoded, then the test succeeds.

## Extraction

If predicate test succeeds, then the decoded base64 content it extracted and
returned as a string literal.

## Example

```tremor
match { "test": "8J+MiiBzbm90IGJhZGdlcg==", "footle":·"bar" } of
  case foo = %{test ~= base64|| } => foo
end;
## Output: 🌊 snot badger
```
