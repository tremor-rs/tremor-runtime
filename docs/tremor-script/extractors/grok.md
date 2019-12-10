# Grok

The grok extractor is useful for parsing unstructured data into a structured form. It is based on logstash's grok plugin. Grok uses regular expressions, so any regular expression can be used as a grok pattern.

Grok pattern is of the form `%{SYNTAX : SEMANTIC}` where `SYNTAX` is the name of the pattern that matches the text and `SEMANTIC` is the identifier

## Predicate

When used with `~`, the predicate passes if the target matches he pattern passed by the input (fetched from the grok pattern's file).

## Extraction

If the predicate passes, the extractor returns the matches found when the target was matched to the pattern.

## Example

```tremor
match { "meta": "55.3.244.1 GET /index.html 15824 0.043" } of
  case rp = %{ meta ~= grok |%{IP:client} %{WORD:method} %{URIPATHPARAM:request} %{NUMBER:bytes} %{NUMBER:duration} | } => rp
  default => "no match"
end;
```
