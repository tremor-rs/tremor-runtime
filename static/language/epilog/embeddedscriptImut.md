
This rule is used in productions that contain an optional `script` element.

As such, it does not have an `end` token. That token is defined by the parent rule.

The host rule will terminate with an `end` so `end` in an optional embedded script isn't needed.

