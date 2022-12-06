
A case definition in a pattern match function definition

```tremor
use std::type;

fn snottify(s) of
  # Matches the literal string "badger"
  case ("badger") => "snot badger, hell yea!"
  # Matches any well formed json argument
  case (~ json||) => let s.snot = true; s
  # Matches any literal string
  case (s) when type::is_string(s) => "snot #{s}"
  # Matches, everything else
  case _ => "snot caller, you can't snottify that!"
end;
```

