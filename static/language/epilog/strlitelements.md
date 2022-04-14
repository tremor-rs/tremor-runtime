### Inside a literal tremor string

A string literal in tremor is a composition of multiple segments or parts.

These can be composed of:
* One or many single line string parts
* One or many multi line string parts
* A blackslash escaped `\\#` to escape interpolated syntax, optinally followed by more string literal parts
* Or, a `#{` .. `}` delimited interpolated section
  * Within an interpolated section there are no constraints on raw newline usage
  * For complex interpolated sections, prefer good indentation!


