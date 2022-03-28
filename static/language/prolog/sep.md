The `Sep` rule is a [LALRPOP](http://lalrpop.github.io/lalrpop/) convenience that allows defining
a [macro rule](http://lalrpop.github.io/lalrpop/tutorial/006_macros.html) template for a common 
sub rule sequence.

The `Sep` macro rule definition in tremor DSLs allows lists or sequences of expressions to
be separated by a specified delimiter. The delimiter is optional for the final item in a list
or sequence.


|Argument|Description|
|---|---|
|T|The term rule - specifies what is to be separated|
|D|The delimiter rule - specifies how elements are separated|
|L|A list of accumulated terms|

