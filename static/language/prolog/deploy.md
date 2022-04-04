The `Deploy` rule defines the logical entry point into Tremor's command
oriented deployment syntax. The deployment grammar defines units of
deployment that the runtime manages on behalf of users.

The grammar embeds the statement oriented query syntax and expression
oriented scripting syntax where appropriate.

A legal deployment is composed of:
* An optional set of module comments
* A sequence of top level expressions. There must be at least one defined.
* An optional end of stream token.

At least one *deploy* command.

