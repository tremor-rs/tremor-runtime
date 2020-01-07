# The system namespace

The system namespace contains functions that provide information about the tremor runtime system.

## Functions

### system::hostname() -> string

Returns the name of the host where tremor is running.

### system::ingest_ns() -> int

Returns the ingest time into tremor of the current event.
