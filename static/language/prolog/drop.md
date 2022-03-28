Drop halts event processing for the current event being processed returning
control to the tremor runtime, dropping the event.

### Constraints

The `drop` operation should be used with care as the in-flight event is
discarded by the runtime. Where circuit breakers, guaranteed delivery and
quality of service operations are being managed by the engine downstream
these should be carefully programmed so that `drop` operations have no
side-effects on non-functional behaviours of the tremor runtime.

Here be dragons!

