# tessera-sink-worker

The worker executable spawned by [`tessera-sink`](../tessera-sink/). One
process per worker; the `Sink` owner launches N of these.

It is a thin shell over `tessera_sink::run_worker`: it parses the argv
contract (`--pool-description`, `--control-description`,
`--ack-description`, slot geometry, `--worker-id`) and runs the worker
loop. You normally never invoke this by hand — the owner discovers and
spawns it (config path → `TESSERA_SINK_WORKER_BIN` → sibling-of-exe →
`PATH`).

**Status:** v0.0.1. Functional in development builds; not yet
published.
