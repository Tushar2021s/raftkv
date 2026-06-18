# raftkv

A distributed, replicated key-value store built from scratch on top of a
custom implementation of the Raft consensus algorithm
([Ongaro & Ousterhout, 2014](https://raft.github.io/raft.pdf)).

No consensus libraries, no gRPC, no off-the-shelf Raft package: every RPC,
every election, every log-replication edge case, and the network layer
itself are implemented from first principles in Go.

## Why build the RPC layer from scratch instead of using gRPC?

Two reasons. First, it forces a real understanding of how nodes actually
agree on anything, rather than treating consensus as a library call.
Second, it makes deterministic chaos-testing possible: the test harness
swaps in a fully in-memory network that can drop, delay, or partition
messages on command, instead of fighting real timing and real sockets to
simulate failures. This is the same approach MIT's 6.5840 distributed
systems course and FoundationDB's deterministic simulation framework use.

## Architecture

```
                 ┌──────────────────────────┐
 client ───────► │   kvstore.Server (any     │
                  │   node, redirects writes │
                  │   to current leader)     │
                  └─────────────┬─────────────┘
                                │ Put/Get/Delete
                  ┌─────────────▼─────────────┐
                  │      raft.Node            │
                  │  (election, replication,  │
                  │   commit, snapshotting)    │
                  └─────┬───────────────┬──────┘
                        │               │
              ┌─────────▼───┐   ┌───────▼────────┐
              │ raft.Transport│  │ raft.Persister │
              │ (interface)   │  │ (interface)    │
              └─────┬─────┬───┘  └────────┬───────┘
                    │     │               │
       ┌────────────▼┐   ┌▼─────────────┐ ▼
       │HTTPTransport │   │ SimNetwork   │ file-backed
       │ (real cluster)│   │(chaos tests) │  persister
       └──────────────┘   └──────────────┘
```

Raft's consensus logic (`raft/`) never imports `net/http` or touches a
file directly — it only knows about the `Transport` and `Persister`
interfaces. That's what makes the chaos-testing harness possible without
any special-casing inside the consensus code itself.

## Progress

- [x] Stage 1 — Scaffold: core types, RPC contracts, Transport/Persister interfaces
- [x] Stage 2 — Leader election (RequestVote, randomized timeouts, heartbeats)
- [x] Stage 3 — Log replication (AppendEntries, majority commit, fast backtracking, Submit API)
- [x] Stage 4 — KV state machine + HTTP API (Put/Get/Delete, leader redirect, idempotent writes, CLI client)
- [ ] Stage 5 — Crash-safe persistence
- [ ] Stage 6 — Snapshotting / log compaction
- [ ] Stage 7 — Dynamic cluster membership changes
- [ ] Stage 8 — Chaos-testing harness (simulated network)
- [ ] Stage 9 — Benchmarks (throughput, latency percentiles, failover time)

## Layout

| Path | Purpose |
|---|---|
| `raft/` | Core consensus: election, replication, persistence, snapshotting |
| `transport/` | Concrete `Transport` implementations: real HTTP, and the simulated network used for testing |
| `kvstore/` | The key-value state machine and client-facing API that sits on top of Raft |
| `cmd/server`, `cmd/client` | Runnable binaries |
| `chaos/` | Fault-injection test scenarios |
| `bench/` | Throughput/latency benchmarking |
