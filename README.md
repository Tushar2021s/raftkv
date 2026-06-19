# raftkv

A distributed, fault-tolerant key-value store built from scratch on top of a
complete implementation of the Raft consensus algorithm
([Ongaro & Ousterhout, 2014](https://raft.github.io/raft.pdf)).

No consensus libraries. No gRPC. No off-the-shelf Raft package. Every RPC,
every election, every log-replication edge case, the persistence layer, the
snapshotting mechanism, the membership change protocol, and the network layer
itself are implemented from first principles in Go.

---

## Measured Performance

All numbers measured on a 2024 MacBook Air (Apple M2), in-process cluster.

### Write Latency — 3-node cluster, 400 operations per run

| Concurrency | p50 | p90 | p99 |
|---|---|---|---|
| 1 writer | 9.995ms | 10.343ms | 10.880ms |
| 4 writers | 10.003ms | 10.452ms | 10.750ms |
| 8 writers | 9.986ms | 10.090ms | 10.255ms |

**Under 10% simulated packet loss (4 writers):** p50 9.996ms · p90 10.082ms · p99 10.877ms
— latency is essentially unchanged, showing the retry mechanism absorbs packet loss without tail-latency blowup.

**3-node vs 5-node cluster (4 writers, 300 ops):**

| Cluster | p50 | p90 | p99 |
|---|---|---|---|
| 3-node | 10.007ms | 10.106ms | 10.211ms |
| 5-node | 9.994ms | 10.085ms | 10.714ms |

Adding nodes does not meaningfully increase write latency — replication is pipelined.

### Leader Failover — 5-node cluster, 20 measured rounds

| p50 | p90 | p99 |
|---|---|---|
| 360ms | 454ms | 467ms |

The tight p90→p99 gap (13ms) means failover time is consistent, not occasionally catastrophic.

### Throughput

**803 writes/sec** sustained from 8 concurrent clients against a 3-node cluster with no faults.

---

## Architecture

```
                 ┌──────────────────────────────────┐
 HTTP client ──► │  kvstore.Server                  │
                 │  GET /kv/{key}                   │
                 │  PUT /kv/{key}   → 307 if not    │
                 │  DELETE /kv/{key}   leader        │
                 │  POST /cluster/add               │
                 │  POST /cluster/remove            │
                 └──────────────┬───────────────────┘
                                │ Put / Get / Delete
                 ┌──────────────▼───────────────────┐
                 │  kvstore.StateMachine             │
                 │  • in-memory map[string]string    │
                 │  • reqID deduplication (exactly   │
                 │    once delivery on retry)        │
                 │  • auto-snapshot at threshold     │
                 └──────────────┬───────────────────┘
                                │ Submit / applyCh
                 ┌──────────────▼───────────────────┐
                 │  raft.Node                        │
                 │  • leader election                │
                 │  • log replication                │
                 │  • majority commit                │
                 │  • snapshotting / compaction      │
                 │  • dynamic membership             │
                 └────────┬──────────────┬──────────┘
                          │              │
              ┌───────────▼──┐  ┌────────▼──────────┐
              │  Transport   │  │  Persister        │
              │  (interface) │  │  (interface)      │
              └──────┬───┬───┘  └────────┬──────────┘
                     │   │               │
          ┌──────────▼┐ ┌▼────────────┐ ┌▼──────────────┐
          │HTTP        │ │SimNetwork   │ │FilePersister  │
          │Transport   │ │(chaos tests)│ │fsync + CRC32  │
          └────────────┘ └─────────────┘ └───────────────┘
```

**Key design principle:** `raft.Node` only knows about the `Transport` and
`Persister` interfaces. It never imports `net/http` or touches a file directly.
This is what makes the chaos-testing harness possible: `SimNetwork` swaps in as
a drop-in `Transport` that can drop, delay, or partition messages on command —
no changes to consensus logic required.

---

## What's Implemented

### Stage 1 — Scaffold
Core types (`LogEntry`, `ApplyMsg`, `ServerState`), RPC structs
(`RequestVoteArgs`, `AppendEntriesArgs`, `InstallSnapshotArgs`), and the
`Transport`/`Persister` interfaces that decouple consensus from networking and
disk I/O.

### Stage 2 — Leader Election
`RequestVote` RPC, randomised election timeouts (300–600ms), heartbeat loop.
Randomisation is what makes split votes rare: two candidates timing out in the
same millisecond is vanishingly unlikely. Tests confirm a different node wins
each run.

### Stage 3 — Log Replication
`AppendEntries` RPC carrying real log entries, majority-commit rule with the
§5.4.2 current-term-only constraint (prevents silent data loss on re-election),
and fast log backtracking via `ConflictIndex`/`ConflictTerm` — a lagged
follower is caught up in O(1) RPCs instead of O(n).

### Stage 4 — KV State Machine + HTTP API
`kvstore.StateMachine` consuming `applyCh` and maintaining the in-memory map.
`reqID`-based deduplication for exactly-once delivery on client retry. HTTP
server exposing `GET/PUT/DELETE /kv/{key}` with 307 redirects to the leader
so clients can contact any node. CLI client binary included.

### Stage 5 — Crash-Safe Persistence
`FilePersister`: write-to-tmp-then-rename (POSIX `rename()` is atomic so
readers never see a half-written file), `fsync` before rename (forces data out
of OS page cache before the rename makes it visible), CRC32 checksum prefix
(detects bit-rot on read). A restarted node recovers its term, votedFor, and
log without replaying anything from peers.

### Stage 6 — Snapshotting / Log Compaction
`TakeSnapshot(index, data)` called by the state machine after applying
`snapshotThreshold` entries: trims the log to a single sentinel entry,
persists the snapshot bytes, and immediately resets `nextIndex` for all peers
so the next replication round correctly falls through to `InstallSnapshot`
rather than trying to slice into a compacted log. `HandleInstallSnapshot`
catches up a lagged follower that needs entries the leader has already
discarded.

### Stage 7 — Dynamic Membership (Joint Consensus)
`ChangeConfig(ConfigChange{Type: AddNode/RemoveNode, NodeID: N})` proposes a
membership change via joint consensus: the cluster enters a transitional state
where both the old and new member sets must independently reach majority before
anything commits. This prevents the split-brain scenario where two disjoint
subsets of nodes simultaneously elect leaders during a transition. Leader
self-removal causes automatic step-down. Exposed via `POST /cluster/add` and
`POST /cluster/remove`.

### Stage 8 — Chaos Testing Harness
`chaos.SimNetwork` implements `raft.Transport` with three fault modes:
- `Partition(a, b)` — blocks all RPCs between two nodes bidirectionally
- `SetDropRate(r)` — randomly drops fraction `r` of all messages
- `SetDelay(min, max)` — adds artificial latency to every delivered RPC

Five chaos scenarios: leader crash failover timing, network partition and
recovery, 20% packet loss (all 20 writes succeed), concurrent write throughput
(803 ops/sec), and split-brain prevention (minority partition correctly blocked
from committing).

### Stage 9 — Latency Benchmarks
`bench` package measuring p50/p90/p99 write latency across concurrency levels
(1/4/8 writers), cluster sizes (3-node vs 5-node), and fault conditions (10%
packet loss). Failover latency distribution over 20 rounds. Results above.

---

## Running a Real Cluster

Three terminal tabs from the project root:

```bash
# Tab 1
go run ./cmd/server -id 0 -cluster localhost:7000:8000,localhost:7001:8001,localhost:7002:8002

# Tab 2
go run ./cmd/server -id 1 -cluster localhost:7000:8000,localhost:7001:8001,localhost:7002:8002

# Tab 3
go run ./cmd/server -id 2 -cluster localhost:7000:8000,localhost:7001:8001,localhost:7002:8002
```

Then from any fourth tab:

```bash
go run ./cmd/client -server localhost:8000 put name tushar
go run ./cmd/client -server localhost:8000 get name
go run ./cmd/client -server localhost:8001 status
# → nodeId=1  state=Follower  term=1  leader=0
```

Kill any node and writes/reads still work — the cluster re-elects a leader
within ~400ms.

---

## Running Tests

```bash
# Full suite with race detector
go test ./... -race -timeout 180s

# Chaos tests only
go test ./chaos/... -v -race

# Latency benchmarks (prints p50/p90/p99)
go test ./bench/... -v -run "TestWriteLatency|TestFailoverLatency"
```

---

## Repository Layout

| Path | What lives there |
|---|---|
| `raft/` | Core consensus: election, replication, persistence, snapshotting, membership |
| `transport/` | `HTTPTransport` (real cluster) and `LocalTransport` (fast unit tests) |
| `kvstore/` | State machine, HTTP server, CLI client |
| `chaos/` | `SimNetwork` fault injector and chaos test scenarios |
| `bench/` | p50/p90/p99 write latency and failover latency benchmarks |
| `cmd/server` | Runnable server binary |
| `cmd/client` | Runnable CLI client |

---

## Why No gRPC?

Writing the RPC layer from scratch (plain HTTP + JSON) has two advantages.
First, it forces genuine understanding of how nodes agree on anything, rather
than treating consensus as a library call. Second, it makes the `SimNetwork`
swap-in possible: the chaos harness needs to intercept every message between
nodes, which is trivial when the transport is an interface you control and
impossible when it's a real socket library. This is the same approach MIT's
6.5840 distributed systems course and FoundationDB's deterministic simulation
framework use.
