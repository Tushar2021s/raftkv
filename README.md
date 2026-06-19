# raftkv

A production-grade distributed key-value store built on the Raft consensus algorithm, written from scratch in Go without external dependencies beyond the standard library.

## Architecture

```
┌──────────────────────────────────────────────────┐
│  Client (HTTP / CLI)                             │
└────────────────────┬─────────────────────────────┘
                     │ REST API (PUT/GET/DELETE)
┌────────────────────▼─────────────────────────────┐
│  kvstore.Server  (HTTP handler + redirect logic)  │
├───────────────────────────────────────────────────┤
│  kvstore.StateMachine  (apply loop, dedup, snap)  │
├───────────────────────────────────────────────────┤
│  raft.Node  (leader election, log replication,    │
│              snapshotting, crash recovery)         │
├───────────────────────────────────────────────────┤
│  Transport (LocalTransport for tests /            │
│             HTTPTransport for real clusters)      │
│  Persister (MemoryPersister / FilePersister)      │
└───────────────────────────────────────────────────┘
```

## Progress

- [x] Stage 1 — Scaffold: types, RPC contracts, Transport/Persister interfaces
- [x] Stage 2 — Leader election: RequestVote, randomised timeouts, split-vote recovery
- [x] Stage 3 — Log replication: AppendEntries, majority commit, fast backtracking
- [x] Stage 4 — KV state machine: HTTP API, leader redirect, idempotent writes, CLI client
- [x] Stage 5 — Crash-safe persistence: fsync, atomic rename, CRC32 checksums
- [x] Stage 6 — Snapshotting / log compaction: TakeSnapshot, InstallSnapshot RPC, auto-compaction
- [x] Stage 7 — Dynamic membership changes: joint consensus, AddMember, RemoveMember, leader self-removal
- [ ] Stage 8 — Chaos-testing harness (simulated network partitions, drops, kills)
- [ ] Stage 9 — Benchmarking (throughput, latency percentiles, failover time)
- [ ] Stage 10 — Architecture write-up with real numbers

## Running a real 3-node cluster

```bash
# Terminal 1
go run ./cmd/server -id 0 -cluster localhost:7000:8000,localhost:7001:8001,localhost:7002:8002

# Terminal 2
go run ./cmd/server -id 1 -cluster localhost:7000:8000,localhost:7001:8001,localhost:7002:8002

# Terminal 3
go run ./cmd/server -id 2 -cluster localhost:7000:8000,localhost:7001:8001,localhost:7002:8002

# Terminal 4 — interact
go run ./cmd/client -server localhost:8000 put name tushar
go run ./cmd/client -server localhost:8000 get name
go run ./cmd/client -server localhost:8001 status
```

## Key engineering decisions

| Decision | Why |
|---|---|
| Hand-rolled HTTP/JSON RPC | No gRPC toolchain needed; every RPC is curl-able |
| `logicalToPhysical()` for all log indexing | Safe after compaction; no-op before first snapshot |
| Atomic write-then-rename with fsync | Crash mid-write never corrupts the file |
| CRC32 checksum prefix on persisted files | Detects bit-rot and truncated writes on read |
| Signal pending *before* TakeSnapshot | Client latency excludes compaction time |
| Reset nextIndex after TakeSnapshot | Eliminates race between compaction and replication |
| Synchronous snapshot in apply loop | Snapshot on disk before next replication round |

## Running tests

```bash
go test ./... -v -race
```
