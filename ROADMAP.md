# ToskaStore Roadmap

ToskaStore should not try to compete head-on as "Redis, but Elixir." Redis, Valkey, and Dragonfly already own the Redis-compatible data-structure server category through mature protocols, client ecosystems, clustering, replication, and raw throughput. etcd owns the strongly consistent distributed coordination category through revisions, watches, leases, transactions, and Raft.

ToskaStore's strongest competitive lane is narrower:

> A small, durable, HTTP-native operational KV store that is easier to deploy and reason about than Redis or etcd for simple apps, agents, edge services, internal tools, and config/state storage.

## Near-Term Priorities

### 1. Revisions, CAS, and Conditional Writes

Status: Complete.

Implementation checkpoints:

- [x] Store per-key metadata in ETS, snapshots, and AOF records.
- [x] Preserve compatibility with existing value-only AOF/snapshot records.
- [x] Add conditional write/delete support in the store API.
- [x] Expose metadata, `ETag`, and conditional writes over HTTP.
- [x] Add regression tests and API documentation.

Add per-key metadata:

- `version`
- `created_at`
- `updated_at`
- `expires_at`
- optional tombstone metadata for delete history if needed later

Expose safe conditional updates:

- `PUT /kv/:key` with `if_version`, `if_absent`, and `if_present`
- `DELETE /kv/:key` with `if_version`
- `GET /kv/:key` returns metadata
- HTTP `ETag` support for key versions

This is the foundation for safe distributed locks, optimistic concurrency, watch streams, and transactions.

### 2. Atomic Batch / Transaction Endpoint

Status: Complete.

Implementation checkpoints:

- [x] Add store-level compare-and-apply execution inside the KV GenServer.
- [x] Support `version`, `exists`, and `value` comparisons.
- [x] Support `put`, `delete`, and `get` transaction operations.
- [x] Expose `POST /kv/txn` over HTTP.
- [x] Add regression tests and API documentation.

Add `POST /kv/txn` for compare-and-apply operations:

```json
{
  "compare": [{"key": "x", "version": 3}],
  "success": [{"op": "put", "key": "x", "value": "next"}],
  "failure": []
}
```

The current GenServer serialization model makes this practical without server-side scripting. This enables locks, counters, config swaps, idempotency records, and job claiming.

### 3. Watch / Change Feed

Add an HTTP-native change stream:

- `GET /kv/watch?prefix=...&since_revision=...`
- Server-Sent Events as the first transport
- durable replay from AOF/revision history where feasible
- events for `put`, `delete`, and `expire`

This is one of the most important differentiators for ToskaStore's HTTP-native positioning.

### 4. Leases and Locks

Build coordination primitives on top of revisions and TTLs:

- `POST /leases`
- `POST /leases/:id/keepalive`
- `DELETE /leases/:id`
- `PUT /kv/:key` with `lease_id`
- `POST /locks/:name/acquire`
- `POST /locks/:name/release`

This gives ToskaStore a clear lightweight-coordination use case without requiring users to deploy etcd.

### 5. Scalable Prefix Range API

Improve key enumeration beyond in-memory sorting:

- ordered key index or ordered ETS-backed representation
- `GET /kv?prefix=&start=&limit=&cursor=`
- optional `include_values=true`
- optional metadata in range responses
- stable pagination without collecting all matching keys first

This keeps the key-listing API useful as datasets grow.

### 6. OpenAPI Spec and Client SDKs

Add a maintained API contract:

- `openapi.yaml`
- generated or hand-maintained clients for Elixir, Go, TypeScript, and Python
- examples for common workflows: cache, config store, locks, idempotency keys, and watches

ToskaStore's main adoption advantage is HTTP/JSON ergonomics, so client friction should be very low.

### 7. Production Security

Move beyond one shared token:

- separate read, write, admin, and replication tokens
- named tokens for auditability
- optional TLS configuration
- optional mTLS for replication and admin endpoints
- audit log for writes and admin changes

### 8. Crash Recovery Hardening

Make durability boring and explicit:

- tolerate truncated or corrupt AOF tails
- startup recovery report
- explicit durability mode in `/stats`
- crash/restart test harness
- snapshot verification CLI
- documented recovery procedures

Durability trust matters more than feature breadth for this project.

## Later Candidates

### Redis Protocol Compatibility Subset

A minimal RESP-compatible subset could unlock existing clients:

- `GET`
- `SET`
- `DEL`
- `MGET`
- `EXPIRE`
- `TTL`
- `SCAN`
- `INCR`

This should remain a subset. Full Redis compatibility would be a large project and would dilute ToskaStore's simpler HTTP-native identity.

### Additional Data Types

Add only the obvious primitives first:

- counters
- hashes/maps
- sets

Avoid streams, sorted sets, probabilistic structures, and scripting until there is clear demand.

### Raft Clustering

Strongly consistent clustering would make ToskaStore more directly competitive with etcd, but it is a large architectural step. Prefer single-leader durability, follower replication, watches, leases, and clear failover documentation first.

### Storage Engine Evolution

For larger-than-memory datasets, evaluate an LSM-style or embedded durable backend. Do this only after the API and durability model are stable enough to preserve compatibility across storage engines.

## Recommended Next PR

Implement the watch / change feed.

Per-key revisions, conditional writes, and atomic transactions are now in place, so the next highest-leverage step is an HTTP-native stream for key changes.
