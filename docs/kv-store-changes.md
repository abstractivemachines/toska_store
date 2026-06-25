# KV Store Changes

This document summarizes the key/value store work added to Toska.

## Scope

- Added a durable string-to-string KV store with TTL support.
- Implemented JSON append-only log (AOF) + JSON snapshot persistence.
- Exposed HTTP/JSON endpoints for KV operations and store stats.
- Added replication scaffolding endpoints for snapshot + AOF streaming.
- Added snapshot/AOF integrity checks with checksums and recovery behavior.
- Added HTTP integration tests and a basic benchmark script.
- Added a replication follower process that bootstraps from snapshot and tails the AOF stream.
- Added a `toska replicate` CLI command with daemon support.
- Added follower offset persistence and replication status endpoint (`/replication/status`).
- Expanded configuration defaults, validation, and documentation.
- Added AOF compaction thresholds/intervals with manual `compact` support.
- Added scoped KV/admin/replication auth tokens, rate limiting, and read-only enforcement in follower mode.

## Core Storage

- New ETS-backed store with read/write concurrency: `apps/toska/lib/toska/kv_store.ex`.
- TTL handling with periodic cleanup and per-key expiration timestamps.
- AOF replay at boot; snapshots written periodically and on demand.
- Sync modes for durability vs throughput: `always`, `interval`, `none`.

## Server Lifecycle

- Server now starts the KV store on boot and stops it on shutdown: `apps/toska/lib/toska/server.ex`.

## HTTP API

- JSON request parsing enabled for the router.
- New endpoints:
  - `GET /kv/:key`
  - `PUT /kv/:key` with `{"value": "...", "ttl_ms": 5000}`
  - `DELETE /kv/:key`
  - `POST /kv/mget` with `{"keys": ["a", "b"]}`
  - `GET /stats`
  - plus the existing `/status` and `/health`.
- Replication endpoints:
  - `GET /replication/info`
  - `GET /replication/snapshot`
  - `GET /replication/aof?since=0`
- Router updates live in `apps/toska/lib/toska/router.ex`.

## Configuration

- Default config now includes persistence and TTL scheduler settings:
  - `data_dir`, `aof_file`, `snapshot_file`
  - `sync_mode`, `sync_interval_ms`
  - `snapshot_interval_ms`, `ttl_check_interval_ms`
- New validations for the above: `apps/toska/lib/toska/config_manager.ex`.
- Config CLI help updated: `apps/toska/lib/toska/commands/config.ex`.
- Docs updated in `README.md` and `apps/toska/README.md`.

## Tests

- Added KV store tests for put/get, TTL expiration, and AOF replay:
  - `apps/toska/test/kv_store_test.exs`
- Added HTTP router integration tests:
  - `apps/toska/test/router_kv_test.exs`
- Benchmark script:
  - `scripts/bench_kv.exs`
