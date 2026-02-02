# Codebase Assessment

This document provides an analysis of the ToskaStore codebase, identifying architectural patterns, strengths, and areas for improvement.

## Overview

ToskaStore is a disk-backed key-value store written in Elixir with an HTTP/JSON REST API and CLI interface. It uses ETS for in-memory storage with AOF (Append-Only File) and snapshots for persistence.

**Version**: 0.9.1
**Type**: Umbrella Elixir project

## Project Structure

```
apps/toska/
├── lib/toska/
│   ├── toska.ex           # Main public API
│   ├── cli.ex             # escript entry point
│   ├── application.ex     # OTP Application supervision
│   ├── kv_store.ex        # Core KV store (ETS + AOF + snapshots)
│   ├── server.ex          # HTTP server orchestration
│   ├── router.ex          # HTTP endpoints (Plug-based)
│   ├── config_manager.ex  # Persistent config storage
│   ├── command_parser.ex  # CLI command routing
│   ├── rate_limiter.ex    # Token bucket rate limiting
│   ├── server_control.ex  # Local/distributed control
│   ├── node_control.ex    # Distributed node management
│   ├── commands/          # CLI command implementations
│   └── replication/       # Leader-follower replication
└── test/                  # 14 test files, 133+ tests
```

## Architecture

### Layered Design

1. **CLI Layer** - Command-line interface via escript
   - `Toska.CLI` - Entry point
   - `Toska.CommandParser` - Command routing
   - `Toska.Commands.*` - Individual command implementations

2. **Server Layer** - OTP GenServer processes
   - `Toska.Server` - HTTP server lifecycle management
   - `Toska.KVStore` - Persistent key-value storage
   - `Toska.Replication.Follower` - Replication client

3. **HTTP API Layer** - Plug-based routing
   - `Toska.Router` - Request routing with middleware chain

4. **Infrastructure**
   - `Toska.ConfigManager` - Persistent JSON configuration
   - `Toska.NodeControl` - Distributed node management

### Key Design Patterns

- **GenServer Pattern** - Standard OTP pattern for stateful processes
- **Plug Middleware Pipeline** - Auth, rate limiting, read-only enforcement
- **Command Behavior** - Extensible CLI command structure
- **Token Bucket** - ETS-backed rate limiting

## Strengths

### Clean Architecture
- Clear separation between CLI, HTTP, and storage layers
- Modular design with well-defined module responsibilities
- Explicit error handling with `{:ok, value}` / `{:error, reason}` tuples

### Robust Persistence
- AOF with per-entry SHA256 checksums
- Snapshot support with checksum verification
- Atomic file writes using temp files and `File.rename/2`
- Automatic compaction when AOF grows beyond threshold

### Good OTP Practices
- Proper GenServer usage throughout
- Supervision tree for fault tolerance
- Graceful shutdown handling

### Test Coverage
- 80% coverage threshold enforced
- 14 test files with 133+ test cases
- Good coverage of edge cases (checksum failures, missing files)

### Minimal Dependencies
- Only essential packages: Jason, Bandit, Plug
- No heavy ORM or framework overhead

### Replication
- Simple leader-follower model with snapshot + AOF polling
- Offset tracking for crash recovery
- Configurable poll intervals

## Problem Areas

### 1. Configuration Validation Gap

**Location**: `lib/toska/config_manager.ex`

Config values are validated when set via the API but not when loaded from disk on startup. A corrupted or manually edited config file could cause unexpected behavior.

**Impact**: Low - Falls back to defaults, but silently.

**Recommendation**: Add validation during config file load with warning logs for invalid values.

### 2. Limited Observability

**Location**: Throughout codebase

Logging is minimal. No structured logging or telemetry integration for production debugging.

**Impact**: Medium - Makes debugging production issues harder.

**Recommendation**:
- Add structured logging with consistent formats
- Consider integrating Telemetry for metrics
- Add request ID tracking for HTTP requests

### 3. HTTP Client Choice for Replication

**Location**: `lib/toska/replication/follower.ex`

Uses Erlang's built-in `:httpc` which lacks connection pooling and modern features.

**Impact**: Low - Works fine but could limit scalability.

**Recommendation**: Consider `Finch` or `Req` for connection pooling if replication load increases.

### 4. Distributed Node Complexity

**Location**: `lib/toska/node_control.ex`

Uses EPMD for remote node control, adding an operational dependency that may not be obvious.

**Impact**: Low - Feature is optional and gracefully degrades.

**Recommendation**: Document EPMD requirement clearly or consider simpler IPC alternatives.

### 5. Missing API Documentation

**Location**: N/A

No OpenAPI/Swagger specification for the REST API.

**Impact**: Low - Makes API consumption harder for external users.

**Recommendation**: Add OpenAPI spec or at minimum document all endpoints with request/response examples.

### 6. Rate Limiter Silent Disable

**Location**: `lib/toska/rate_limiter.ex:33-35`

When `per_sec <= 0` or `burst <= 0`, rate limiting is silently disabled rather than raising a configuration error.

**Impact**: Low - Intentional behavior but could surprise operators.

**Recommendation**: Add documentation or log a warning when rate limiting is disabled via config.

### 7. No End-to-End HTTP Tests

**Location**: `test/`

Router tests use `Plug.Test` for unit-level testing. No integration tests with actual HTTP client.

**Impact**: Low - Good unit coverage exists.

**Recommendation**: Add integration tests using `HTTPoison` or similar to test full request lifecycle.

### 8. ETS Table Ownership

**Location**: `lib/toska/rate_limiter.ex`

The rate limiter ETS table is created at module load time without explicit ownership management.

**Impact**: Low - Works in practice since the table is named.

**Recommendation**: Consider creating the table in a supervised process for clearer lifecycle management.

## Performance Characteristics

From benchmarks in `perf/`:

| Metric | Toska | Redis |
|--------|-------|-------|
| PUT ops/sec | 20,375 | 259,067 |
| GET ops/sec | 28,522 | - |

The ~10x difference vs Redis is expected due to HTTP/JSON overhead vs RESP binary protocol. Performance is linear and predictable, matching the "scale without surprises" goal.

## Summary

ToskaStore is a well-engineered project with strong Elixir/OTP practices. No critical issues were found. The main areas for improvement are:

1. **Observability** - Add structured logging and metrics
2. **Documentation** - Add API specification
3. **Validation** - Validate config on load, not just on set

The codebase successfully achieves its goal of being a clear, reliable KV store with predictable performance.
