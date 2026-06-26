# Toska

A command-line interface for the ToskaStore server, built with Elixir and designed for extensibility.

## Overview

Toska provides a comprehensive command-line interface for managing the Toska server process. It includes commands for starting/stopping the server, checking status, and managing configuration.

## Architecture

The CLI is built with a modular, extensible architecture:

- **Command Parser** (`Toska.CommandParser`) - Routes commands to appropriate handlers
- **Command Behavior** (`Toska.Commands.Command`) - Defines common behavior for all commands
- **Individual Commands** (`Toska.Commands.*`) - Specific command implementations
- **Server Management** (`Toska.Server`) - GenServer for managing the server process
- **Configuration Management** (`Toska.ConfigManager`) - Persistent configuration storage

## Installation

From the umbrella project root:

```bash
# Install dependencies
mix deps.get

# Compile the application
mix compile

# Build the escript (optional)
cd apps/toska && mix escript.build
```

## Usage

### Via Mix (from umbrella root)

```bash
# Start the server
mix run -e "Toska.run([\"start\", \"--port\", \"8080\"])"

# Check status
mix run -e "Toska.run([\"status\"])"

# Show help
mix run -e "Toska.run([\"--help\"])"
```

### Via Escript (from apps/toska)

```bash
# Build the escript first
mix escript.build

# Run commands
./toska start --port 8080
./toska status
./toska --help
```

### Programmatically

```elixir
# In an Elixir session or module
Toska.run(["start", "--port", "8080"])
Toska.run(["status"])
```

## Commands

### Global Options

- `-h, --help` - Show help information

### start

Start the Toska server with various options.

```bash
toska start [options]

Options:
  -p, --port PORT     Port to bind the server (default: config port or 4000)
  --host HOST         Host to bind the server (default: config host or localhost)
  --env ENV           Environment to run in (default: config env or dev)
  -d, --daemon        Run as background daemon process
  -h, --help          Show this help

Examples:
  toska start
  toska start --port 8080
  toska start --host 0.0.0.0 --port 3000
  toska start --daemon

Daemon logs are written to `~/.toska/toska_daemon.log`.
```

### stop

Stop the Toska server gracefully or forcefully.

```bash
toska stop [options]

Options:
  -f, --force     Force stop the server
  -h, --help      Show this help

Examples:
  toska stop
  toska stop --force
```

### status

Display current server status and system information.

```bash
toska status [options]

Options:
  -v, --verbose   Show detailed status information
  -j, --json      Output status in JSON format
  -h, --help      Show this help

Examples:
  toska status
  toska status --verbose
  toska status --json
```

### config

Manage server configuration with subcommands.

```bash
toska config <subcommand> [options]

Subcommands:
  get <key>           Get configuration value
  set <key> <value>   Set configuration value
  list                List all configuration
  reset [key]         Reset configuration to defaults

Examples:
  toska config get port
  toska config set port 8080
  toska config set host "0.0.0.0"
  toska config list
  toska config reset port
  toska config reset  # Reset all to defaults
```

### replicate

Start a replication follower for a leader URL.

```bash
toska replicate start --leader http://localhost:4000
toska replicate --leader http://localhost:4000 --poll 2000 --timeout 5000
toska replicate --leader http://localhost:4000 --daemon
```

## HTTP API

When the server is running, the HTTP API provides a simple JSON key/value store:

- `GET /kv/:key` - Fetch a value by key
- `GET /kv` - Range scan keys with cursor pagination (`?prefix=todo:&start=todo:1&limit=100`)
- `GET /kv/keys` - List keys in lexicographic order (`?prefix=todo:` optional, `?limit=100` optional, max `1000`, `?cursor=...` for the next page)
- `PUT /kv/:key` - Set a value with optional `ttl_ms` (`{"value": "...", "ttl_ms": 5000}`)
- `DELETE /kv/:key` - Remove a key
- `POST /kv/mget` - Fetch multiple keys (`{"keys": ["a", "b"]}`)
- `POST /kv/txn` - Atomically compare keys and apply a success or failure operation list
- `GET /kv/watch?prefix=&since_revision=0` - Server-Sent Events stream for key changes
- `POST /leases` - Create a lease (`{"ttl_ms": 30000, "id": "optional-id"}`)
- `POST /leases/:id/keepalive` - Renew a lease to `now + ttl_ms`
- `DELETE /leases/:id` - Revoke a lease and remove attached keys/locks
- `POST /locks/:name/acquire` - Acquire a named lock with an active `lease_id`
- `POST /locks/:name/release` - Release a named lock with its owning `lease_id`
- `GET /stats` - Store metrics and persistence info
- `GET /replication/info` - Snapshot + AOF metadata for followers
- `GET /replication/snapshot` - JSON snapshot file
- `GET /replication/aof?since=0&max_bytes=65536` - AOF stream from a byte offset
- `GET /replication/status` - Follower status

The maintained HTTP API contract lives at `../../openapi.yaml`, and the initial generated client plan is documented in `../../docs/client-sdks.md`.

Follower mode is enabled by setting `replica_url` (or `TOSKA_REPLICA_URL`) and starting the server.
When follower mode is enabled, KV, lease, and lock write endpoints return `403` to enforce read-only access.

KV, lease, lock, stats, metrics, admin, and replication endpoints can require scoped auth tokens and apply rate limits:
- `read_auth_token` (or `TOSKA_READ_AUTH_TOKEN`) protects KV read, watch, range, mget, stats, and metrics endpoints.
- `write_auth_token` (or `TOSKA_WRITE_AUTH_TOKEN`) protects KV write, transaction, lease, and lock endpoints.
- `admin_auth_token` (or `TOSKA_ADMIN_AUTH_TOKEN`) protects admin endpoints.
- `replication_auth_token` (or `TOSKA_REPLICATION_AUTH_TOKEN`) protects replication endpoints.
- `named_auth_tokens` (or `TOSKA_NAMED_AUTH_TOKENS`) accepts a JSON array of named tokens with `name`, `token`, and `scopes` fields. Valid scopes are `read`, `write`, `admin`, and `replication`; names may use letters, numbers, `.`, `_`, `:`, `@`, and `-`.
- Scoped tokens fall back to `auth_token` (or `TOSKA_AUTH_TOKEN`) when unset.
- Protected endpoints expect `Authorization: Bearer <token>` or `X-Toska-Token`.
- `rate_limit_per_sec` + `rate_limit_burst` (or `TOSKA_RATE_LIMIT_PER_SEC`, `TOSKA_RATE_LIMIT_BURST`).

Write and admin requests emit `toska_audit` log entries with the matched token name when a named token is used.

`GET /kv/:key` returns `value` plus metadata (`version`, `created_at`, `updated_at`, `expires_at`, `lease_id`) and sets an `ETag` matching the current version.
`PUT /kv/:key` accepts optional `if_version`, `if_absent`, and `if_present` fields. `DELETE /kv/:key` accepts `if_version` as a query parameter. `PUT` and `DELETE` also accept `If-Match: "<version>"`; failed conditions return `412`.

`POST /kv/txn` accepts `compare`, `success`, and `failure` arrays. Compares support `version`, `exists`, and `value`; operations support `put`, `delete`, and `get`. The selected operation list runs atomically inside the KV store.

`GET /kv/watch` streams `put`, `delete`, and `expire` events as Server-Sent Events. Use `prefix` to filter keys and `since_revision` to replay retained history after a known store revision. Watch replay is available from the current AOF/revision window and in-memory history; requests older than retained history return `409`.

```bash
curl -N "http://localhost:4000/kv/watch?prefix=jobs:&since_revision=0"
```

`POST /leases` creates a durable TTL ownership record. Writing a key with `lease_id` attaches the key to that lease; keepalive extends the lease and attached key expirations. Revoking or expiring the lease removes attached keys and releases attached locks.

```bash
curl -s -X POST http://localhost:4000/leases \
  -H 'content-type: application/json' \
  -d '{"id":"worker-1","ttl_ms":30000}'

curl -s -X PUT http://localhost:4000/kv/jobs/active/worker-1 \
  -H 'content-type: application/json' \
  -d '{"value":"running","lease_id":"worker-1"}'
```

`POST /locks/:name/acquire` acquires a named lock with an active lease. A held lock returns `409`; release requires the owning `lease_id`.

`GET /kv` returns `{"items": [...], "next_cursor": "..."}` from the ordered key index. Use `prefix` to filter, `start` as an inclusive lower-bound key, and `cursor` to continue after a page. Add `include_values=true` and `include_metadata=true` when range items should include values or metadata.

`GET /kv/keys` returns `{"keys": [...], "next_cursor": "..."}`. When `next_cursor` is present, pass it back as `cursor` with the same `prefix` to fetch the next page. `limit` defaults to `100`, may be `0`, and cannot exceed `1000`.

## Configuration

Configuration is stored in `~/.toska/toska_config.json` and includes defaults used by `start`.
Set `TOSKA_CONFIG_DIR` to override the configuration directory.
Set `TOSKA_DATA_DIR` to override the data directory for AOF/snapshot files.

- **port** (integer): Server port (default: 4000)
- **host** (string): Server host (default: "localhost")
- **env** (string): Environment - dev|test|prod (default: "dev")
- **log_level** (string): Log level - debug|info|warn|error (default: "info")
- **data_dir** (string): Data directory for AOF/snapshots (default: `~/.toska/data`)
- **aof_file** (string): AOF filename (default: `toska.aof`)
- **snapshot_file** (string): Snapshot filename (default: `toska_snapshot.json`)
- **sync_mode** (string): AOF sync mode (always|interval|none, default: interval)
- **sync_interval_ms** (integer): AOF sync interval (default: 1000)
- **snapshot_interval_ms** (integer): Snapshot interval (default: 60000)
- **ttl_check_interval_ms** (integer): TTL cleanup interval (default: 1000)
- **compaction_interval_ms** (integer): AOF compaction interval (default: 300000)
- **compaction_aof_bytes** (integer): AOF size threshold for compaction (default: 10485760)
- **watch_history_limit** (integer): Maximum in-memory watch events retained for replay (default: 10000)
- **replica_url** (string): Leader URL for follower replication (default: empty)
- **replica_poll_interval_ms** (integer): Follower poll interval (default: 1000)
- **replica_http_timeout_ms** (integer): Follower HTTP timeout (default: 5000)
- **auth_token** (string): Legacy Bearer token fallback for protected API endpoints (default: empty)
- **read_auth_token** (string): Bearer token for read endpoints, falling back to `auth_token` when empty (default: empty)
- **write_auth_token** (string): Bearer token for write endpoints, falling back to `auth_token` when empty (default: empty)
- **admin_auth_token** (string): Bearer token for admin endpoints, falling back to `auth_token` when empty (default: empty)
- **replication_auth_token** (string): Bearer token for replication endpoints, falling back to `auth_token` when empty (default: empty)
- **named_auth_tokens** (array): Named token objects with `name`, `token`, and `scopes` fields for audit attribution. Names may use letters, numbers, `.`, `_`, `:`, `@`, and `-` (default: empty)
- **rate_limit_per_sec** (integer): Requests per second limit (default: 0, disabled)
- **rate_limit_burst** (integer): Burst capacity for rate limiting (default: 0, disabled)

Runtime control metadata (node/cookie) is stored in `~/.toska/toska_runtime.json`.
Daemon logs are written to `~/.toska/toska_daemon.log`.
Snapshots include checksums; AOF records include per-line checksums.
Follower offsets persist to `replica.offset` in the data directory.

## Development

### Adding New Commands

1. Create a new module in `lib/toska/commands/` implementing the `Toska.Commands.Command` behavior
2. Add the command route in `Toska.CommandParser.parse/1`
3. Update help text and documentation

Example command structure:

```elixir
defmodule Toska.Commands.MyCommand do
  @behaviour Toska.Commands.Command
  
  alias Toska.Commands.Command

  @impl true
  def execute(args) do
    # Parse options and execute command logic
    :ok
  end

  @impl true
  def show_help do
    IO.puts("Help text for my command")
    :ok
  end
end
```

### Running Tests

```bash
cd apps/toska
mix test
```

### Code Quality

```bash
# Format code
mix format

# Check for issues
mix credo

# Type checking (if dialyzer is added)
mix dialyzer
```

## Server Process Foundation

The CLI includes a GenServer (`Toska.Server`) that provides the foundation for the actual server process. This includes:

- Proper OTP supervision tree
- Configuration management
- Status monitoring
- Graceful shutdown handling

The server can be extended to include actual business logic, HTTP endpoints, database connections, etc.

## Extensibility

The CLI is designed for easy extension:

- **Commands**: Add new commands by implementing the Command behavior
- **Options**: Use OptionParser for consistent option handling
- **Configuration**: Extend the ConfigManager for new config keys
- **Server**: Extend the Server GenServer for additional functionality

## Future Enhancements

- Integration with external monitoring systems
- Plugin system for third-party commands
- Enhanced logging and metrics
- Clustering support
- Database integration
- HTTP API endpoints
