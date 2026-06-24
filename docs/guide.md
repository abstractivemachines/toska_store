# ToskaStore

This is the full guide. For a quick install/start, see `README.md`.

ToskaStore is a disk-backed string KV store with a clean HTTP/JSON surface and a minimal CLI. It is built in Elixir, designed for clarity, and intended to scale without surprises.

## Table of Contents

- [Installation & Setup](#installation--setup)
- [Dependencies](#dependencies)
- [Building](#building)
- [Testing](#testing)
- [Server Commands](#server-commands)
- [HTTP Endpoints](#http-endpoints)
- [Configuration Management](#configuration-management)
- [Development](#development)
- [Project Structure](#project-structure)

## Installation & Setup

### Prerequisites

- Elixir 1.18 or higher
- Erlang/OTP compatible version

### Getting Started

> **Important**: ToskaStore is an **umbrella project**. The CLI executable is built in the `apps/toska/` directory, not the root directory.

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd toska_store
   ```

2. **Install dependencies**
   ```bash
   mix deps.get
   ```

3. **Build the application**
   ```bash
   mix compile
   ```

4. **Build the CLI executable**

   Since this is an umbrella project, you have two options:

   **Option A: Build from the app directory (Recommended)**
   ```bash
   cd apps/toska
   mix escript.build
   # Creates: apps/toska/toska
   ```

   **Option B: Build from root with target app**
   ```bash
   # From the root directory
   MIX_TARGET_APP=toska mix escript.build
   # Creates: apps/toska/toska
   ```

5. **Create a convenient symlink (Optional)**
   ```bash
   # From the root directory, create a symlink for easier access
   ln -s apps/toska/toska ./toska
   ```

After building, you'll have a `toska` executable in the `apps/toska/` directory that provides the complete CLI interface.

## Dependencies

### Managing Dependencies

```bash
# Get all dependencies
mix deps.get

# Update dependencies
mix deps.update

# Update specific dependency
mix deps.update jason

# Clean dependencies
mix deps.clean

# Clean and reinstall all dependencies
mix deps.clean --all && mix deps.get

# Show dependency tree
mix deps.tree
```

### Main Dependencies

- **[Jason](https://hex.pm/packages/jason)** `~> 1.4` - JSON encoding/decoding
- **[Bandit](https://hex.pm/packages/bandit)** `~> 1.0` - HTTP server
- **[Plug](https://hex.pm/packages/plug)** `~> 1.15` - HTTP middleware and utilities

## Building

### Development Build

```bash
# Compile the application
mix compile

# Build with warnings as errors
mix compile --warnings-as-errors

# Force recompilation
mix compile --force
```

### Production Build

```bash
# Build for production
MIX_ENV=prod mix compile

# Build escript for production
MIX_ENV=prod mix escript.build
```

## Testing

```bash
# Run all tests
mix test

# Run tests with detailed output
mix test --trace

# Run specific test file
mix test test/toska_test.exs

# Run tests with coverage
mix test --cover

# Run tests in watch mode (requires mix_test_watch)
mix test.watch
```

## Server Commands

### Start Server

```bash
# Start with default settings (localhost:4000)
./apps/toska/toska start

# Start with custom port
./apps/toska/toska start --port 8080

# Start with custom host and port
./apps/toska/toska start --host 0.0.0.0 --port 8080

# Start in daemon mode
./apps/toska/toska start --daemon
```

### Stop Server

```bash
# Stop gracefully
./apps/toska/toska stop

# Force stop
./apps/toska/toska stop --force
```

### Server Status

```bash
# Check server status
./apps/toska/toska status

# Check status in JSON format
./apps/toska/toska status --json
```

## HTTP Endpoints

When the server is running, the HTTP API provides a simple JSON key/value store:

- `GET /` - Welcome page with server status
- `GET /status` - JSON status
- `GET /health` - Health check
- `GET /kv/keys` - List keys in lexicographic order (`?prefix=todo:` optional, `?limit=100` optional, max `1000`, `?cursor=...` for the next page)
- `GET /kv/:key` - Fetch a value by key
- `PUT /kv/:key` - Set a value with optional `ttl_ms` (`{"value": "...", "ttl_ms": 5000}`)
- `DELETE /kv/:key` - Remove a key
- `POST /kv/mget` - Fetch multiple keys (`{"keys": ["a", "b"]}`)
- `GET /stats` - Store metrics and persistence info
- `GET /replication/info` - Snapshot + AOF metadata for followers
- `GET /replication/snapshot` - JSON snapshot file
- `GET /replication/aof?since=0&max_bytes=65536` - AOF stream from a byte offset
- `GET /replication/status` - Follower status

Follower mode is enabled by setting `replica_url` (or `TOSKA_REPLICA_URL`) and starting the server.
When follower mode is enabled, KV write endpoints (`PUT`/`DELETE`) return `403` to enforce read-only access.

KV endpoints (`/kv/*` and `/stats`) can require an auth token and apply rate limits:
- `auth_token` (or `TOSKA_AUTH_TOKEN`) expects `Authorization: Bearer <token>` or `X-Toska-Token`.
- `rate_limit_per_sec` + `rate_limit_burst` (or `TOSKA_RATE_LIMIT_PER_SEC`, `TOSKA_RATE_LIMIT_BURST`).

### Key Listing

`GET /kv/keys` returns keys in lexicographic order and supports cursor-based pagination.

```bash
curl -s "http://localhost:4000/kv/keys?prefix=todo:&limit=100"
```

Response:

```json
{
  "keys": ["todo:1", "todo:2"],
  "next_cursor": null
}
```

When `next_cursor` is present, pass it back as `cursor` with the same `prefix` to fetch the next page. `limit` defaults to `100`, may be `0`, and cannot exceed `1000`. Invalid limits return `400`.

## Configuration Management

ToskaStore provides configuration management through the CLI.
Set `TOSKA_CONFIG_DIR` to override the configuration directory used for `toska_config.json`.

### View Configuration

```bash
# List all configuration
./apps/toska/toska config list

# List configuration in JSON format
./apps/toska/toska config list --json

# Get specific configuration value
./apps/toska/toska config get port
./apps/toska/toska config get host
./apps/toska/toska config get env
./apps/toska/toska config get log_level
```

### Update Configuration

```bash
# Set server port
./apps/toska/toska config set port 8080

# Set server host
./apps/toska/toska config set host "0.0.0.0"

# Set environment
./apps/toska/toska config set env prod

# Set log level
./apps/toska/toska config set log_level info

# Set data directory
./apps/toska/toska config set data_dir "/var/lib/toska"

# Set snapshot interval
./apps/toska/toska config set snapshot_interval_ms 60000
```

### Reset Configuration

```bash
# Reset specific configuration key
./apps/toska/toska config reset port

# Reset all configuration to defaults
./apps/toska/toska config reset

# Reset with confirmation skip
./apps/toska/toska config reset --confirm
./apps/toska/toska config reset -y
```

### Available Configuration Keys

- **port** - Server port (integer, default: 4000)
- **host** - Server host (string, default: "localhost")
- **env** - Environment (dev|test|prod, default: "dev")
- **log_level** - Log level (debug|info|warn|error, default: "info")
- **data_dir** - Data directory for AOF/snapshots (default: `~/.toska/data`)
- **aof_file** - AOF filename (default: `toska.aof`)
- **snapshot_file** - Snapshot filename (default: `toska_snapshot.json`)
- **sync_mode** - AOF sync mode (always|interval|none, default: interval)
- **sync_interval_ms** - AOF sync interval (default: 1000)
- **snapshot_interval_ms** - Snapshot interval (default: 60000)
- **ttl_check_interval_ms** - TTL cleanup interval (default: 1000)
- **compaction_interval_ms** - AOF compaction interval (default: 300000)
- **compaction_aof_bytes** - AOF size threshold for compaction (default: 10485760)
- **replica_url** - Leader URL for follower replication (default: empty)
- **replica_poll_interval_ms** - Follower poll interval (default: 1000)
- **replica_http_timeout_ms** - Follower HTTP timeout (default: 5000)
- **auth_token** - Bearer token for KV endpoints (default: empty)
- **rate_limit_per_sec** - Requests per second limit (default: 0, disabled)
- **rate_limit_burst** - Burst capacity for rate limiting (default: 0, disabled)

Snapshots include a checksum and version field. AOF records include per-line checksums for integrity.

Runtime control metadata is stored in `~/.toska/toska_runtime.json`.

## Development

### Development Workflow

```bash
# Start development environment
mix deps.get
mix compile

# Build the CLI executable
cd apps/toska && mix escript.build

# Run tests during development
mix test --stale

# Start interactive shell
iex -S mix

# Format code
mix format

# Check for unused dependencies
mix deps.unlock --unused

# Generate documentation
mix docs
```

### Environment Variables

The application respects the following environment variables:

- `MIX_ENV` - Set the Mix environment (dev, test, prod)
- `PORT` - Override default port (when used programmatically)
- `TOSKA_CONFIG_DIR` - Override config directory for `toska_config.json`
- `TOSKA_DATA_DIR` - Override data directory for AOF/snapshot files
- `TOSKA_REPLICA_URL` - Leader URL for replication follower
- `TOSKA_REPLICA_POLL_MS` - Override follower poll interval
- `TOSKA_REPLICA_HTTP_TIMEOUT_MS` - Override follower HTTP timeout
- `TOSKA_AUTH_TOKEN` - Require auth token for KV endpoints
- `TOSKA_RATE_LIMIT_PER_SEC` - Requests per second limit
- `TOSKA_RATE_LIMIT_BURST` - Burst capacity for rate limiting

### Benchmarking

A basic benchmark script is available in `scripts/bench_kv.exs`:

```bash
TOSKA_BENCH_URL=http://localhost:4000 \
TOSKA_BENCH_OPS=10000 \
TOSKA_BENCH_CONCURRENCY=20 \
TOSKA_BENCH_MODE=mixed \
mix run scripts/bench_kv.exs
```

### Code Organization

- **CLI Layer**: Command parsing and user interface (`lib/toska/cli.ex`, `lib/toska/commands/`)
- **Server Layer**: HTTP server management (`lib/toska/server.ex`)
- **HTTP Layer**: Request routing and handling (`lib/toska/router.ex`)
- **Configuration**: Configuration management (`lib/toska/config_manager.ex`)

## Project Structure

ToskaStore is organized as an Elixir umbrella application:

```
toska_store/
├── mix.exs                    # Umbrella project configuration
├── mix.lock                   # Dependency lock file
├── README.md                  # Quick start
├── docs/                      # Full docs and notes
├── apps/                       # Umbrella applications
│   └── toska/                  # Main application
│       ├── lib/                # Application code
│       ├── test/               # Tests
│       ├── mix.exs             # App-specific config
│       └── toska               # CLI executable (after build)
└── scripts/                    # Helper scripts
```
