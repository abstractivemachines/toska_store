# Client SDK Plan

`openapi.yaml` is the source of truth for ToskaStore's HTTP API. The first SDK
goal is to make common KV, transaction, watch, lease, lock, and range workflows
easy without hiding the HTTP model.

## Initial Targets

- TypeScript for Node.js and browser-compatible runtimes.
- Python for scripts, agents, and internal services.
- Go for small infrastructure services.
- Elixir for BEAM services that want a client instead of direct process calls.

## Generation Strategy

Start with generated clients from `openapi.yaml`, then add thin ergonomic
wrappers where generated code is awkward:

- `get`, `put`, `delete`, `mget`, `range`, and `keys` helpers.
- `txn` builder helpers for compares and operations.
- SSE watch helpers that expose decoded `WatchEvent` values.
- Lease and lock helpers that preserve server response metadata.
- Auth configuration for `Authorization: Bearer <token>` and `X-Toska-Token`.
- Typed error objects that preserve status code, response body, and retry hints.

Generated clients should keep all request/response schema names aligned with
the OpenAPI components so examples, tests, and docs can cross-reference the same
terms.

## Packaging

Proposed package names:

- TypeScript: `@toskastore/client`
- Python: `toskastore`
- Go: `github.com/abstractivemachines/toska-store-go`
- Elixir: `toska_client`

Each package should include a compatibility table that names the ToskaStore API
version and the OpenAPI contract commit it was generated from.

## Behavioral Requirements

- Never retry non-idempotent writes automatically unless the caller opts in.
- Surface `412` condition failures, `409` lock conflicts, `403` read-only
  follower errors, and `429` rate limits as distinct typed errors.
- Preserve `ETag` values on key reads and writes for conditional updates.
- Support configurable base URL, timeouts, headers, and token auth.
- Keep watch clients cancellable and reconnectable from the last received
  revision when the caller opts in.

## Release Phases

1. Add generated TypeScript and Python clients with smoke tests against the
   existing router tests or a local test server.
2. Add Go and Elixir clients once the generated OpenAPI surface settles.
3. Add cookbook examples for config storage, idempotency keys, leader election
   locks, and watch-driven reloads.
4. Add SDK conformance tests that run the same HTTP workflow matrix against all
   published clients.

## Acceptance Criteria

- `openapi.yaml` validates in CI and every operation has a unique
  `operationId`.
- Every SDK can run create, read, update, delete, range, transaction, watch,
  lease, and lock examples against a local ToskaStore server.
- Error behavior is documented and covered by tests for condition failures,
  lock conflicts, read-only followers, auth failures, and rate limits.
