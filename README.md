# ToskaStore

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**Maintained by [@nullsync](https://github.com/nullsync) at [Abstractive Machines LLC](https://github.com/abstractivemachines)**

ToskaStore is a disk-backed string KV store with a clean HTTP/JSON surface and a minimal CLI. It is built in Elixir, designed for clarity, and intended to scale without surprises.

## Quick Start

```bash
mix deps.get
mix compile
cd apps/toska
mix escript.build
./toska start
```

Smoke test:

```bash
curl -s -X PUT http://localhost:4000/kv/hello \
  -H 'content-type: application/json' \
  -d '{"value":"world"}'

curl -s http://localhost:4000/kv/hello
```

## Docs

- [docs/guide.md](docs/guide.md) - full install/build/config/API/CLI/development guide
- [openapi.yaml](openapi.yaml) - maintained HTTP API contract
- [docs/client-sdks.md](docs/client-sdks.md) - initial client SDK generation plan
- [docs/kv-store-changes.md](docs/kv-store-changes.md) - KV store change log and milestones

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## Security

See [SECURITY.md](SECURITY.md) for security policy and vulnerability reporting.

## License

Licensed under the Apache License 2.0. See `LICENSE` and `NOTICE` for details.

---

Tip: `./toska status` and `/health` are the fastest sanity checks.
