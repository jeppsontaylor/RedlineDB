# RedlineDB

RedlineDB is an embedded database workspace with:

- a safe Rust facade in `redlinedb`
- the SQL engine in `redlinedb-sql`
- the storage/kernel layer in `redlinedb-kernel`
- a C ABI in `redlinedb-ffi`
- a CLI in `redlinedb-cli`
- an optional local server in `redlinedb-server`

This repository is currently in the Phase 7 productization stage. The public surface is intentionally SQLite-like in shape, but RedlineDB is not claiming SQLite file-format compatibility.

## Status

The workspace builds and tests as a unit. The current code includes:

- multi-connection embedded use through the Rust API
- stable error and value types
- a C ABI with `rldb_*` entry points
- a command-line shell for one-shot queries, stats, and backups
- a framed local server protocol
- typed query-spec execution for supported CRUD operations

## Quick start

```bash
rtk cargo test --workspace
rtk cargo run -p redlinedb-cli -- --help
rtk cargo run -p redlinedb-server -- --help
```

## Repository layout

- `crates/kernel` - storage engine and catalog
- `crates/sql` - SQL parser, planner, and execution layer
- `crates/redlinedb` - public Rust facade
- `crates/ffi` - native C ABI
- `crates/cli` - command-line interface
- `crates/server` - local framed server
- `crates/bench` - benchmark harness
- `tips/` - project notes and phase artifacts

## Contributing

Bug reports and patches are welcome. Keep changes focused, add tests when behavior changes, and run the workspace checks before opening a PR.

## License

RedlineDB is licensed under Apache-2.0. See [LICENSE](LICENSE).
