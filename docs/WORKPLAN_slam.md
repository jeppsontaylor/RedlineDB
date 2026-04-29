# WORKPLAN_slam

Status snapshot for the SQLite-compatibility, benchmark, and kernel follow-on work.

## Phase 9 Baseline

The phase-8 working tree (33 modified + 22 untracked files) was split into six
subsystem-scoped commits and tagged `phase9-baseline`. Cumulative state passes
all proof lanes; intermediate states may not compile standalone (this is
acceptable for `git bisect` — non-buildable commits are skipped by default).

- `6779250 chore(parser): split parser.rs into ddl/dml/pragma/select/helpers submodules`
- `06f0552 feat(bench): add certify lane scaffold and modular harness`
- `8432ee6 feat(kernel+sql): catalog snapshot WAL, relation-qualified loads, busy timeout`
- `4320364 chore(ffi): add sqlite3.h compat header and wire busy-timeout pass-through`
- `8d54e84 feat(facade): wire busy-timeout, benchmark stats, OS advisory lock through redlinedb`
- `49ba716 chore(agent+docs+scripts): phase 9 proof lanes, xbabe1 scripts, workplan docs`

Post-split proof:
- `cargo fmt --check` — green
- `./scripts/check_file_sizes.sh` — green
- `cargo check --workspace --locked` — green
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — green
- `cargo test --workspace --quiet --locked` — `174 passed (28 suites, 3.62s)`

## Verified Proof

These commands passed in the current workspace:

1. `rtk cargo fmt --check`
2. `./scripts/check_file_sizes.sh`  
   Result: passed, no active source file over the size cap
3. `rtk cargo check --workspace --locked`
4. `rtk cargo clippy --workspace --all-targets --locked -- -D warnings`
5. `rtk cargo test --workspace --quiet --locked`  
   Result: `174 passed (28 suites, 3.69s)`
6. `rtk cargo test -p redlinedb-bench --quiet --locked`  
   Result: `7 passed (4 suites, 0.27s)`
7. `rtk cargo test -p redlinedb --quiet --locked`  
   Result: `8 passed (3 suites, 0.34s)`
8. `rtk cargo test -p redlinedb-ffi --quiet --locked`  
   Result: `5 passed (1 suite, 0.04s)`
9. `rtk cargo test -p redlinedb-sql --quiet --locked`  
   Result: `26 passed (3 suites, 1.46s)`
10. `rtk cargo run -p redlinedb-bench -- compare --config crates/bench/bench/smoke.toml --out target/bench/smoke.jsonl --report target/bench/smoke.md --seed 7`
11. `rtk cargo run -p redlinedb-bench -- compat --engine both --test-dir crates/bench/compat --seed 7`  
    Result: `{"files": 3, "cases": 40, "failures": []}`
12. `rtk cargo run -p redlinedb-bench -- recover-matrix --config crates/bench/bench/recovery-matrix.toml --out target/bench/recovery-matrix.json --seed 7`
13. `rtk cargo check --workspace --locked`
14. `rtk cargo test -p redlinedb-kernel --quiet --locked`  
    Result: `127 passed (15 suites, 1.63s)`
15. `rtk cargo test -p redlinedb-sql --quiet --locked`  
    Result: `26 passed (3 suites, 2.00s)`
16. `rtk cargo run -p redlinedb-bench -- certify --config crates/bench/bench/smoke.toml --out-dir target/bench/certify-smoke --seed 7 --repetitions 1 --warmup 0`
17. `./scripts/check_file_sizes.sh`  
    Result: passed, no active source file over the size cap

### Raw Artifacts

1. `target/bench/smoke.jsonl`  
   SHA-256: `8abf3835fe0f1843a5e59edd9e763c23416299fb73facfcfb7448515388caafd`
2. `target/bench/smoke.md`  
   SHA-256: `5d7557082846743655eb6b91490bd2efeec35f78166624905544e52068df33b4`
3. `target/bench/recovery-matrix.json`  
   SHA-256: `1e3e2975fad274fadb195c8b0ccc28ccf272a05a9bbf2c942ba2c36777f70ef0`
4. `target/bench/certify-smoke/report.json`  
   SHA-256: `6a9a687079f079509ccbb8c642255d620ce03849c1e986b646db0c513070a58a`
5. `target/bench/certify-smoke/manifest.json`  
   SHA-256: `e00a6c7aa7cfe7d7b8d43166345d7b4e23614f659415bb4d8fe61c79c53eb77f`
6. `target/bench/certify-smoke/report.md`  
   SHA-256: `a6e25ac459a6ae1086225ea373741772c542290647116ffd0f6c2a56f2fcb479`
7. `target/bench/certify-smoke/runs.jsonl`  
   SHA-256: `3c29a2d064e9fab171a3e4c49540dd962663ff1369ff322ca71e2aaaf436a36c`
8. `target/bench/certify-smoke/summary.csv`  
   SHA-256: `ab6f4a46da1170d94d6d907741e2a5c723f5a98383cb770b9e9d16ffe4fafb7a`

## Work Completed

1. Busy-timeout propagation is now real across the kernel row-lock manager, SQL unique-lock table, SQL database/connection wrappers, the public Rust facade, and the sqlite-style C API.
2. The benchmark harness now records an environment snapshot per run, including host, git state, rustc version, SQLite version, CPU count, memory, and optional image digest.
3. The benchmark matrix was expanded with secondary-index reads, range reads, hot-row updates, and 95/5, 80/20, and 50/50 mixed workloads.
4. Remote benchmark orchestration was added under `scripts/bench/` together with a pinned Dockerfile for the `xbabe1` execution path.
5. The proof-lane metadata was updated to include the new compat and remote benchmark lanes.
6. The bench recovery harness and the public timeout tests were tightened so the checked-in code matches the proof runs.
7. WAL catalog snapshots are now encoded as logical WAL payloads, replayed during recovery, and used as the durable source for DDL recovery when `schema.redline` is missing.
8. SQL table row loading is now relation-qualified end to end; the executor no longer falls back to the global row-directory scan for table access.
9. Planner output has been made conservative again so it no longer advertises index access paths that the executor does not actually take yet.
10. `crates/sql/src/parser.rs` has been split into smaller parser submodules, and the size warning is gone.
11. `redlinedb-bench` now has a child-process-backed `certify` lane that writes `runs.jsonl`, `summary.csv`, `report.md`, and `manifest.json` under a dedicated artifact tree.

## NEEDS_REVIEW

These are the remaining complex items from the original plan that should be re-read by a stronger reviewer before anyone treats them as hardened claims:

1. `crates/sql/src/parser.rs` is now split into smaller submodules and is back under the file-size cap. It should still be re-reviewed whenever new SQLite syntax is added.
2. `crates/kernel/src/engine/mod.rs` now commits catalog snapshots through WAL and replays them on open, but the sidecar/cache recovery story still needs a deeper crash and fault-injection review.
3. `crates/sql/src/exec.rs`, `crates/sql/src/exec/tail.rs`, and the planner are still scan-heavy in places because physical index execution is not wired through the executor yet.
4. Deterministic failpoints are not yet implemented, so the failpoint matrix remains a review item rather than a closed proof lane.
5. Raw SQLite VFS/fsync/RSS/IO metrics are still not fully captured in the benchmark output. The new certification lane writes reproducible artifacts, but it is still not a complete telemetry system.
6. The benchmark interpretation layer still needs a stronger review before any headline performance claim is made from the new matrices.

## Still Open

The workspace is green, the new benchmark lanes exist, and the timeout behavior now works through the public APIs. The remaining open work is the deeper certification and engine-hardening scope from the original plan:

1. Large-machine 128-thread certification reruns.
2. Deterministic crash/failpoint certification.
3. Full physical-index execution wiring through SQL DML and access paths.
4. Catalog/DDL crash-atomicity tightening.

Those items are not represented here as finished facts; they are the next layer after the verified smoke lane and recovery matrix.
