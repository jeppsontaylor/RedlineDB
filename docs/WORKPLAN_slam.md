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

## Phase 9 Wave 1 Fusion (G + D + F)

Three lanes landed on top of `phase9-baseline` (1d0561c) and tagged `wave1-fused`:

- Lane G — Docker / proof-lane integration (3 commits): `1c934d1`, `321e89f`, `7f10bb9`. Added `strace` to Dockerfile, replaced `compare` with `certify` across `agent/proof-lanes.toml`, `agent/test-map.json`, `justfile`; added `phase9-failpoint-matrix` placeholder lane; pointed compat lanes at `crates/bench/compat` (recursive); `xbabe1_run.sh` exports `REDLINEDB_BENCH_IMAGE_DIGEST`.
- Lane D — Failpoint infrastructure (1 commit): `2e104c6`. Added `fail` crate as optional dep gated on the new `failpoints` feature in `crates/kernel/Cargo.toml`; `crates/kernel/src/failpoints/{mod,macros}.rs` provide `fail_point!` (no-op when feature off); smoke test `crates/kernel/tests/failpoint_smoke.rs`.
- Lane F — Bench telemetry (3 commits): `0a3879d`, `50fec7c`, `af7d490`. Added `crates/bench/src/{process_metrics,strace_capture}.rs`; extended `RunRecord.process_metrics`; SQLite PRAGMA snapshot + validation; extended `CertificationManifest` with `pragmas`, `pragma_validation`, `checksums`, `strace_reason`, `strace_syscall_counts`, `process_metrics_per_run`.

Post-fusion proof:
- `cargo fmt --check` — green
- `./scripts/check_file_sizes.sh` — green
- `cargo check --workspace --locked` — green
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — green
- `cargo test --workspace --quiet --locked` — `178 passed (29 suites, 3.78s)` (174 baseline + 4 new bench tests)
- `cargo run -p redlinedb-bench -- certify --config crates/bench/bench/smoke.toml --out-dir target/bench/wave1-certify --seed 7 --repetitions 1 --warmup 0` — exit 0

Wave 1 artifact SHA-256 (target/bench/wave1-certify/):
- `manifest.json` — `f125341e8d3392e45cba745becc451e05075ee99375304b4ed299a6bbae390c2`
- `runs.jsonl` — `37ef9fac7fbdcb609509ef006d4fe232c99faaa5f6f8124f57ed953c58432ae2`
- `summary.csv` — `facef8706bf05ba469819e55814761a0d177aec969ab4b0f0ebdf0251d970081`
- `report.md` — `c50aef81e4315047728b5801e460bde64dacc7b03bc58c159bbba15bb0cea24a`

## Phase 9 Wave 2 Fusion (A + H combined)

Lane A+H landed on top of `wave1-fused` (4d48dd6) and tagged `wave2-fused`. Four commits, fast-forwarded into main:

- `9bf5c3a phase:9/lane-a/catalog-set-meta: add apply_set_index_meta_page_id helper`
- `754e3dc phase:9/lane-h/lsn-sentinels: distinguish mutation from legit-init Lsn use`
- `24e43f0 phase:9/lane-a/btree-create: allocate physical pages for CREATE INDEX`
- `47b8526 phase:9/lane-a/tests: end-to-end create_index, recover, and atomicity`

Engine `create_index()` now allocates a `BtreeIndex` via `BtreeIndex::create_with_wal()` and persists `IndexDef.meta_page_id` through the existing `WalPayload::CatalogSnapshot` path (no new `CatalogDelta`). DDL backfill scans the heap and inserts via `BtreeIndex::insert_tx`. `Engine::open()` rehydrates index handles from catalog. New accessor `Engine::index_handle()` ready for SQL exec layer.

Lane H flipped 12 mutation-sentinel `Lsn::ZERO` → `Lsn(1)` in `crates/kernel/src/index/mod.rs` (create-meta, create-root, leaf insert, delete-mark, leaf compact, leaf-split L/R, internal split absorbed/rewrite/right, root promotion, set_meta_root). Legitimate-init sites in `engine/page_heap.rs` recovery replay paths confirmed and audit-commented. Engine-side mutation calls (insert/update/delete) already used `Lsn(1)`; an audit comment was added at the first call.

Post-fusion proof:
- `cargo fmt --check` — green
- `./scripts/check_file_sizes.sh` — green (largest active file `index/mod.rs` at 1441 LOC)
- `cargo check --workspace --locked` — green
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — green
- `cargo test --workspace --quiet --locked` — `181 passed (29 suites, 3.85s)` (178 → 181, +3 new kernel tests)
- `cargo test -p redlinedb-kernel --quiet --locked` — 130 passed (was 127)
- `cargo run -p redlinedb-bench -- recover-matrix --config crates/bench/bench/recovery-matrix.toml --out target/bench/wave2-recovery.json --seed 7` — exit 0, 24/36 cases passed (same as pre-Wave-2; the 12 pre-existing failures are Lane E failpoint-matrix work, not regressed)

Wave 2 artifact SHA-256:
- `target/bench/wave2-recovery.json` — `58568ff50625e2e57508ba0584263924162cc39fe59f0f8db8604d3a70fb96a8`

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
