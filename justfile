set shell := ["bash", "-euo", "pipefail", "-c"]

default: fast

fast:
  rtk cargo fmt --check
  ./scripts/check_file_sizes.sh
  rtk cargo check --workspace --locked
  rtk cargo test --workspace --quiet --locked

hygiene:
  rtk cargo fmt --check
  ./scripts/check_file_sizes.sh

clippy:
  rtk cargo clippy --workspace --all-targets --locked -- -D warnings

medium:
  rtk cargo test --workspace --quiet --locked
  rtk cargo run -p redlinedb-cli -- --help
  rtk cargo run -p redlinedb-server -- --help

phase8-smoke:
  rtk cargo test --workspace --quiet --locked
  rtk cargo run -p redlinedb-cli -- --help
  rtk cargo run -p redlinedb-server -- --help

phase9-smoke:
  rtk cargo test -p redlinedb-bench --quiet --locked
  rtk cargo run -p redlinedb-bench -- compare --config crates/bench/bench/smoke.toml --out target/bench/smoke.jsonl --report target/bench/smoke.md --seed 7
  rtk cargo run -p redlinedb-bench -- compat --engine both --test-dir crates/bench/compat/slt --seed 7

security:
  rtk cargo audit
  rtk cargo deny check
  rtk gitleaks detect --source .

security-local:
  rtk cargo audit
  rtk cargo deny check
  rtk gitleaks detect --source .

release:
  rtk cargo build --workspace --release --locked
