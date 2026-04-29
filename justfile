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
  rtk cargo run -p redlinedb-bench -- certify --config crates/bench/bench/smoke.toml --out-dir target/bench/certify-smoke --seed 7 --repetitions 1 --warmup 0
  rtk cargo run -p redlinedb-bench -- compat --engine both --test-dir crates/bench/compat --seed 7

phase9-certify:
  rtk cargo run -p redlinedb-bench -- certify --config crates/bench/bench/certification.toml --out-dir target/bench/certify-certification --seed 7 --repetitions 5 --warmup 1

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
