#![cfg(feature = "failpoints")]
//! Smoke tests for the Lane D failpoint scaffolding.
//!
//! These are gated on the `failpoints` feature so they are skipped under
//! default-feature workspace runs. They verify that:
//!
//! 1. `cfg("name", "panic")` actually triggers a panic at the named site,
//!    proving registry wiring through the `fail` crate.
//! 2. `fail_point!("name")` is a no-op when the failpoint is unset, proving
//!    the macro expansion stays inert in the absence of configuration.

use redlinedb_kernel::{fail_point, failpoints};

#[test]
fn cfg_panics_when_set() {
    // Each test gets its own scenario so configurations do not leak across
    // threads. We hold the scenario for the duration of the test and let the
    // unwind path drop it after we observe the panic.
    let scenario = fail::FailScenario::setup();
    failpoints::cfg("kernel::lane_d::panic_probe", "panic").expect("configure panic action");

    let result = std::panic::catch_unwind(|| {
        fail_point!("kernel::lane_d::panic_probe");
    });

    // Reset before asserting so a failed assertion does not leave a panic
    // action armed for any subsequent test on this scenario.
    failpoints::cfg("kernel::lane_d::panic_probe", "off").expect("disable panic action");
    drop(scenario);

    assert!(
        result.is_err(),
        "fail_point! with panic action must propagate a panic"
    );
}

#[test]
fn fail_point_macro_no_op_when_unset() {
    let scenario = fail::FailScenario::setup();

    // No `cfg` call has been made for this name, so the expansion must be a
    // pure no-op and execution must continue past the macro site without
    // panicking, returning, or otherwise short-circuiting the test body.
    fail_point!("kernel::lane_d::unset_probe");
    let reached = true;

    drop(scenario);
    assert!(
        reached,
        "fail_point! must be a no-op when no action is configured"
    );
}

/// Lane E smoke: arm `engine::commit::before_publish` with `panic` and
/// confirm a kernel `Engine::commit` invocation actually triggers the
/// hook. We do not exercise recovery here because the unit-test scope
/// only needs to prove the hook is reachable from the canonical commit
/// path; the bench-level matrix proves the recovery contract.
#[test]
fn engine_commit_before_publish_hook_fires() {
    use redlinedb_kernel::engine::{Engine, EngineConfig};

    let scenario = fail::FailScenario::setup();
    failpoints::cfg("engine::commit::before_publish", "panic").expect("configure commit failpoint");

    let tmp = tempfile::tempdir().expect("tempdir");
    let engine = Engine::create(tmp.path(), EngineConfig::default()).expect("create engine");
    let mut tx = engine
        .begin(redlinedb_kernel::txn::Isolation::Snapshot)
        .expect("begin");
    engine
        .insert(&mut tx, b"value-0".to_vec())
        .expect("insert before commit");

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = engine.commit(tx);
    }));

    // Disarm before asserting so a failed assertion does not leak the
    // panic action into other tests sharing the scenario.
    failpoints::cfg("engine::commit::before_publish", "off").expect("disable commit failpoint");
    drop(scenario);

    assert!(
        result.is_err(),
        "engine::commit::before_publish must panic when armed"
    );
}
