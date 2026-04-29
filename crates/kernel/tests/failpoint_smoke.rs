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
