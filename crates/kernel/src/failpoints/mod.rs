//! Deterministic failpoint registry for the RedlineDB kernel.
//!
//! When the `failpoints` feature is enabled, this module forwards to the
//! [`fail`](https://docs.rs/fail) crate so that tests and benchmarks can
//! configure injection of `panic`, `return(value)`, `off`, `sleep(N)`,
//! `pause`, `print`, and `yield` actions at named points in kernel code.
//!
//! When the feature is disabled, every entry point is an inline no-op so the
//! release build pays zero runtime cost. Lane D owns this scaffolding only;
//! Lane E wires the actual `fail_point!` invocations into hot paths.

pub mod macros;

#[cfg(feature = "failpoints")]
use std::sync::Once;

#[cfg(feature = "failpoints")]
static INIT: Once = Once::new();

/// Initialise the failpoint scenario for the lifetime of the process.
///
/// `fail::FailScenario::setup` returns a guard whose `Drop` impl clears every
/// configured failpoint, which is the correct semantics for a single test
/// case but the wrong semantics for a long-lived embedded engine. We call it
/// exactly once and `mem::forget` the guard so the registry stays armed and
/// callers may then `cfg` failpoints freely from anywhere in the program.
///
/// Calling `init` more than once is safe; the [`Once`] guard makes subsequent
/// calls inert. Without the `failpoints` feature this is a statically
/// inlined no-op.
#[cfg(feature = "failpoints")]
pub fn init() {
    INIT.call_once(|| {
        let scenario = fail::FailScenario::setup();
        std::mem::forget(scenario);
    });
}

/// Initialise the failpoint scenario (no-op when feature is disabled).
#[cfg(not(feature = "failpoints"))]
pub fn init() {}

/// Configure a named failpoint with one of the registry actions.
///
/// Accepted actions match the `fail` crate grammar: `panic`, `return(value)`,
/// `off`, `sleep(N)`, `pause`, `print`, `yield`, plus optional `K%`,
/// `K*action`, and `name->action` chaining.
#[cfg(feature = "failpoints")]
pub fn cfg(name: &str, action: &str) -> Result<(), String> {
    fail::cfg(name, action)
}

/// Configure a named failpoint (no-op when feature is disabled).
#[cfg(not(feature = "failpoints"))]
pub fn cfg(_name: &str, _action: &str) -> Result<(), String> {
    Ok(())
}
