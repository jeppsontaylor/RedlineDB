use std::time::Duration;

use hdrhistogram::Histogram;

use crate::report::LatencySummary;

#[derive(Debug)]
pub struct Metrics {
    latency: Histogram<u64>,
    operations: u64,
    failures: u64,
    busy_errors: u64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            latency: Histogram::new(3).expect("histogram"),
            operations: 0,
            failures: 0,
            busy_errors: 0,
        }
    }

    pub fn record_success(&mut self, elapsed: Duration) {
        self.operations += 1;
        let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        let _ = self.latency.record(micros.max(1));
    }

    pub fn record_failure(&mut self, busy: bool) {
        self.failures += 1;
        if busy {
            self.busy_errors += 1;
        }
    }

    pub fn operations(&self) -> u64 {
        self.operations
    }

    pub fn failures(&self) -> u64 {
        self.failures
    }

    pub fn busy_errors(&self) -> u64 {
        self.busy_errors
    }

    pub fn latency(&self) -> LatencySummary {
        LatencySummary {
            p50_us: self.latency.value_at_quantile(0.50),
            p95_us: self.latency.value_at_quantile(0.95),
            p99_us: self.latency.value_at_quantile(0.99),
            p999_us: self.latency.value_at_quantile(0.999),
            max_us: self.latency.max(),
        }
    }

    pub fn merge(&mut self, other: &Self) {
        self.operations += other.operations;
        self.failures += other.failures;
        self.busy_errors += other.busy_errors;
        let _ = self.latency.add(&other.latency);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_summary_tracks_percentiles() {
        let mut metrics = Metrics::new();
        metrics.record_success(Duration::from_micros(10));
        metrics.record_success(Duration::from_micros(20));
        metrics.record_success(Duration::from_micros(30));
        let latency = metrics.latency();
        assert!(latency.p50_us >= 10);
        assert!(latency.p999_us >= 30);
        assert_eq!(latency.max_us, 30);
    }
}
