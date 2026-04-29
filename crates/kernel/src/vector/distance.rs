//! Minimal scalar distance kernels — Lane V2 stand-in for Lane V1's
//! `distance.rs`. Every item in this file is gated under
//! `feature = "vector_v1_unmerged"` (which is the kernel's default until
//! V1 lands) so that during fusion the entire file is replaced by Lane V1's
//! SIMD-aware version. HNSW imports `l2_distance`, `cosine_distance`, and
//! `inner_product_distance` by name; the V1 surface preserves those
//! signatures, so callers don't change.

use crate::{Error, Result};

/// Distance metric for vector indexes. Mirrors the SQL-side
/// `USING hnsw(metric='l2'|'cosine'|'ip')` parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Metric {
    L2,
    Cosine,
    InnerProduct,
}

impl Metric {
    /// Wire encoding: a single byte that survives serialization in the
    /// HNSW meta page.
    pub fn as_u8(self) -> u8 {
        match self {
            Metric::L2 => 0,
            Metric::Cosine => 1,
            Metric::InnerProduct => 2,
        }
    }

    /// Inverse of [`Metric::as_u8`].
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Metric::L2),
            1 => Ok(Metric::Cosine),
            2 => Ok(Metric::InnerProduct),
            _ => Err(Error::CorruptPage("unknown vector metric")),
        }
    }

    /// Compute the configured distance between two same-length vectors.
    /// Returns an error when dimensions disagree — HNSW relies on this
    /// to surface mis-shaped queries instead of silently truncating.
    pub fn distance(self, a: &[f32], b: &[f32]) -> Result<f32> {
        match self {
            Metric::L2 => l2_distance(a, b),
            Metric::Cosine => cosine_distance(a, b),
            Metric::InnerProduct => inner_product_distance(a, b),
        }
    }
}

fn check_same_dim(a: &[f32], b: &[f32]) -> Result<()> {
    if a.len() != b.len() {
        return Err(Error::CorruptPage("vector dimension mismatch"));
    }
    Ok(())
}

/// Squared L2 distance. We intentionally do not take a square root: HNSW
/// orders candidates by distance, and `sqrt` is monotone, so omitting it
/// preserves order while saving an FPU op per comparison. Matches what
/// Lane V1's SIMD kernel returns.
#[cfg(feature = "vector_v1_unmerged")]
pub fn l2_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    check_same_dim(a, b)?;
    let mut acc = 0.0_f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        acc += d * d;
    }
    Ok(acc)
}

/// Cosine distance: `1 - cos_sim`. Returns 0 for identical inputs and 2
/// for diametrically opposite ones; matches Lane V1's contract.
#[cfg(feature = "vector_v1_unmerged")]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    check_same_dim(a, b)?;
    let mut dot = 0.0_f32;
    let mut na = 0.0_f32;
    let mut nb = 0.0_f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    let denom = (na.sqrt() * nb.sqrt()).max(f32::MIN_POSITIVE);
    Ok(1.0 - dot / denom)
}

/// Inner-product *distance*: `-(a · b)`. Negation makes "closer" mean
/// numerically smaller, the convention HNSW's beam search expects.
#[cfg(feature = "vector_v1_unmerged")]
pub fn inner_product_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    check_same_dim(a, b)?;
    let mut acc = 0.0_f32;
    for i in 0..a.len() {
        acc += a[i] * b[i];
    }
    Ok(-acc)
}
