//! DiskANN (Vamana) single-layer SSD-resident vector graph index.
//!
//! Reference: Subramanya, Devvrit, Kadekodi, Krishnaswamy, Simhadri,
//! "DiskANN: Fast Accurate Billion-point Nearest Neighbor Search on a Single
//! Node," NeurIPS 2019.
//!
//! This commit lands the public type skeleton plus the sector layout — the
//! algorithm modules (RobustPrune, Vamana builder, beam search, the
//! `DiskAnnIndex` public API) follow in subsequent commits.

mod sectors;

pub use sectors::{SECTOR_SIZE, SectorError, SectorLayout, decode_node, encode_node};

/// Stable identifier for a vector row stored in the graph. Carries the
/// original row id supplied at build time so callers can map search hits back
/// to heap tuples without an extra lookup table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId(pub u64);

/// User-facing build configuration (degree, search list size, alpha).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DiskAnnParams {
    /// Maximum out-degree per node (`R` in the paper). Standard 64.
    pub max_degree: usize,
    /// Search-list size used during build (`L` in the paper). Should be
    /// >= max_degree; 100 is a typical baseline.
    pub search_list_size: usize,
    /// RobustPrune relaxation factor. 1.0 == strict; 1.2 trades a few
    /// extra edges for substantially better recall.
    pub alpha: f32,
    /// Random seed for the medoid bootstrap and tie-breaks. Tests pin this.
    pub seed: u64,
}

impl Default for DiskAnnParams {
    fn default() -> Self {
        Self {
            max_degree: 64,
            search_list_size: 100,
            alpha: 1.2,
            seed: 0x5EED_D15C_0A99_BABE_u64,
        }
    }
}

/// Scalar-only L2-squared fallback shared by builder and searcher. When Lane
/// V1 lands, callers should switch to `crate::vector::distance::l2_squared`
/// and this helper can be deleted. Currently unused (consumed by the
/// algorithm modules that land in subsequent commits on this lane).
// TODO(lane-v1-cleanup): replace with `crate::vector::distance::l2_squared`.
#[allow(dead_code)]
pub(crate) mod distance {
    /// Squared L2 distance — sufficient for ranking; we never need the
    /// square root because monotonicity is preserved.
    #[inline]
    pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len());
        let mut acc = 0.0f32;
        for i in 0..a.len() {
            let d = a[i] - b[i];
            acc += d * d;
        }
        acc
    }
}
