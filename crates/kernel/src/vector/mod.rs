//! Vector index lanes for RedlineDB phase-10.
//!
//! Lane V3 (DiskANN) provides a single-layer Vamana graph search index sized
//! for SSD-resident workloads. Lanes V1 (VECTOR type + SIMD distance kernels)
//! and V2 (HNSW) live in sibling modules; until V1 lands, V3 ships its own
//! scalar L2 fallback in [`diskann::distance`] marked for cleanup.

pub mod diskann;
