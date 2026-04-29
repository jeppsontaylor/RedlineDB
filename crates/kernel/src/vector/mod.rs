//! Vector indexing primitives for RedlineDB phase-10.
//!
//! Lane V2 owns the HNSW (Hierarchical Navigable Small World) approximate
//! nearest-neighbor index in [`hnsw`]. Lane V1 owns the in-memory `VECTOR`
//! type, SIMD distance kernels (`distance.rs`, `simd.rs`), the flat scan
//! baseline (`flat.rs`), and the on-disk codec (`codec.rs`). Lane V3 owns
//! `diskann/`. This `mod.rs` is shared across the three lanes — fusion
//! reconciles a single declaration list.
//!
//! Until Lane V1 lands its real `distance.rs` (with SIMD), this branch ships
//! a minimal scalar shim. The shim itself is gated under
//! `feature = "vector_v1_unmerged"` so fusion can delete it cleanly and let
//! HNSW compile unchanged against the V1 surface.

pub mod hnsw;

pub(crate) mod distance;
