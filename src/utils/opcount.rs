//! Recursive-call counters, for answering "how much work is one `and`?".
//!
//! A per-operation *time* tells you the constant is large; it does not tell you
//! whether that is because each recursive step is expensive or because there are
//! many of them. These separate the two, and they are machine-independent, so
//! they compare directly against another engine's call counts.
//!
//! Off unless `--features opcount`, where every entry point below compiles to
//! nothing. Never enable it for a timing run: the counters are a contended
//! atomic increment on the hottest path in the crate.

#[cfg(feature = "opcount")]
mod on {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Counter slots. `*_CALL` is every entry into the recursive function,
    /// `*_HIT` the subset that the memo table answered without recursing, so
    /// `CALL - HIT` is the work actually done.
    #[derive(Clone, Copy, PartialEq, Eq)]
    #[repr(usize)]
    pub enum C {
        PairProductCall,
        PairProductHit,
        PairMapCall,
        PairMapHit,
        ReduceCall,
        ReduceHit,
        BddPairProductCall,
        BddPairProductHit,
        BddPairMapCall,
        BddPairMapHit,
        BddReduceCall,
        BddReduceHit,
        NodeIntern,
        NodeInternNew,
        ReturnMapIntern,
        ReturnMapInternNew,
    }
    pub const N: usize = 16;
    pub const NAMES: [&str; N] = [
        "pair_product.call",
        "pair_product.hit",
        "pair_map.call",
        "pair_map.hit",
        "reduce.call",
        "reduce.hit",
        "bdd_pair_product.call",
        "bdd_pair_product.hit",
        "bdd_pair_map.call",
        "bdd_pair_map.hit",
        "bdd_reduce.call",
        "bdd_reduce.hit",
        "node_intern.total",
        "node_intern.new",
        "return_map_intern.total",
        "return_map_intern.new",
    ];

    #[allow(clippy::declare_interior_mutable_const)]
    const ZERO: AtomicU64 = AtomicU64::new(0);
    static COUNTERS: [AtomicU64; N] = [ZERO; N];

    #[inline]
    pub fn bump(c: C) {
        COUNTERS[c as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// All counters, in `NAMES` order.
    pub fn snapshot() -> [u64; N] {
        std::array::from_fn(|i| COUNTERS[i].load(Ordering::Relaxed))
    }

    pub fn reset() {
        for c in &COUNTERS {
            c.store(0, Ordering::Relaxed);
        }
    }
}

#[cfg(not(feature = "opcount"))]
mod off {
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub enum C {
        PairProductCall,
        PairProductHit,
        PairMapCall,
        PairMapHit,
        ReduceCall,
        ReduceHit,
        BddPairProductCall,
        BddPairProductHit,
        BddPairMapCall,
        BddPairMapHit,
        BddReduceCall,
        BddReduceHit,
        NodeIntern,
        NodeInternNew,
        ReturnMapIntern,
        ReturnMapInternNew,
    }
    pub const N: usize = 16;
    pub const NAMES: [&str; N] = [""; N];

    #[inline(always)]
    pub fn bump(_c: C) {}
    pub fn snapshot() -> [u64; N] {
        [0; N]
    }
    pub fn reset() {}
}

#[cfg(feature = "opcount")]
pub use on::*;
#[cfg(not(feature = "opcount"))]
pub use off::*;
