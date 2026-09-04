//! Interning the exit values of a diagram.
//!
//! Every operation that rebuilds a diagram ends the same way: give each exit a
//! value, collapse the exits whose values coincide, and reduce the node against
//! the resulting map. The collapsing step is a find-or-insert into a short list,
//! and it is the same problem whether the values are booleans out of
//! [`GcflobddT::map`](crate::gcflobdd::GcflobddT::map) or amplitudes out of a
//! matrix product -- so one interner serves both.

use crate::utils::{HashMap, new_hash_map};

/// A hashable stand-in for a value, if one exists.
///
/// Collapsing exits searches the value list linearly, which is quadratic --
/// and that bites exactly when a diagram has many exits, as a
/// Fourier-transformed state does. A key turns the search into a hash lookup.
///
/// **Contract**: two values must have equal keys if and only if they are `==`.
/// Returning `None` (the default) is always safe and falls back to the linear
/// scan; a *wrong* key silently merges distinct values.
pub trait DedupKey {
    fn dedup_key(&self) -> Option<u128> {
        None
    }
}

/// The bit pattern of an `f64`, normalised so that it agrees with `==`.
///
/// `-0.0 == 0.0` but their bit patterns differ, and no NaN is `==` anything, so
/// both have to be kept away from the hashed path. Complex and other composite
/// amplitudes build their keys out of this.
#[inline]
pub fn float_key(value: f64) -> Option<u128> {
    if value.is_nan() {
        None
    } else if value == 0.0 {
        Some(0)
    } else {
        Some(value.to_bits() as u128)
    }
}

impl DedupKey for bool {
    #[inline]
    fn dedup_key(&self) -> Option<u128> {
        Some(*self as u128)
    }
}

macro_rules! integer_dedup_key {
    ($t:ty) => {
        impl DedupKey for $t {
            #[inline]
            fn dedup_key(&self) -> Option<u128> {
                Some(*self as i128 as u128)
            }
        }
    };
}
integer_dedup_key!(i32);
integer_dedup_key!(i64);

impl DedupKey for f64 {
    #[inline]
    fn dedup_key(&self) -> Option<u128> {
        float_key(*self)
    }
}

/// `rug::Complex` carries a precision and has no cheap canonical bit pattern,
/// so it stays on the linear scan.
#[cfg(feature = "complex")]
impl DedupKey for rug::Complex {}

/// Where hashing starts to pay for itself, measured on the dense matrix
/// multiply (few exits) against the QFT (many).
const HASH_THRESHOLD: usize = 16;

/// Interns values, giving each distinct one an index.
///
/// Scans while the list is short and switches to hashing once it is not: the
/// scan wins for the handful of values a boolean or structured operand
/// produces, and the hash saves the quadratic blow-up on a diagram with
/// thousands of exits. Values whose type cannot produce a
/// [`DedupKey::dedup_key`] simply stay on the scan.
pub(super) struct ValueSet<T> {
    pub(super) values: Vec<T>,
    keys: HashMap<u128, usize>,
    hashed: bool,
    unkeyable: bool,
}

impl<T> Default for ValueSet<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            keys: new_hash_map(),
            hashed: false,
            unkeyable: false,
        }
    }
}

impl<T: PartialEq + DedupKey> ValueSet<T> {
    pub(super) fn intern(&mut self, value: T) -> usize {
        if !self.hashed && !self.unkeyable && self.values.len() >= HASH_THRESHOLD {
            match self
                .values
                .iter()
                .map(T::dedup_key)
                .collect::<Option<Vec<_>>>()
            {
                Some(keys) => {
                    self.keys = keys.into_iter().zip(0..).collect();
                    self.hashed = true;
                }
                // One value without a key means the map could never answer
                // correctly; stop trying.
                None => self.unkeyable = true,
            }
        }
        if self.hashed {
            if let Some(key) = value.dedup_key() {
                let next = self.values.len();
                let values = &mut self.values;
                return *self.keys.entry(key).or_insert_with(|| {
                    values.push(value);
                    next
                });
            }
            // Mixed keyable and not: fall back for good, which stays correct
            // because both paths search the same list.
            self.hashed = false;
            self.unkeyable = true;
        }
        match self.values.iter().position(|v| *v == value) {
            Some(index) => index,
            None => {
                self.values.push(value);
                self.values.len() - 1
            }
        }
    }
}
