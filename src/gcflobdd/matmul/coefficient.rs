//! The integer that counts how many products coincide.
//!
//! The deferred semiring records, for each pair of operand exits, how many
//! times that product occurs. A Hadamard layer over `n` qubits makes that count
//! `2^n`, so the width of this integer is what caps the qubit count: `i128`
//! stops at 128 qubits.
//!
//! Which representation is used is a build-time choice:
//!
//! - default: `i128`, and an overflow panics rather than wrapping;
//! - `bigint` feature: [`rug::Integer`], arbitrary precision, no ceiling --
//!   what the reference C++ CFLOBDD does with `boost::multiprecision::cpp_int`.
//!
//! BENCHMARKS.md measures both.

#[cfg(not(feature = "bigint"))]
type Repr = i128;
#[cfg(feature = "bigint")]
type Repr = rug::Integer;

/// A path multiplicity. Cloning is cheap in the default build and an
/// allocation under `bigint`, so the hot paths pass it by reference.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Default)]
pub struct Coefficient(Repr);

impl Coefficient {
    #[inline]
    pub fn one() -> Self {
        Self(Repr::from(1))
    }

    #[inline]
    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }

    /// Sum. Panics on overflow in the default build; `bigint` cannot overflow.
    #[inline]
    pub fn add(&self, rhs: &Self) -> Self {
        #[cfg(not(feature = "bigint"))]
        {
            Self(
                self.0
                    .checked_add(rhs.0)
                    .expect("matmul coefficient overflow: rebuild with --features bigint"),
            )
        }
        #[cfg(feature = "bigint")]
        {
            Self(rug::Integer::from(&self.0 + &rhs.0))
        }
    }

    /// Product, with the same overflow behaviour as [`Self::add`].
    #[inline]
    pub fn mul(&self, rhs: &Self) -> Self {
        #[cfg(not(feature = "bigint"))]
        {
            Self(
                self.0
                    .checked_mul(rhs.0)
                    .expect("matmul coefficient overflow: rebuild with --features bigint"),
            )
        }
        #[cfg(feature = "bigint")]
        {
            Self(rug::Integer::from(&self.0 * &rhs.0))
        }
    }

    /// The value as an `i128`, or `None` if it does not fit -- which only a
    /// `bigint` build can produce.
    #[inline]
    pub fn to_i128(&self) -> Option<i128> {
        #[cfg(not(feature = "bigint"))]
        {
            Some(self.0)
        }
        #[cfg(feature = "bigint")]
        {
            self.0.to_i128()
        }
    }

    /// The value as an `f64`, rounding once it exceeds 2^53. Amplitudes are
    /// floating point anyway, so this is the natural conversion for them.
    #[inline]
    pub fn to_f64(&self) -> f64 {
        #[cfg(not(feature = "bigint"))]
        {
            self.0 as f64
        }
        #[cfg(feature = "bigint")]
        {
            self.0.to_f64()
        }
    }

    /// The value as an exact `rug::Integer`, for value types that can use it.
    /// Available whenever rug is -- either feature pulls it in.
    #[cfg(any(feature = "complex", feature = "bigint"))]
    #[inline]
    pub fn to_rug(&self) -> rug::Integer {
        #[cfg(not(feature = "bigint"))]
        {
            rug::Integer::from(self.0)
        }
        #[cfg(feature = "bigint")]
        {
            self.0.clone()
        }
    }
}

impl From<i64> for Coefficient {
    #[inline]
    fn from(value: i64) -> Self {
        Self(Repr::from(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arithmetic_and_conversions() {
        let one = Coefficient::one();
        assert!(!one.is_zero());
        assert!(Coefficient::default().is_zero());
        assert_eq!(one.add(&one), Coefficient::from(2));
        assert_eq!(one.add(&Coefficient::from(-1)), Coefficient::default());
        assert_eq!(
            Coefficient::from(3).mul(&Coefficient::from(-4)).to_i128(),
            Some(-12)
        );
        assert_eq!(Coefficient::from(5).to_f64(), 5.0);

        // Equal values compare and hash alike whichever representation is in
        // use, which the `MatMulMap` canonical form depends on.
        assert_eq!(Coefficient::from(7), Coefficient::from(7));
        assert_ne!(Coefficient::from(7), Coefficient::from(8));
    }

    /// Doubling past 2^127: the default build refuses, `bigint` keeps going.
    #[test]
    fn overflow_behaviour_matches_the_build() {
        let mut value = Coefficient::one();
        let mut doublings = 0;
        for _ in 0..200 {
            let doubled =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| value.add(&value)));
            match doubled {
                Ok(next) => {
                    value = next;
                    doublings += 1;
                }
                Err(_) => break,
            }
        }
        if cfg!(feature = "bigint") {
            assert_eq!(doublings, 200, "arbitrary precision should never overflow");
        } else {
            assert_eq!(doublings, 126, "i128 holds 2^126 but not 2^127 signed");
        }
    }
}
