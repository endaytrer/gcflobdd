use std::cmp::Ordering;

use crate::gcflobdd::matmul::coefficient::Coefficient;

/// A symbolic linear combination of products of operand values.
///
/// A `MatMulMap` maps a pair `(i, j)` -- exit `i` of the left operand node and
/// exit `j` of the right operand node -- to an integer coefficient, and denotes
///
/// ```text
///     sum over (i, j)  coeff * value1[i] * value2[j]
/// ```
///
/// This is the "deferred semiring" of the matrix-multiply recursion: no real
/// number is ever touched below the top node, so the node-level recursion (and
/// its cache) depends only on structure, and the same code serves every value
/// type.
///
/// The coefficients count how many products coincide, which for a
/// Hadamard-style operator is exponential in the qubit count; see
/// [`Coefficient`] for the width and what happens when it runs out.
///
/// Entries are kept sorted by `(i, j)` and any key whose coefficient reaches
/// zero is dropped, so `PartialEq`/`Hash` are canonical and the empty map is
/// the *only* spelling of zero. (The C++ implementation instead writes zero as
/// a `(-1, -1)` sentinel that coexists with genuine zero coefficients, which is
/// exactly the conflation the algorithm notes warn about.)
#[derive(Clone, PartialEq, Eq, Hash, Debug, Default)]
pub(crate) struct MatMulMap(Vec<(usize, usize, Coefficient)>);

impl MatMulMap {
    /// The additive identity: an empty combination.
    #[inline]
    pub fn zero() -> Self {
        Self(Vec::new())
    }

    /// `value1[i] * value2[j]`.
    #[inline]
    pub fn single(i: usize, j: usize) -> Self {
        Self(vec![(i, j, Coefficient::one())])
    }

    #[inline]
    pub fn is_zero(&self) -> bool {
        self.0.is_empty()
    }

    /// The pairs and their coefficients. By reference: a coefficient is an
    /// allocation in a `bigint` build.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (usize, usize, &Coefficient)> + '_ {
        self.0.iter().map(|(i, j, c)| (*i, *j, c))
    }

    /// Merge, summing coefficients on shared keys. Keys that cancel to a zero
    /// coefficient disappear, which is the one place the recursion can shrink.
    pub fn add(&self, rhs: &Self) -> Self {
        let mut out = Vec::with_capacity(self.0.len() + rhs.0.len());
        let (mut a, mut b) = (0, 0);
        while a < self.0.len() && b < rhs.0.len() {
            let (i1, j1, c1) = &self.0[a];
            let (i2, j2, c2) = &rhs.0[b];
            match (i1, j1).cmp(&(i2, j2)) {
                Ordering::Less => {
                    out.push(self.0[a].clone());
                    a += 1;
                }
                Ordering::Greater => {
                    out.push(rhs.0[b].clone());
                    b += 1;
                }
                Ordering::Equal => {
                    let c = c1.add(c2);
                    if !c.is_zero() {
                        out.push((*i1, *j1, c));
                    }
                    a += 1;
                    b += 1;
                }
            }
        }
        out.extend_from_slice(&self.0[a..]);
        out.extend_from_slice(&rhs.0[b..]);
        Self(out)
    }

    /// Scale every coefficient by `coeff`.
    pub fn scale(&self, coeff: &Coefficient) -> Self {
        if coeff.is_zero() {
            return Self::zero();
        }
        Self(
            self.0
                .iter()
                .map(|(i, j, c)| (*i, *j, c.mul(coeff)))
                .collect(),
        )
    }

    /// Translate the pair keys out of a child's exit space into its parent's,
    /// through the two connections' return maps.
    ///
    /// Distinct child pairs routinely land on the same parent pair; summing the
    /// coefficients there is how "I added N terms" is compressed into "one pair
    /// with coefficient N".
    pub fn lift(&self, return_map1: &[usize], return_map2: &[usize]) -> Self {
        let mut out = self
            .0
            .iter()
            .map(|(i, j, c)| (return_map1[*i], return_map2[*j], c.clone()))
            .collect::<Vec<_>>();
        out.sort_unstable_by_key(|(i, j, _)| (*i, *j));
        Self::compacted(out)
    }

    /// Sum adjacent entries that share a key, dropping the ones that cancel.
    fn compacted(sorted: Vec<(usize, usize, Coefficient)>) -> Self {
        let mut out: Vec<(usize, usize, Coefficient)> = Vec::with_capacity(sorted.len());
        for (i, j, c) in sorted {
            match out.last_mut() {
                Some(last) if last.0 == i && last.1 == j => {
                    last.2 = last.2.add(&c);
                    if last.2.is_zero() {
                        out.pop();
                    }
                }
                _ => out.push((i, j, c)),
            }
        }
        Self(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_merges_and_cancels() {
        let a = MatMulMap::single(1, 2);
        assert_eq!(a.add(&MatMulMap::zero()), a);
        assert_eq!(a.add(&a), MatMulMap(vec![(1, 2, Coefficient::from(2))]));
        // Cancellation leaves the *empty* map, not a zero-coefficient key.
        assert_eq!(a.add(&a.scale(&Coefficient::from(-1))), MatMulMap::zero());
        assert!(a.add(&a.scale(&Coefficient::from(-1))).is_zero());

        let b = MatMulMap::single(0, 3);
        assert_eq!(
            a.add(&b),
            MatMulMap(vec![(0, 3, Coefficient::one()), (1, 2, Coefficient::one())])
        );
        assert_eq!(a.add(&b), b.add(&a));
    }

    #[test]
    fn lift_translates_and_sums_collisions() {
        // Child exits 0 and 1 both lead to parent exit 0 on the left.
        let m = MatMulMap::single(0, 0).add(&MatMulMap::single(1, 0));
        assert_eq!(
            m.lift(&[0, 0], &[7]),
            MatMulMap(vec![(0, 7, Coefficient::from(2))])
        );
        assert_eq!(
            m.lift(&[0, 1], &[7]),
            MatMulMap(vec![(0, 7, Coefficient::one()), (1, 7, Coefficient::one())])
        );
        // ... and a lift that cancels drops the key entirely.
        let m = MatMulMap::single(0, 0).add(&MatMulMap::single(1, 0).scale(&Coefficient::from(-1)));
        assert!(m.lift(&[3, 3], &[4]).is_zero());
    }

    #[test]
    fn scale_by_zero_is_zero() {
        assert!(
            MatMulMap::single(4, 5)
                .scale(&Coefficient::default())
                .is_zero()
        );
    }
}
