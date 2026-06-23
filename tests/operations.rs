//! Exercises the ported `pair_product` / `reduce` operations on a tiny grammar
//! whose diagrams are small enough to reason about by hand.

use gcflobdd::declare_grammar;
use gcflobdd::gcflobdd::connection::Connection;
use gcflobdd::grammar::{RecursiveGrammar, Unit};
use std::rc::Rc;

declare_grammar! {
    A => Unit, Unit;          // NUM_VARS = 2
    Tri => Unit, Unit, Unit;  // NUM_VARS = 3
}

type AConn = <A as RecursiveGrammar>::Connection;
type TriConn = <Tri as RecursiveGrammar>::Connection;

#[test]
fn pair_product_distributes_distinctions() {
    let d0 = AConn::mk_distinction(0);
    let d1 = AConn::mk_distinction(1);
    let nd = AConn::mk_no_distinction();

    // distinction × no-distinction is the same distinction (no-distinction has a
    // single exit, so the product structure is unchanged).
    let (r, map) = AConn::pair_product(&d0, &nd);
    assert!(Rc::ptr_eq(&r, &d0));
    assert_eq!(map, vec![(0, 0), (1, 0)]);

    let (r, map) = AConn::pair_product(&nd, &d0);
    assert!(Rc::ptr_eq(&r, &d0));
    assert_eq!(map, vec![(0, 0), (0, 1)]);

    // distinguishing both variables gives the full 2-variable tree: 4 exits, one
    // per (var0, var1) combination, in scan order.
    let (r, map) = AConn::pair_product(&d0, &d1);
    assert_eq!(r.num_exits(), 4);
    assert_eq!(map, vec![(0, 0), (0, 1), (1, 0), (1, 1)]);

    // identical operands short-circuit via the pointer-equality fast path.
    let (r2, map2) = AConn::pair_product(&d0, &d0);
    assert!(Rc::ptr_eq(&r2, &d0));
    assert_eq!(map2, vec![(0, 0), (1, 1)]);
}

#[test]
fn pair_product_is_cached() {
    let d0 = AConn::mk_distinction(0);
    let d1 = AConn::mk_distinction(1);
    let (r1, _) = AConn::pair_product(&d0, &d1);
    let (r2, _) = AConn::pair_product(&d0, &d1);
    assert!(Rc::ptr_eq(&r1, &r2), "second call must hit the cache");
}

#[test]
fn reduce_identity_merge_and_xor() {
    let d0 = AConn::mk_distinction(0);
    let d1 = AConn::mk_distinction(1);
    let nd = AConn::mk_no_distinction();

    // identity reduce-map returns the same Rc.
    let r = AConn::reduce(&d0, &[0, 1], 2);
    assert!(Rc::ptr_eq(&r, &d0));

    // merging both exits collapses to no-distinction.
    let r = AConn::reduce(&d0, &[0, 0], 1);
    assert!(Rc::ptr_eq(&r, &nd));

    // reduce the 4-exit product with an XOR pattern → a 2-exit diagram, and the
    // result is cached (a second identical reduce is pointer-equal).
    let (prod, _) = AConn::pair_product(&d0, &d1);
    let xor = AConn::reduce(&prod, &[0, 1, 1, 0], 2);
    assert_eq!(xor.num_exits(), 2);
    let xor2 = AConn::reduce(&prod, &[0, 1, 1, 0], 2);
    assert!(Rc::ptr_eq(&xor, &xor2));

    // XOR is symmetric: reducing with the same pattern via the other variable
    // order yields the same node (canonical, hash-consed).
    let (prod2, _) = AConn::pair_product(&d1, &d0);
    let xor3 = AConn::reduce(&prod2, &[0, 1, 1, 0], 2);
    assert!(Rc::ptr_eq(&xor, &xor3));
}

#[test]
fn pair_map_computes_binary_ops() {
    let d0 = AConn::mk_distinction(0);
    let d1 = AConn::mk_distinction(1);

    // op-matrix is indexed `lhs_exit * rhs_num_exits + rhs_exit`.
    let op_xor = gcflobdd::gcflobdd::intern_op_matrix(vec![0, 1, 1, 0]);
    let (xor, map) = AConn::pair_map(&d0, &d1, &op_xor, 2);
    assert_eq!(xor.num_exits(), 2);
    assert_eq!(map, vec![0, 1]);

    // pair_map is exactly pair_product followed by the op relabel + reduce.
    let (prod, _) = AConn::pair_product(&d0, &d1);
    let xor_via_reduce = AConn::reduce(&prod, &[0, 1, 1, 0], 2);
    assert!(Rc::ptr_eq(&xor, &xor_via_reduce));

    // AND maps three of the four input combinations to 0, so it is a distinct
    // 2-exit diagram.
    let op_and = gcflobdd::gcflobdd::intern_op_matrix(vec![0, 0, 0, 1]);
    let (and, and_map) = AConn::pair_map(&d0, &d1, &op_and, 2);
    assert_eq!(and.num_exits(), 2);
    assert_eq!(and_map, vec![0, 1]);
    assert!(!Rc::ptr_eq(&and, &xor));

    // cached: a second identical pair_map is pointer-equal.
    let (xor2, _) = AConn::pair_map(&d0, &d1, &op_xor, 2);
    assert!(Rc::ptr_eq(&xor, &xor2));
}

#[test]
fn three_way_parity_is_canonical_across_orders() {
    // Exercises multi-level recursion (3 components) and canonicity: XOR is
    // associative + commutative, so 3-way parity built in different orders must
    // hash-cons to the *same* node.
    let d0 = TriConn::mk_distinction(0);
    let d1 = TriConn::mk_distinction(1);
    let d2 = TriConn::mk_distinction(2);
    let op_xor = gcflobdd::gcflobdd::intern_op_matrix(vec![0, 1, 1, 0]);

    // (v0 ^ v1) ^ v2
    let (x01, _) = TriConn::pair_map(&d0, &d1, &op_xor, 2);
    assert_eq!(x01.num_exits(), 2);
    let (left, _) = TriConn::pair_map(&x01, &d2, &op_xor, 2);
    assert_eq!(left.num_exits(), 2);

    // v0 ^ (v1 ^ v2)
    let (x12, _) = TriConn::pair_map(&d1, &d2, &op_xor, 2);
    let (right, _) = TriConn::pair_map(&d0, &x12, &op_xor, 2);

    assert!(
        Rc::ptr_eq(&left, &right),
        "parity must be canonical regardless of association order"
    );
}
