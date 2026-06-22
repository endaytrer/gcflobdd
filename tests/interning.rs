//! Verifies the per-type interning tables and operation caches that
//! `declare_grammar!` generates for the connection-graph engine.

use gcflobdd::declare_grammar;
use gcflobdd::gcflobdd::connection_graph::{Connection, OpCached};
use gcflobdd::grammar::declarative::{RecursiveGrammar, Unit};
use std::rc::Rc;

declare_grammar! {
    A => Unit, Unit;       // NUM_VARS = 2
    B => A, A, Unit;       // NUM_VARS = 5; shares the `A, Unit` / `Unit` suffixes
}

type BConn = <B as RecursiveGrammar>::Connection;

#[test]
fn connections_are_hash_consed() {
    // Equal diagrams, built independently, must share one Rc (pointer identity).
    let c1 = BConn::mk_distinction(0);
    let c2 = BConn::mk_distinction(0);
    assert!(Rc::ptr_eq(&c1, &c2), "equal diagrams must be deduplicated");

    let n1 = BConn::mk_no_distinction();
    let n2 = BConn::mk_no_distinction();
    assert!(Rc::ptr_eq(&n1, &n2));

    assert!(!Rc::ptr_eq(&c1, &n1), "distinct diagrams must differ");
}

#[test]
fn op_cache_round_trips_by_pointer_identity() {
    let lhs = BConn::mk_distinction(0);
    let rhs = BConn::mk_no_distinction();

    // Cold miss.
    assert!(BConn::get_pair_product_cache(&lhs, &rhs).is_none());

    // Store and read back.
    BConn::set_pair_product_cache(&lhs, &rhs, (lhs.clone(), vec![(0, 0)]));
    let hit = BConn::get_pair_product_cache(&lhs, &rhs).expect("should be cached");
    assert!(Rc::ptr_eq(&hit.0, &lhs));
    assert_eq!(hit.1, vec![(0, 0)]);

    // Keyed on the ordered pair of operand pointers: swapping them misses.
    assert!(BConn::get_pair_product_cache(&rhs, &lhs).is_none());
}
