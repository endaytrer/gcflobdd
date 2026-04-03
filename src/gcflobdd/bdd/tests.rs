use std::cell::RefCell;

use crate::gcflobdd::{bdd::Bdd, context::Context};

#[test]
fn test_mk_projection_true_false() {
    let context = RefCell::new(Context::default());

    let c1 = Bdd::mk_projection(0, &context);
    let assignment = c1.find_one_path_to(1, 4).unwrap();
    assert_eq!(assignment, vec![Some(true), None, None, None]);

    let c2 = Bdd::mk_projection(1, &context);
    let assignment = c2.find_one_path_to(1, 4).unwrap();
    assert_eq!(assignment, vec![None, Some(true), None, None]);
}

#[test]
fn test_gc() {
    let context = RefCell::new(Context::default());
    assert_eq!(context.borrow().node_count(), 0);
    let c1 = Bdd::mk_projection(0, &context);
    let k1 = context.borrow().node_count();
    let c2 = Bdd::mk_projection(1, &context);
    drop(c2);
    context.borrow_mut().gc();
    assert_eq!(context.borrow().node_count(), k1);
    drop(c1);
    context.borrow_mut().gc();
    assert_eq!(context.borrow().node_count(), 0);
}

#[test]
fn test_pair_product() {
    let context = RefCell::new(Context::default());
    let c1 = Bdd::mk_projection(0, &context);
    let c2 = Bdd::mk_projection(1, &context);
    let c3 = c1.pair_product(&c2, 2, 2, &context);
    let return_map = c3.return_map;
    let c3 = Bdd(c3.entry_point);
    drop(c1);
    drop(c2);
    context.borrow_mut().gc();
    let idx = return_map
        .iter()
        .position(|(i, j)| *i == 1 && *j == 1)
        .unwrap();
    assert_eq!(idx, 3);
    let path = c3.find_one_path_to(idx, 4).unwrap();
    assert_eq!(path, vec![Some(true), Some(true), None, None]);
}

#[test]
fn test_reduce() {
    let context = RefCell::new(Context::default());
    let c1 = Bdd::mk_projection(0, &context);
    let c2 = Bdd::mk_projection(1, &context);
    let c3 = c1.pair_product(&c2, 2, 2, &context);
    let return_map = c3.return_map;
    assert_eq!(return_map, [(0, 0), (0, 1), (1, 0), (1, 1)]);
    let c3 = Bdd(c3.entry_point);
    let c4 = c3.reduce(&[0, 0, 0, 1], 2, &context);
    let path = c4.find_one_path_to(1, 4).unwrap();
    assert_eq!(path, vec![Some(true), Some(true), None, None]);
}

fn mk_op(
    lhs: &Bdd,
    rhs: &Bdd,
    context: &RefCell<Context<'_>>,
    return_map: &[(usize, usize)],
    reduce_map: &[usize],
    num_exits: usize,
) -> Bdd {
    let c1 = lhs.pair_product(rhs, 2, 2, context);
    let ans = Bdd(c1.entry_point);
    assert_eq!(c1.return_map, return_map);
    ans.reduce(reduce_map, num_exits, context)
}
#[test]
fn test_node_table() {
    let context = RefCell::new(Context::default());
    let c1 = Bdd::mk_projection(0, &context);
    let c1_prime = Bdd::mk_projection(0, &context);
    assert_eq!(c1, c1_prime);

    let c0 = Bdd::mk_false(&context);
    let c_true = Bdd::mk_true(&context);
    let not_c1 = Bdd::mk_inverse_projection(0, &context);
    let c2 = Bdd::mk_projection(1, &context);
    let not_c2 = Bdd::mk_inverse_projection(1, &context);

    // c1 AND NOT c1 == False
    let c1_and_not_c1 = mk_op(&c1, &not_c1, &context, &[(0, 1), (1, 0)], &[0, 0], 1);
    assert_eq!(c1_and_not_c1, c0);

    // c1 OR NOT c1 == true
    let c1_or_not_c1 = mk_op(&c1, &not_c1, &context, &[(0, 1), (1, 0)], &[1, 1], 1);
    assert_eq!(c1_or_not_c1, c_true);

    // c1 AND | OR c1 == c1
    let c1_and_c1 = mk_op(&c1, &c1, &context, &[(0, 0), (1, 1)], &[0, 1], 2);
    assert_eq!(c1_and_c1, c1);

    // c1 AND c2 == c2 AND c1
    let c1_and_c2 = mk_op(
        &c1,
        &c2,
        &context,
        &[(0, 0), (0, 1), (1, 0), (1, 1)],
        &[0, 0, 0, 1],
        2,
    );
    let c2_and_c1 = mk_op(
        &c2,
        &c1,
        &context,
        &[(0, 0), (1, 0), (0, 1), (1, 1)],
        &[0, 0, 0, 1],
        2,
    );
    assert_eq!(c1_and_c2, c2_and_c1);

    // c1 OR c2 == c2 OR c1
    let c1_or_c2 = mk_op(
        &c1,
        &c2,
        &context,
        &[(0, 0), (0, 1), (1, 0), (1, 1)],
        &[0, 1, 1, 1],
        2,
    );
    let c2_or_c1 = mk_op(
        &c2,
        &c1,
        &context,
        &[(0, 0), (1, 0), (0, 1), (1, 1)],
        &[0, 1, 1, 1],
        2,
    );
    assert_eq!(c1_or_c2, c2_or_c1);

    // NOT (c1 AND c2) == (NOT c1) OR (NOT c2)
    let not_c1_and_c2 = mk_op(
        &c1,
        &c2,
        &context,
        &[(0, 0), (0, 1), (1, 0), (1, 1)],
        &[1, 1, 1, 0],
        2,
    );
    let not_c1_or_not_c2 = mk_op(
        &not_c1,
        &not_c2,
        &context,
        &[(1, 1), (1, 0), (0, 1), (0, 0)],
        &[1, 1, 1, 0],
        2,
    );

    assert_eq!(not_c1_and_c2, not_c1_or_not_c2);
}
