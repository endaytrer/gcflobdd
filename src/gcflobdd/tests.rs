use super::*;

#[test]
fn test_mk_projection_true_false() {
    let grammar = Grammar::new(&[
        "S2 -> S1 S1".to_string(),
        "S1 -> S0 S0".to_string(),
        "S0 -> a".to_string(),
    ])
    .unwrap();
    let context = RefCell::new(Context::default());
    let c1 = Gcflobdd::mk_projection(0, &grammar, &context);
    let assignment = c1.find_one_satisfiable_assignment().unwrap();
    assert_eq!(assignment, vec![Some(true), None, None, None]);

    let c2 = Gcflobdd::mk_projection(1, &grammar, &context);
    let assignment = c2.find_one_satisfiable_assignment().unwrap();
    assert_eq!(assignment, vec![None, Some(true), None, None]);

    let c3 = Gcflobdd::mk_true(&grammar, &context);
    let assignment = c3.find_one_satisfiable_assignment().unwrap();
    assert_eq!(assignment, vec![None, None, None, None]);

    let c4 = Gcflobdd::mk_false(&grammar, &context);
    let assignment = c4.find_one_satisfiable_assignment();
    assert!(assignment.is_none());
}

#[test]
fn test_gc() {
    let grammar = Grammar::new(&[
        "S2 -> S1 S1".to_string(),
        "S1 -> S0 S0".to_string(),
        "S0 -> a".to_string(),
    ])
    .unwrap();
    let context = RefCell::new(Context::default());
    assert_eq!(context.borrow().node_count(), 0);
    let c1 = Gcflobdd::mk_projection(0, &grammar, &context);
    assert_eq!(context.borrow().node_count(), 7); // 1 * S3 + 2 * S2 + 2 * S1 + fork + don't care
    let c2 = Gcflobdd::mk_projection(1, &grammar, &context);
    assert_eq!(context.borrow().node_count(), 9); // 1 * S3 + 1 * S2
    drop(c2);
    context.borrow_mut().gc();
    assert_eq!(context.borrow().node_count(), 7);
    drop(c1);
    context.borrow_mut().gc();
    assert_eq!(context.borrow().node_count(), 0);
}

#[test]
fn test_pair_product() {
    let grammar = Grammar::new(&[
        "S2 -> S1 S1".to_string(),
        "S1 -> S0 S0".to_string(),
        "S0 -> a".to_string(),
    ])
    .unwrap();
    let context = RefCell::new(Context::default());
    let c1 = Gcflobdd::mk_projection(0, &grammar, &context);
    let c2 = Gcflobdd::mk_projection(1, &grammar, &context);
    let c3 = c1.pair_product(&c2, &context);
    drop(c1);
    drop(c2);
    context.borrow_mut().gc();
    let path = c3.find_one_path_to(&(1, 1)).unwrap();
    assert_eq!(path, vec![Some(true), Some(true), None, None]);
}
