use super::*;

macro_rules! grammar_choice {
    () => {
        [
            Grammar::new(&["S2 -> S1 S1".to_string(), "S1 -> a a".to_string()]).unwrap(),
            Grammar::new(&[
                "S2 -> S1 S1".to_string(),
                "S1 -> S0 S0".to_string(),
                "S0 -> a".to_string(),
            ])
            .unwrap(),
            Grammar::new(&["S2 -> BDD(4)".to_string()]).unwrap(),
            Grammar::new(&["S2 -> BDD(2) BDD(2)".to_string()]).unwrap(),
            Grammar::new(&["S2 -> S1 S1".to_string(), "S1 -> BDD(2)".to_string()]).unwrap(),
        ]
    };
}
#[test]
fn test_mk_projection_true_false() {
    for grammar in grammar_choice!() {
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
}

#[test]
fn test_gc() {
    for grammar in grammar_choice!() {
        let context = RefCell::new(Context::default());
        assert_eq!(context.borrow().node_count(), 0);
        let c1 = Gcflobdd::mk_projection(0, &grammar, &context);
        let k1 = context.borrow().node_count();
        let c2 = Gcflobdd::mk_projection(1, &grammar, &context);
        drop(c2);
        context.borrow_mut().gc();
        assert_eq!(context.borrow().node_count(), k1);
        drop(c1);
        context.borrow_mut().gc();
        assert_eq!(context.borrow().node_count(), 0);
    }
}

#[test]
fn test_pair_product() {
    for grammar in grammar_choice!() {
        let context = RefCell::new(Context::default());
        let c1 = Gcflobdd::mk_projection(0, &grammar, &context);
        let c2 = Gcflobdd::mk_projection(1, &grammar, &context);
        let c3 = c1.pair_product(&c2, &context);
        drop(c1);
        drop(c2);
        context.borrow_mut().gc();
        let path = c3.find_one_path_to(&(true, true)).unwrap();
        assert_eq!(path, vec![Some(true), Some(true), None, None]);
    }
}

#[test]
fn test_op() {
    for grammar in grammar_choice!() {
        let context = RefCell::new(Context::default());
        let c1 = Gcflobdd::mk_projection(0, &grammar, &context);
        let c2 = Gcflobdd::mk_projection(1, &grammar, &context);
        let c3 = c1.mk_op(&c2, |a, b| a & b, &context);
        drop(c1);
        drop(c2);
        context.borrow_mut().gc();
        let path = c3.find_one_satisfiable_assignment().unwrap();
        assert_eq!(path, vec![Some(true), Some(true), None, None]);
    }
}

#[test]
fn test_node_table() {
    for grammar in grammar_choice!() {
        let context = RefCell::new(Context::default());
        let c1 = Gcflobdd::mk_projection(0, &grammar, &context);
        let c1_prime = Gcflobdd::mk_projection(0, &grammar, &context);
        assert_eq!(c1, c1_prime);

        let c0 = Gcflobdd::mk_false(&grammar, &context);
        let c_true = Gcflobdd::mk_true(&grammar, &context);

        // c1 AND NOT c1 == False
        let c1_and_not_c1 = c1.mk_and(&c1.mk_not(), &context);
        assert_eq!(c1_and_not_c1, c0);

        // c1 OR NOT c1 == True
        let c1_or_not_c1 = c1.mk_or(&c1.mk_not(), &context);
        assert_eq!(c1_or_not_c1, c_true);

        // c1 AND c1 == c1
        let c1_and_c1 = c1.mk_and(&c1, &context);
        assert_eq!(c1_and_c1, c1);

        // c1 OR c1 == c1
        let c1_or_c1 = c1.mk_or(&c1, &context);
        assert_eq!(c1_or_c1, c1);

        let c2 = Gcflobdd::mk_projection(1, &grammar, &context);

        // c1 AND c2 == c2 AND c1
        let c1_and_c2 = c1.mk_and(&c2, &context);
        let c2_and_c1 = c2.mk_and(&c1, &context);
        assert_eq!(c1_and_c2, c2_and_c1);

        // c1 OR c2 == c2 OR c1
        let c1_or_c2 = c1.mk_or(&c2, &context);
        let c2_or_c1 = c2.mk_or(&c1, &context);
        assert_eq!(c1_or_c2, c2_or_c1);

        // NOT (c1 AND c2) == (NOT c1) OR (NOT c2)
        let not_c1_and_c2 = c1_and_c2.mk_not();
        let not_c1_or_not_c2 = c1.mk_not().mk_or(&c2.mk_not(), &context);
        assert_eq!(not_c1_and_c2, not_c1_or_not_c2);

        // NOT (NOT c1) == c1
        let not_not_c1 = c1.mk_not().mk_not();
        assert_eq!(not_not_c1, c1);
    }
}
