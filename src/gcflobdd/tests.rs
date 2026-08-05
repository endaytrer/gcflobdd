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
            Grammar::new_bdd(4),
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
fn test_mk_op_pair_map() {
    for grammar in grammar_choice!() {
        let context = RefCell::new(Context::default());
        let c1 = Gcflobdd::mk_projection(0, &grammar, &context);
        let c2 = Gcflobdd::mk_projection(1, &grammar, &context);

        let c3_standard = c1.mk_op(&c2, |a, b| a & b, &context);
        let c3_pair_map = c1.mk_op_pair_map(&c2, |a, b| a & b, &context);

        assert_eq!(c3_standard, c3_pair_map);

        let c_or_standard = c1.mk_op(&c2, |a, b| a | b, &context);
        let c_or_pair_map = c1.mk_op_pair_map(&c2, |a, b| a | b, &context);

        assert_eq!(c_or_standard, c_or_pair_map);

        let c_xor_standard = c1.mk_op(&c2, |a, b| a ^ b, &context);
        let c_xor_pair_map = c1.mk_op_pair_map(&c2, |a, b| a ^ b, &context);

        assert_eq!(c_xor_standard, c_xor_pair_map);
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
fn test_xor() {
    for grammar in grammar_choice!() {
        let context = RefCell::new(Context::default());
        let c1 = Gcflobdd::mk_projection(0, &grammar, &context);
        let c2 = Gcflobdd::mk_projection(1, &grammar, &context);
        let c3 = c1.mk_xor(&c2, &context);
        drop(c1);
        drop(c2);
        context.borrow_mut().gc();
        let path = c3.find_one_satisfiable_assignment().unwrap();
        assert_eq!(path, vec![Some(false), Some(true), None, None]);
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

#[test]
fn test_node_table_failed() {
    let grammar = Grammar::new(&["S2 -> S1 S1".to_string(), "S1 -> a a".to_string()]).unwrap();
    let context = RefCell::new(Context::default());
    let c1 = Gcflobdd::mk_projection(0, &grammar, &context);
    let c0 = Gcflobdd::mk_false(&grammar, &context);
    let c1_and_not_c1 = c1.mk_and(&c1.mk_not(), &context);
    assert_eq!(c1_and_not_c1, c0);
}

#[test]
fn test_node_table_failed_bdd() {
    let grammar = Grammar::new_bdd(2);
    let context = RefCell::new(Context::default());
    let c1 = Gcflobdd::mk_projection(0, &grammar, &context);
    let c0 = Gcflobdd::mk_false(&grammar, &context);
    let c1_and_not_c1 = c1.mk_and(&c1.mk_not(), &context);
    assert_eq!(c1_and_not_c1, c0);
}
#[test]
fn test_n_queen_failed() {
    let grammar = Grammar::new(&["S2 -> S1 S1".to_string(), "S1 -> a a".to_string()]).unwrap();
    let context = RefCell::new(Context::default());
    let c2 = Gcflobdd::mk_projection(2, &grammar, &context)
        .mk_or(&Gcflobdd::mk_projection(3, &grammar, &context), &context);
    assert_ne!(c2, Gcflobdd::mk_false(&grammar, &context));
}

// ---------------------------------------------------------------------------
// Oracle-based tests for sat_count / evaluate.
//
// A boolean function over `n` variables is represented as a truth table
// (`Vec<bool>` of length `2^n`, indexed so that bit `i` of the index is the
// value of variable `i`). We build the equivalent GCFLOBDD from minterms using
// only the primitive operators, then check the new operations against the table.
// ---------------------------------------------------------------------------

/// Deterministic xorshift PRNG so the tests are reproducible.
fn prng(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// The assignment (indexed by variable) encoded by integer `a`.
fn assignment_of(a: usize, n: usize) -> Vec<bool> {
    (0..n).map(|i| (a >> i) & 1 == 1).collect()
}

/// Build the GCFLOBDD for a truth table using only primitive operators.
fn build_from_table<'g>(
    table: &[bool],
    n: usize,
    grammar: &'g Grammar,
    context: &RefCell<Context<'g>>,
) -> Gcflobdd<'g> {
    let mut acc = Gcflobdd::mk_false(grammar, context);
    for (a, &val) in table.iter().enumerate() {
        if !val {
            continue;
        }
        let mut term = Gcflobdd::mk_true(grammar, context);
        for i in 0..n {
            let proj = Gcflobdd::mk_projection(i, grammar, context);
            let lit = if (a >> i) & 1 == 1 {
                proj
            } else {
                proj.mk_not()
            };
            term = term.mk_and(&lit, context);
        }
        acc = acc.mk_or(&term, context);
    }
    acc
}

/// (grammar, n) pairs exercising every leaf/grouping shape.
fn oracle_grammars() -> Vec<(Grammar, usize)> {
    vec![
        // n = 4
        (
            Grammar::new(&["S2 -> S1 S1".to_string(), "S1 -> a a".to_string()]).unwrap(),
            4,
        ),
        (Grammar::new(&["S2 -> BDD(4)".to_string()]).unwrap(), 4),
        (
            Grammar::new(&["S2 -> BDD(2) BDD(2)".to_string()]).unwrap(),
            4,
        ),
        (Grammar::new_bdd(4), 4),
        // n = 5 (odd, forces uneven aligned splits)
        (
            Grammar::new(&["S -> A a".to_string(), "A -> a a a a".to_string()]).unwrap(),
            5,
        ),
        (
            Grammar::new(&["S -> BDD(2) BDD(3)".to_string()]).unwrap(),
            5,
        ),
        // n = 6
        (
            Grammar::new(&["S2 -> S1 S1".to_string(), "S1 -> a a a".to_string()]).unwrap(),
            6,
        ),
        (Grammar::new(&["S -> BDD(4) a a".to_string()]).unwrap(), 6),
        // n = 8: two field-like BDD leaves + a balanced tree
        (
            Grammar::new(&["S -> BDD(4) BDD(4)".to_string()]).unwrap(),
            8,
        ),
        (
            Grammar::new(&[
                "S3 -> S2 S2".to_string(),
                "S2 -> S1 S1".to_string(),
                "S1 -> a a".to_string(),
            ])
            .unwrap(),
            8,
        ),
    ]
}

#[test]
fn test_evaluate_matches_primitives() {
    // `evaluate` must agree with the value implied by the minterm construction.
    let mut state = 0x1234_5678_9abc_def0u64;
    for (grammar, n) in oracle_grammars() {
        assert_eq!(grammar.root.num_vars, n);
        let context = RefCell::new(Context::default());
        for _ in 0..4 {
            let table: Vec<bool> = (0..(1usize << n))
                .map(|_| prng(&mut state) & 1 == 0)
                .collect();
            let f = build_from_table(&table, n, &grammar, &context);
            for (a, &val) in table.iter().enumerate() {
                assert_eq!(f.evaluate(&assignment_of(a, n)), val, "n={n}, a={a}");
            }
        }
    }
}

/// `values()` must list exactly `expected`, and `find_one_path_to_index` must
/// hand back an assignment producing each one -- with the positions it leaves
/// unconstrained genuinely free.
fn check_values_and_paths<T: Clone + PartialEq + Ord + std::fmt::Debug>(
    f: &GcflobddT<'_, T>,
    expected: &[T],
    n: usize,
) {
    let mut listed = f.values().to_vec();
    listed.sort_unstable();
    assert_eq!(listed, expected, "n={n}");

    for (index, value) in f.values().iter().enumerate() {
        let path = f.find_one_path_to_index(index);
        assert_eq!(path.len(), n);
        let mut assignment: Vec<bool> = path.iter().map(|b| b.unwrap_or(false)).collect();
        assert!(f.evaluate(&assignment) == *value, "n={n} index={index}");
        for (variable, bit) in path.iter().enumerate() {
            if bit.is_some() {
                continue;
            }
            assignment[variable] = !assignment[variable];
            assert!(
                f.evaluate(&assignment) == *value,
                "n={n} variable {variable} was reported free but is not"
            );
            assignment[variable] = !assignment[variable];
        }
    }
}

#[test]
fn test_values_and_find_one_path_to_index() {
    let mut state = 0x5eed_1234_abcd_0f0fu64;
    for (grammar, n) in oracle_grammars() {
        let context = RefCell::new(Context::default());
        for _ in 0..3 {
            let table: Vec<bool> = (0..(1usize << n))
                .map(|_| prng(&mut state) % 3 == 0)
                .collect();
            let mut distinct = table.clone();
            distinct.sort_unstable();
            distinct.dedup();
            // Every grouping shape, but only two values: `build_from_table`
            // goes through the primitive operators, which BDD leaves support.
            let f = build_from_table(&table, n, &grammar, &context);
            check_values_and_paths(&f, &distinct, n);

            // Many values, which needs `from_table` and so an all-`a` grammar.
            if n == 4 {
                let table: Vec<i32> = (0..16).map(|_| (prng(&mut state) % 5) as i32).collect();
                let mut distinct = table.clone();
                distinct.sort_unstable();
                distinct.dedup();
                let grammar =
                    Grammar::new(&["S2 -> S1 S1".to_string(), "S1 -> a a".to_string()]).unwrap();
                let context = RefCell::new(Context::default());
                let f = GcflobddT::from_table(&table, &grammar, &context);
                check_values_and_paths(&f, &distinct, n);
            }
        }
    }
}

#[test]
fn test_sat_count_matches_popcount() {
    let mut state = 0xdead_beef_cafe_babeu64;
    for (grammar, n) in oracle_grammars() {
        let context = RefCell::new(Context::default());
        for _ in 0..4 {
            let table: Vec<bool> = (0..(1usize << n))
                .map(|_| prng(&mut state) & 3 == 0)
                .collect();
            let popcount = table.iter().filter(|&&x| x).count();
            let f = build_from_table(&table, n, &grammar, &context);
            let log2 = f.sat_count();
            if popcount == 0 {
                assert_eq!(log2, f64::NEG_INFINITY);
            } else {
                let count = log2.exp2().round() as usize;
                assert_eq!(count, popcount, "n={n}, log2={log2}");
            }
        }
    }
    // Boundary cases: constants.
    for (grammar, n) in oracle_grammars() {
        let context = RefCell::new(Context::default());
        assert_eq!(
            Gcflobdd::mk_false(&grammar, &context).sat_count(),
            f64::NEG_INFINITY
        );
        let full = Gcflobdd::mk_true(&grammar, &context).sat_count();
        assert_eq!(full.exp2().round() as usize, 1usize << n);
    }
}
