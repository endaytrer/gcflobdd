use super::*;

#[test]
fn test_mk_projection_true_false() {
    let grammar = Grammar::new(&["S2 -> S1 S1".to_string(), "S1 -> a a".to_string()]).unwrap();
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
    assert_eq!(context.borrow().node_count(), 6); // 1 * S3 + 2 * S2 + 2 * S1 + fork + don't care
    let c2 = Gcflobdd::mk_projection(1, &grammar, &context);
    assert_eq!(context.borrow().node_count(), 8); // 1 * S3 + 1 * S2
    drop(c2);
    context.borrow_mut().gc();
    assert_eq!(context.borrow().node_count(), 6);
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
    let path = c3.find_one_path_to(&(true, true)).unwrap();
    assert_eq!(path, vec![Some(true), Some(true), None, None]);
}

#[test]
fn test_op() {
    let grammar = Grammar::new(&["S2 -> S1 S1".to_string(), "S1 -> a a".to_string()]).unwrap();
    let context = Context::new();
    let c1 = Gcflobdd::mk_projection(0, &grammar, &context);
    let c2 = Gcflobdd::mk_projection(1, &grammar, &context);
    let c3 = c1.mk_op(&c2, |a, b| a & b, &context);
    drop(c1);
    drop(c2);
    context.borrow_mut().gc();
    let path = c3.find_one_satisfiable_assignment().unwrap();
    assert_eq!(path, vec![Some(true), Some(true), None, None]);
}

fn _gen_balanced_grammar(level: usize) -> Vec<String> {
    if level == 0 {
        return vec!["S0 -> a".to_string()];
    }
    let mut grammar = vec![];
    for i in (1..level).rev() {
        grammar.push(format!("S{} -> S{} S{}", i, i - 1, i - 1));
    }
    grammar.push("S0 -> a a".to_string());
    grammar
}

#[test]
fn test_nqueens() {
    let n = 12;
    // let l = (2.0 * (n as f64).log2()).ceil() as usize;
    // let rules = _gen_balanced_grammar(l);
    // let grammar = Grammar::new(&rules).unwrap();
    // assert_eq!(grammar.root.num_vars, 128);
    let s2 = vec!["S1"; n].join(" ");
    let s2_gen_rule = format!("S2 -> {}", s2);

    let s1 = vec!["a"; n].join(" ");
    let s1_gen_rule = format!("S1 -> {}", s1);
    let grammar = Grammar::new(&[s2_gen_rule, s1_gen_rule]).unwrap();

    let context = RefCell::new(Context::default());

    let mut vars = Vec::new();
    for i in 0..n {
        let mut row = Vec::new();
        for j in 0..n {
            row.push(Gcflobdd::mk_projection(i * n + j, &grammar, &context));
        }
        vars.push(row);
    }

    let mut or_batch = Vec::new();
    for i in 0..n {
        let mut condition = Gcflobdd::mk_false(&grammar, &context);
        for j in 0..n {
            condition = condition.mk_or(&vars[i][j], &context);
        }
        or_batch.push(condition);
    }

    let mut imp_batch = Vec::new();
    for i in 0..n {
        let mut row = Vec::new();
        for j in 0..n {
            let mut a = Gcflobdd::mk_true(&grammar, &context);
            let mut b = Gcflobdd::mk_true(&grammar, &context);
            let mut c = Gcflobdd::mk_true(&grammar, &context);
            let mut d = Gcflobdd::mk_true(&grammar, &context);

            /* No one in the same column */
            for l in 0..n {
                if l != j {
                    let mp = vars[i][j].mk_implies(&vars[i][l].mk_not(), &context);
                    a = a.mk_and(&mp, &context);
                }
            }

            /* No one in the same row */
            for k in 0..n {
                if k != i {
                    let mp = vars[i][j].mk_implies(&vars[k][j].mk_not(), &context);
                    b = b.mk_and(&mp, &context);
                }
            }

            /* No one in the same up-right diagonal */
            for k in 0..n {
                let ll = k as i32 - i as i32 + j as i32;
                if ll >= 0 && ll < n as i32 {
                    let ll = ll as usize;
                    if k != i {
                        let mp = vars[i][j].mk_implies(&vars[k][ll].mk_not(), &context);
                        c = c.mk_and(&mp, &context);
                    }
                }
            }

            /* No one in the same down-right diagonal */
            for k in 0..n {
                let ll = i as i32 + j as i32 - k as i32;
                if ll >= 0 && ll < n as i32 {
                    let ll = ll as usize;
                    if k != i {
                        let mp = vars[i][j].mk_implies(&vars[k][ll].mk_not(), &context);
                        d = d.mk_and(&mp, &context);
                    }
                }
            }

            c = c.mk_and(&d, &context);
            b = b.mk_and(&c, &context);
            a = a.mk_and(&b, &context);
            row.push(a);
        }
        imp_batch.push(row);
    }

    let mut queen = Gcflobdd::mk_true(&grammar, &context);

    for i in 0..n {
        println!("Combining OR condition for row {} / {}", i, n);
        queen = queen.mk_and(&or_batch[i], &context);
    }

    for i in 0..n {
        let mut tmp_queen = Gcflobdd::mk_true(&grammar, &context);
        for j in 0..n {
            println!(
                "Combining implication condition for position ({}, {}) / {}",
                i, j, n
            );
            tmp_queen = tmp_queen.mk_and(&imp_batch[i][j], &context);
        }
        queen = queen.mk_and(&tmp_queen, &context);
    }

    let path = queen.find_one_satisfiable_assignment().unwrap();
    for i in 0..n {
        for j in 0..n {
            print!(
                "{}",
                if (path[i * n + j]) == Some(true) {
                    "Q"
                } else {
                    "."
                }
            );
        }
        println!();
    }
}
