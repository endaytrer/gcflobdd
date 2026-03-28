use gcflobdd::gcflobdd::Gcflobdd;
use gcflobdd::gcflobdd::context::Context;
use gcflobdd::grammar::Grammar;
use std::cell::RefCell;

fn gen_balanced_grammar(level: usize) -> Vec<String> {
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

fn main() {
    let mut n = 8;
    let mut use_balanced = false;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--n" => {
                if i + 1 < args.len() {
                    n = args[i + 1].parse().unwrap();
                    i += 1;
                }
            }
            "--grammar" => {
                if i + 1 < args.len() {
                    match args[i + 1].as_str() {
                        "balanced" => use_balanced = true,
                        "default" => use_balanced = false,
                        _ => panic!("Unknown grammar choice: {}", args[i + 1]),
                    }
                    i += 1;
                }
            }
            _ => {
                println!(
                    "Usage: {} [--n <N>] [--grammar <balanced|default>]",
                    args[0]
                );
                return;
            }
        }
        i += 1;
    }

    println!(
        "Running nqueens with n={}, grammar={}",
        n,
        if use_balanced { "balanced" } else { "default" }
    );

    let grammar = if use_balanced {
        let l = (2.0 * (n as f64).log2()).ceil() as usize;
        let rules = gen_balanced_grammar(l);
        Grammar::new(&rules).unwrap()
    } else {
        let s2 = vec!["S1"; n].join(" ");
        let s2_gen_rule = format!("S2 -> {}", s2);

        let s1 = vec!["a"; n].join(" ");
        let s1_gen_rule = format!("S1 -> {}", s1);
        Grammar::new(&[s2_gen_rule, s1_gen_rule]).unwrap()
    };

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
