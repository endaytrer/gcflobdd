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

fn gen_aligned_balanced_grammar_recursive(
    num_exits: usize,
    symbol_prefix: &str,
    terminal_symbol: &str,
) -> Vec<(usize, String)> {
    if num_exits == 1 {
        panic!("num_exits must be greater than 1");
    }
    if num_exits == 2 {
        return vec![(2, format!("{} {}", terminal_symbol, terminal_symbol))];
    }
    if num_exits == 3 {
        return [
            vec![(3, format!("{}2 {}", symbol_prefix, terminal_symbol))],
            gen_aligned_balanced_grammar_recursive(2, symbol_prefix, terminal_symbol),
        ]
        .concat();
    }
    let num_a_vars = num_exits / 2 + num_exits % 2;
    let num_b_vars = num_exits / 2;
    if num_a_vars == num_b_vars {
        let mut productions =
            gen_aligned_balanced_grammar_recursive(num_a_vars, symbol_prefix, terminal_symbol);
        productions.insert(
            0,
            (
                num_exits,
                format!(
                    "{}{} {}{}",
                    symbol_prefix, productions[0].0, symbol_prefix, productions[0].0
                ),
            ),
        );
        return productions;
    }
    let a_productions =
        gen_aligned_balanced_grammar_recursive(num_a_vars, symbol_prefix, terminal_symbol);
    let b_productions =
        gen_aligned_balanced_grammar_recursive(num_b_vars, symbol_prefix, terminal_symbol);
    let mut symbols_map = vec![None; num_a_vars + 1];
    for (sz, rule) in b_productions {
        symbols_map[sz].get_or_insert(rule);
    }
    for (sz, rule) in a_productions {
        symbols_map[sz].get_or_insert(rule);
    }
    symbols_map
        .into_iter()
        .enumerate()
        .filter_map(|(sz, x)| x.map(|x| (sz, x)))
        .chain(std::iter::once((
            num_exits,
            format!(
                "{}{} {}{}",
                symbol_prefix, num_a_vars, symbol_prefix, num_b_vars
            ),
        )))
        .rev()
        .collect()
}

fn gen_aligned_balanced_grammar(
    num_exits: usize,
    symbol_prefix: &str,
    terminal_symbol: &str,
) -> Vec<String> {
    gen_aligned_balanced_grammar_recursive(num_exits, symbol_prefix, terminal_symbol)
        .into_iter()
        .map(|(sz, rule)| format!("{}{} -> {}", symbol_prefix, sz, rule))
        .collect()
}

fn size_to_readable(size: usize) -> String {
    let mut size = size as f64;
    let mut unit = 0;
    let prefixes = vec!["B", "KiB", "MiB", "GiB"];
    while size >= 1024.0 {
        size /= 1024.0;
        unit += 1;
    }
    format!("{:.2}{}", size, prefixes[unit])
}

fn main() {
    let mut n = 8;
    enum GrammarChoice {
        Default,
        Balanced,
        AlignedBalanced,
        FullAlignedBalanced,
        Ndd,
        Bdd,
    }
    let mut grammar_choice = GrammarChoice::Default;

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
                        "balanced" => grammar_choice = GrammarChoice::Balanced,
                        "aligned-balanced" => grammar_choice = GrammarChoice::AlignedBalanced,
                        "full-aligned-balanced" => {
                            grammar_choice = GrammarChoice::FullAlignedBalanced
                        }
                        "default" => grammar_choice = GrammarChoice::Default,
                        "ndd" => grammar_choice = GrammarChoice::Ndd,
                        "bdd" => grammar_choice = GrammarChoice::Bdd,
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
        match grammar_choice {
            GrammarChoice::Balanced => "balanced",
            GrammarChoice::AlignedBalanced => "align-balanced",
            GrammarChoice::FullAlignedBalanced => "full-align-balanced",
            GrammarChoice::Default => "default",
            GrammarChoice::Ndd => "ndd",
            GrammarChoice::Bdd => "bdd",
        }
    );

    let grammar = match grammar_choice {
        GrammarChoice::Balanced => {
            let l = (2.0 * (n as f64).log2()).ceil() as usize;
            let rules = gen_balanced_grammar(l);
            Grammar::new(&rules).unwrap()
        }
        GrammarChoice::AlignedBalanced => {
            let rules_a = gen_aligned_balanced_grammar(n, "S", "a");
            let rules_b = gen_aligned_balanced_grammar(n, "G", format!("S{}", n).as_str());
            Grammar::new(&[rules_b, rules_a].concat()).unwrap()
        }
        GrammarChoice::FullAlignedBalanced => {
            let rules = gen_aligned_balanced_grammar(n * n, "S", "a");
            Grammar::new(&rules).unwrap()
        }
        GrammarChoice::Default => {
            let s2 = vec!["S1"; n].join(" ");
            let s2_gen_rule = format!("S2 -> {}", s2);

            let s1 = vec!["a"; n].join(" ");
            let s1_gen_rule = format!("S1 -> {}", s1);
            Grammar::new(&[s2_gen_rule, s1_gen_rule]).unwrap()
        }
        GrammarChoice::Ndd => {
            let s1 = vec![format!("BDD({})", n); n].join(" ");
            let s1_gen_rule = format!("S1 -> {}", s1);
            Grammar::new(&[s1_gen_rule]).unwrap()
        }
        GrammarChoice::Bdd => Grammar::new_bdd(n * n),
    };

    let context = RefCell::new(Context::default());

    let start_time = std::time::Instant::now();
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
    let end_time = std::time::Instant::now();
    println!(
        "Solved in {} ms, with {} memory usage",
        end_time.duration_since(start_time).as_millis(),
        size_to_readable(context.borrow().size_estimate())
    );
    println!("Path:");
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
