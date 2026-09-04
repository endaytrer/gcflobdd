//! Why the same engine is fast on one workload and slow on another.
//!
//! Per-operation *time* says the constant is large; it does not say whether that
//! is because each recursive step is expensive or because there are many of
//! them. This runs several function families through the identical boolean code
//! path and reports, for each, the recursive calls per top-level operation, the
//! memo-hit rate, the nodes interned, and the resulting diagram size.
//!
//!     cargo run --release --features opcount --test workprofile
//!
//! Without `--features opcount` the counter columns read zero; the sizes and
//! times are still real.

use std::cell::RefCell;
use std::time::Instant;

use gcflobdd::gcflobdd::Gcflobdd;
use gcflobdd::gcflobdd::context::Context;
use gcflobdd::grammar::Grammar;
use gcflobdd::utils::opcount::{self, NAMES, N};

/// The 104-bit five-tuple, aligned-balanced with one shared subtree per width --
/// the grammar `NETWORK.md` measures.
fn five_tuple_grammar() -> Grammar {
    Grammar::new(&[
        "S -> A B".to_string(),
        "A -> W32 W32".to_string(),
        "B -> W16 C".to_string(),
        "C -> W16 W8".to_string(),
        "W32 -> W16 W16".to_string(),
        "W16 -> W8 W8".to_string(),
        "W8 -> W4 W4".to_string(),
        "W4 -> W2 W2".to_string(),
        "W2 -> a a".to_string(),
    ])
    .expect("well-formed")
}

/// A perfectly balanced grammar over `2^level` variables: one symbol per level,
/// so every subtree of a given width is the *same* grammar node. This is the
/// most sharing a grammar can offer; whether the diagram takes it up is then
/// entirely a property of the function.
fn balanced_grammar(level: usize) -> Grammar {
    let mut rules = vec![];
    for i in (1..level).rev() {
        rules.push(format!("S{} -> S{} S{}", i, i - 1, i - 1));
    }
    rules.push("S0 -> a a".to_string());
    Grammar::new(&rules).expect("well-formed")
}

struct Report {
    label: String,
    vars: usize,
    top_ops: usize,
    ms: f64,
    nodes: usize,
    edges: usize,
    counters: [u64; N],
}

fn run<'g, F>(label: &str, grammar: &'g Grammar, body: F) -> Report
where
    F: FnOnce(&'g Grammar, &[Gcflobdd<'g>], &RefCell<Context<'g>>) -> (usize, (usize, usize)),
{
    let context = Context::new();
    let vars: Vec<Gcflobdd> = (0..grammar.num_vars())
        .map(|i| Gcflobdd::mk_projection(i, grammar, &context))
        .collect();
    opcount::reset();
    let t0 = Instant::now();
    let (top_ops, (nodes, edges)) = body(grammar, &vars, &context);
    let ms = t0.elapsed().as_secs_f64() * 1e3;
    Report {
        label: label.to_string(),
        vars: grammar.num_vars(),
        top_ops,
        ms,
        nodes,
        edges,
        counters: opcount::snapshot(),
    }
}

// ---------------------------------------------------------------- workloads

/// Network shape: IP prefixes, i.e. conjunctions of the *top* bits of a field,
/// then Yang and Lam's atomic-predicate split over them. Copied from
/// `opbench.rs` so the two measure the same thing.
fn prefix_predicates<'g>(
    vars: &[Gcflobdd<'g>],
    context: &RefCell<Context<'g>>,
    n: usize,
) -> Vec<Gcflobdd<'g>> {
    const FIELDS: [(usize, usize); 2] = [(0, 32), (32, 32)];
    let mut out = Vec::with_capacity(n);
    let mut seed = 0x9e3779b97f4a7c15u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        seed >> 33
    };
    for _ in 0..n {
        let (base, width) = FIELDS[next() as usize % FIELDS.len()];
        let len = 8 + next() as usize % 17;
        let value = next();
        let mut acc: Option<Gcflobdd> = None;
        for bit in 0..len.min(width) {
            let one = (value >> (len - 1 - bit)) & 1 == 1;
            let lit = if one { vars[base + bit].clone() } else { vars[base + bit].mk_not() };
            acc = Some(match acc {
                None => lit,
                Some(a) => a.mk_and(&lit, context),
            });
        }
        out.push(acc.unwrap());
    }
    out
}

fn atomic_predicates<'g>(
    predicates: &[Gcflobdd<'g>],
    context: &RefCell<Context<'g>>,
) -> (usize, usize, Gcflobdd<'g>) {
    let any = &predicates[0];
    let everything = any.mk_or(&any.mk_not(), context);
    let empty = any.mk_and(&any.mk_not(), context);
    let mut atoms: Vec<Gcflobdd> = vec![everything];
    let mut ops = 0usize;
    for p in predicates {
        let not_p = p.mk_not();
        let n = atoms.len();
        for i in 0..n {
            let a = atoms[i].clone();
            let inside = a.mk_and(p, context);
            ops += 1;
            if inside == a || inside == empty {
                continue;
            }
            let outside = a.mk_and(&not_p, context);
            ops += 1;
            atoms[i] = inside;
            if outside != empty {
                atoms.push(outside);
            }
        }
    }
    let last = atoms.last().unwrap().clone();
    (atoms.len(), ops, last)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let bits: usize = args
        .iter()
        .position(|a| a == "--bits")
        .map(|i| args[i + 1].parse().unwrap())
        .unwrap_or(7); // 2^7 = 128 variables, near the five-tuple's 104
    // How many predicates the network workload splits over. The atom count, and
    // so the size of the diagrams the loop conjoins, grows steeply with this;
    // 48 is a working set of a few hundred atoms, 200 is tens of thousands --
    // the scale `bench/netverify` runs at.
    let preds: usize = args
        .iter()
        .position(|a| a == "--preds")
        .map(|i| args[i + 1].parse().unwrap())
        .unwrap_or(48);

    let five = five_tuple_grammar();
    let bal = balanced_grammar(bits);
    let n = bal.num_vars();

    let mut reports = vec![];

    // --- the network workload, verbatim -----------------------------------
    reports.push(run("net: atomic predicates", &five, move |_g, vars, ctx| {
        let preds = prefix_predicates(vars, ctx, preds);
        let (_atoms, ops, last) = atomic_predicates(&preds, ctx);
        (ops, last.count_nodes_and_edges())
    }));

    // A single network predicate, built and left alone: how big is one prefix?
    reports.push(run("net: one /24 prefix", &five, |_g, vars, ctx| {
        let mut acc = vars[0].clone();
        for bit in 1..24 {
            acc = acc.mk_and(&vars[bit], ctx);
        }
        (23, acc.count_nodes_and_edges())
    }));

    // --- self-similar families, same engine -------------------------------
    // Parity: p(x_0..x_{2m-1}) = p(first half) XOR p(second half). The recursion
    // is on the function itself, so one grouping per level serves the whole
    // tree -- the property a CFLOBDD is built to exploit.
    reports.push(run("self-similar: parity", &bal, |_g, vars, ctx| {
        let mut acc = vars[0].clone();
        for v in &vars[1..] {
            acc = acc.mk_xor(v, ctx);
        }
        (vars.len() - 1, acc.count_nodes_and_edges())
    }));

    // All-equal, the support of a GHZ state: equal(whole) = equal(left) AND
    // equal(right) AND (they agree). Self-similar in the same way.
    reports.push(run("self-similar: all bits equal", &bal, |g, vars, ctx| {
        let mut acc = Gcflobdd::mk_true(g, ctx);
        let mut ops = 0;
        for v in &vars[1..] {
            let same = vars[0].mk_xor(v, ctx).mk_not();
            acc = acc.mk_and(&same, ctx);
            ops += 2;
        }
        (ops, acc.count_nodes_and_edges())
    }));

    // Conjunction of every variable: trivially self-similar, the easy case.
    reports.push(run("self-similar: all ones", &bal, |_g, vars, ctx| {
        let mut acc = vars[0].clone();
        for v in &vars[1..] {
            acc = acc.mk_and(v, ctx);
        }
        (vars.len() - 1, acc.count_nodes_and_edges())
    }));

    // --- control: the same op count on structure-free functions -----------
    // A random DNF over all n variables. Nothing recurs, so nothing shares.
    reports.push(run("no structure: random DNF", &bal, |g, vars, ctx| {
        let mut seed = 0x243f6a8885a308d3u64;
        let mut next = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            seed >> 33
        };
        let mut acc = Gcflobdd::mk_false(g, ctx);
        let mut ops = 0;
        for _ in 0..24 {
            let mut term: Option<Gcflobdd> = None;
            for _ in 0..6 {
                let v = &vars[next() as usize % vars.len()];
                let lit = if next() & 1 == 0 { v.clone() } else { v.mk_not() };
                term = Some(match term {
                    None => lit,
                    Some(t) => { ops += 1; t.mk_and(&lit, ctx) }
                });
            }
            acc = acc.mk_or(&term.unwrap(), ctx);
            ops += 1;
        }
        (ops, acc.count_nodes_and_edges())
    }));

    // ------------------------------------------------------------- report
    println!("variables: five-tuple 104, balanced 2^{bits} = {n}\n");
    println!(
        "{:<30} {:>5} {:>10} {:>10} {:>8} {:>9} {:>9} {:>7}",
        "workload", "vars", "top ops", "ms", "nodes", "calls/op", "hits/op", "hit %"
    );
    for r in &reports {
        let calls: u64 = r.counters[0] + r.counters[2] + r.counters[4]
            + r.counters[6] + r.counters[8] + r.counters[10];
        let hits: u64 = r.counters[1] + r.counters[3] + r.counters[5]
            + r.counters[7] + r.counters[9] + r.counters[11];
        let t = r.top_ops.max(1) as f64;
        println!(
            "{:<30} {:>5} {:>10} {:>10.1} {:>8} {:>9.1} {:>9.1} {:>6.1}%",
            r.label, r.vars, r.top_ops, r.ms, r.nodes,
            calls as f64 / t, hits as f64 / t,
            if calls > 0 { 100.0 * hits as f64 / calls as f64 } else { 0.0 },
        );
    }

    println!("\nraw counters (per top-level operation)");
    print!("{:<30}", "workload");
    for name in NAMES.iter() {
        print!(" {:>13}", name.rsplit('.').next().unwrap_or(name));
    }
    println!();
    for r in &reports {
        print!("{:<30}", r.label);
        let t = r.top_ops.max(1) as f64;
        for c in r.counters.iter() {
            print!(" {:>13.2}", *c as f64 / t);
        }
        println!();
    }
    println!("\n(counter names, in order: {})", NAMES.join(", "));
    for r in &reports {
        println!("{:<30} edges {}", r.label, r.edges);
    }
}
