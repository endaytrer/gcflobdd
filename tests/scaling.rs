//! Where the compression comes from, and where it does not.
//!
//! Both columns run in *this* engine: `S -> BDD(n)` is a grammar with a single
//! BDD leaf, so it is an ordinary BDD sharing this crate's interning and caches,
//! while the balanced grammar is a full CFLOBDD over the same variables in the
//! same order. Any difference between the two columns is the representation,
//! not the implementation.
//!
//! Only the CFLOBDD column reports a size. `count_nodes_and_edges` counts
//! grammar-level nodes and does not descend into a `BDD` leaf, so it would
//! report 2 for every BDD row whatever the BDD underneath was doing -- see the
//! same caveat on `field-grouped` in `NETWORK.md`. Time is measured for both.
//!
//!     cargo test --release --features opcount --test scaling
//!
//! Functions are chosen for one property: whether `f` over 2m variables
//! decomposes into the *same* function over m. Parity and all-bits-equal do;
//! an IP prefix does not.

use std::cell::RefCell;
use std::time::Instant;

use gcflobdd::gcflobdd::Gcflobdd;
use gcflobdd::gcflobdd::context::Context;
use gcflobdd::grammar::Grammar;
use gcflobdd::utils::opcount;

fn balanced_grammar(level: usize) -> Grammar {
    let mut rules = vec![];
    for i in (1..level).rev() {
        rules.push(format!("S{} -> S{} S{}", i, i - 1, i - 1));
    }
    rules.push("S0 -> a a".to_string());
    Grammar::new(&rules).expect("well-formed")
}

fn bdd_grammar(n: usize) -> Grammar {
    Grammar::new(&[format!("S -> BDD({n})")]).expect("well-formed")
}

/// XOR of every variable. `parity(x_0..x_{2m-1}) = parity(left) XOR parity(right)`
/// -- the halves are the *same function*, so one grouping per level can serve
/// the whole tree.
fn parity<'g>(vars: &[Gcflobdd<'g>], ctx: &RefCell<Context<'g>>) -> (Gcflobdd<'g>, usize) {
    let mut acc = vars[0].clone();
    for v in &vars[1..] {
        acc = acc.mk_xor(v, ctx);
    }
    (acc, vars.len() - 1)
}

/// All bits equal -- the support of a GHZ state. Also self-similar: both halves
/// must be internally equal and must agree.
fn all_equal<'g>(vars: &[Gcflobdd<'g>], ctx: &RefCell<Context<'g>>) -> (Gcflobdd<'g>, usize) {
    let mut acc = vars[0].mk_xor(&vars[1], ctx).mk_not();
    for v in &vars[2..] {
        acc = acc.mk_and(&vars[0].mk_xor(v, ctx).mk_not(), ctx);
    }
    (acc, 2 * (vars.len() - 1))
}

/// An address prefix: the top half of the variables pinned to a fixed pattern,
/// the rest free. The two halves are *different* functions -- the first is a
/// chain of literals, the second is `true` -- so there is nothing for a level to
/// share. This is the shape a FIB entry and an ACL field match have.
fn prefix<'g>(vars: &[Gcflobdd<'g>], ctx: &RefCell<Context<'g>>) -> (Gcflobdd<'g>, usize) {
    let len = vars.len() / 2;
    let mut acc = vars[0].clone();
    for (i, v) in vars.iter().enumerate().take(len).skip(1) {
        let lit = if i % 3 == 0 { v.mk_not() } else { v.clone() };
        acc = acc.mk_and(&lit, ctx);
    }
    (acc, len - 1)
}

/// Twenty such prefixes unioned, the shape of a forwarding predicate.
fn prefix_union<'g>(vars: &[Gcflobdd<'g>], ctx: &RefCell<Context<'g>>) -> (Gcflobdd<'g>, usize) {
    let width = vars.len();
    let mut seed = 0x9e3779b97f4a7c15u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        seed >> 33
    };
    let mut acc = Gcflobdd::mk_false(vars[0].grammar_ref(), ctx);
    let mut ops = 0;
    for _ in 0..20 {
        let len = (width / 4).max(1) + next() as usize % (width / 4).max(1);
        let value = next();
        let mut term = if value & 1 == 1 { vars[0].clone() } else { vars[0].mk_not() };
        for bit in 1..len.min(width) {
            let one = (value >> (bit % 32)) & 1 == 1;
            let lit = if one { vars[bit].clone() } else { vars[bit].mk_not() };
            term = term.mk_and(&lit, ctx);
            ops += 1;
        }
        acc = acc.mk_or(&term, ctx);
        ops += 1;
    }
    (acc, ops)
}

type Fn2 = for<'g> fn(&[Gcflobdd<'g>], &RefCell<Context<'g>>) -> (Gcflobdd<'g>, usize);

fn measure(grammar: &Grammar, f: Fn2) -> (usize, usize, f64, f64) {
    let context = Context::new();
    let vars: Vec<Gcflobdd> = (0..grammar.num_vars())
        .map(|i| Gcflobdd::mk_projection(i, grammar, &context))
        .collect();
    opcount::reset();
    let t0 = Instant::now();
    let (d, ops) = f(&vars, &context);
    let ms = t0.elapsed().as_secs_f64() * 1e3;
    let (nodes, _edges) = d.count_nodes_and_edges();
    let c = opcount::snapshot();
    let calls: u64 = c[0] + c[2] + c[4] + c[6] + c[8] + c[10];
    (nodes, ops, ms, calls as f64 / ops.max(1) as f64)
}

fn main() {
    let workloads: [(&str, Fn2, bool); 4] = [
        ("parity (self-similar)", parity, true),
        ("all bits equal (self-similar)", all_equal, true),
        ("one address prefix", prefix, false),
        ("20 prefixes unioned", prefix_union, false),
    ];

    for (name, f, self_similar) in workloads {
        println!(
            "\n{name}   {}",
            if self_similar {
                "-- f over 2m vars decomposes into f over m"
            } else {
                "-- the two halves are different functions"
            }
        );
        println!(
            "{:>8} {:>14} {:>12} {:>12} {:>8} {:>10}",
            "vars", "CFLOBDD nodes", "CFLOBDD ms", "BDD ms", "BDD/CF", "calls/op"
        );
        let kmax: usize = std::env::args()
            .position(|a| a == "--kmax")
            .and_then(|i| std::env::args().nth(i + 1))
            .and_then(|v| v.parse().ok())
            .unwrap_or(13);
        for k in 3..=kmax {
            let n = 1usize << k;
            let bal = balanced_grammar(k);
            let (cn, _ops, cms, ccalls) = measure(&bal, f);
            let bdd = bdd_grammar(n);
            let (_bn, _o2, bms, _bc) = measure(&bdd, f);
            println!(
                "{n:>8} {cn:>14} {cms:>12.2} {bms:>12.2} {:>7.1}x {ccalls:>10.1}",
                bms / cms
            );
            use std::io::Write;
            std::io::stdout().flush().ok();
            if cms > 8_000.0 || bms > 8_000.0 {
                println!("         (stopping: past 8 s on one build)");
                break;
            }
        }
    }
}
