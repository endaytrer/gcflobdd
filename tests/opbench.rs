//! Per-operation cost of the boolean layer, in isolation.
//!
//! The quantum benchmarks do a small number of large operations, where a
//! per-operation constant disappears. Network verification
//! ([`NETWORK.md`](../NETWORK.md)) does tens of millions of tiny ones, and there
//! the constant is the whole story: at k=12 the atomic-predicate stage is ~67M
//! conjunctions on diagrams of a few hundred nodes.
//!
//! This measures that constant directly, so a change to the operand or cache
//! representation can be judged without a JVM in the loop.
//!
//!     cargo test --release --test opbench
//!     cargo test --release --test opbench -- --iters 20000000

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// Counts allocations, so "this path allocates a lot" can be a number rather
/// than an impression.
struct Counting;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
/// Allocations bucketed by size class, so "lots of small Vecs" can be checked
/// rather than assumed. Bucket i covers sizes in [2^i, 2^(i+1)).
static BUCKETS: [AtomicUsize; 12] = [const { AtomicUsize::new(0) }; 12];

fn bucket_of(size: usize) -> usize {
    (usize::BITS - size.max(1).leading_zeros()) as usize - 1
}

fn buckets_snapshot() -> [usize; 12] {
    std::array::from_fn(|i| BUCKETS[i].load(Ordering::Relaxed))
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        let b = bucket_of(layout.size());
        if b < BUCKETS.len() {
            BUCKETS[b].fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

use gcflobdd::gcflobdd::Gcflobdd;
use gcflobdd::gcflobdd::context::Context;
use gcflobdd::grammar::Grammar;

/// The 104-bit five-tuple grammar the network benchmark uses: aligned-balanced
/// with one shared subtree per width.
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

/// `n` distinct predicates shaped like the ones a data plane produces: a prefix
/// on a field, i.e. a conjunction of the field's *top* bits.
///
/// Prefixes nest, so the atoms they induce stay bounded. Conjunctions of bits
/// drawn at random from all 104 variables would be near-independent and split
/// the space 2^n ways, which is not a workload -- it is an out-of-memory.
fn prefix_predicates<'g>(
    vars: &[Gcflobdd<'g>],
    context: &RefCell<Context<'g>>,
    n: usize,
) -> Vec<Gcflobdd<'g>> {
    // Field bases in the five-tuple: src_ip at 0, dst_ip at 32.
    const FIELDS: [(usize, usize); 2] = [(0, 32), (32, 32)];
    let mut out = Vec::with_capacity(n);
    let mut seed = 0x9e3779b97f4a7c15u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        seed >> 33
    };
    for _ in 0..n {
        let (base, width) = FIELDS[next() as usize % FIELDS.len()];
        let len = 8 + next() as usize % 17; // /8 .. /24
        let value = next();
        let mut acc: Option<Gcflobdd> = None;
        for bit in 0..len.min(width) {
            let one = (value >> (len - 1 - bit)) & 1 == 1;
            let lit = if one {
                vars[base + bit].clone()
            } else {
                vars[base + bit].mk_not()
            };
            acc = Some(match acc {
                None => lit,
                Some(a) => a.mk_and(&lit, context),
            });
        }
        out.push(acc.unwrap());
    }
    out
}

/// Yang and Lam's atomic predicates, the loop the network benchmark spends its
/// time in. Returns the atom count and the number of conjunctions performed.
///
/// Emptiness is a comparison against one precomputed `false` diagram, not a
/// fresh `a AND NOT a` each time -- the Java harness tests `x == FALSE`, and
/// rebuilding the constant inside the loop would put a conjunction in the count
/// that the workload does not actually do.
fn atomic_predicates<'g>(
    predicates: &[Gcflobdd<'g>],
    context: &RefCell<Context<'g>>,
) -> (usize, usize) {
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
                continue; // a lies wholly inside p, or misses it entirely
            }
            let outside = a.mk_and(&not_p, context);
            ops += 1;
            atoms[i] = inside;
            if outside != empty {
                atoms.push(outside);
            }
        }
    }
    (atoms.len(), ops)
}

fn bench(name: &str, iters: usize, mut f: impl FnMut(usize)) -> f64 {
    for i in 0..iters / 20 {
        f(i);
    }
    let t0 = Instant::now();
    for i in 0..iters {
        f(i);
    }
    let ns = t0.elapsed().as_nanos() as f64 / iters as f64;
    println!("{name:<28} {ns:>8.1} ns/op   ({iters} iterations)");
    ns
}

fn main() {
    let mut iters = 5_000_000usize;
    let args: Vec<String> = std::env::args().collect();
    if let Some(i) = args.iter().position(|a| a == "--iters") {
        iters = args[i + 1].parse().expect("--iters wants a number");
    }

    // `--profile <seconds>` reruns the atomic-predicate workload against a fresh
    // context until the time is up, so a sampling profiler has something to
    // sample. It is the miss path that costs, and it is the miss path this
    // holds open.
    if let Some(i) = args.iter().position(|a| a == "--profile") {
        let secs: u64 = args[i + 1].parse().expect("--profile wants seconds");
        let grammar = five_tuple_grammar();
        let deadline = Instant::now() + std::time::Duration::from_secs(secs);
        let mut rounds = 0usize;
        while Instant::now() < deadline {
            let context = Context::new();
            let vars: Vec<Gcflobdd> = (0..104)
                .map(|i| Gcflobdd::mk_projection(i, &grammar, &context))
                .collect();
            let predicates = prefix_predicates(&vars, &context, 48);
            std::hint::black_box(atomic_predicates(&predicates, &context));
            rounds += 1;
        }
        println!("profiled {rounds} rounds");
        return;
    }

    let grammar = five_tuple_grammar();
    assert_eq!(grammar.num_vars(), 104);
    let context = Context::new();

    let vars: Vec<Gcflobdd> = (0..104)
        .map(|i| Gcflobdd::mk_projection(i, &grammar, &context))
        .collect();

    println!("boolean layer, 104-bit five-tuple grammar");

    // The hot path in atomic-predicate splitting: the same conjunction asked for
    // over and over, so every call after the first is a cache hit and what is
    // left is the cost of getting to the cached answer.
    let (a, b) = (vars[0].clone(), vars[32].clone());
    let cached = bench("and, cache hit", iters, |_| {
        std::hint::black_box(a.mk_and(&b, &context));
    });

    // Cloning an operand is what a cache probe costs before it even hashes.
    let clone_cost = bench("clone an operand", iters, |_| {
        std::hint::black_box(a.clone());
    });

    // The real shape of the atomic-predicate stage: every conjunction is a
    // *different* pair, so the operation cache misses and the cost is the
    // pair-map and reduction underneath it. Cycling a small set of operands
    // measures cache hits instead, however many iterations it runs for -- which
    // is exactly the mistake that made the cache probe look like the bottleneck.
    let predicates = prefix_predicates(&vars, &context, 48);
    let allocs_before = ALLOCS.load(Ordering::Relaxed);
    let buckets_before = buckets_snapshot();
    let t0 = Instant::now();
    let (atoms, ops) = atomic_predicates(&predicates, &context);
    let elapsed = t0.elapsed();
    let allocs = ALLOCS.load(Ordering::Relaxed) - allocs_before;
    let buckets_after = buckets_snapshot();
    let per_op = elapsed.as_nanos() as f64 / ops as f64;
    println!(
        "{:<28} {:>8.1} ns/op   ({ops} conjunctions -> {atoms} atoms, {} ms)",
        "atomic predicates (misses)",
        per_op,
        elapsed.as_millis()
    );
    println!(
        "{:<28} {:>8.1} allocations per conjunction",
        "  of which allocation:",
        allocs as f64 / ops as f64
    );
    println!("  by size class:");
    for i in 0..BUCKETS.len() {
        let n = buckets_after[i] - buckets_before[i];
        if n == 0 {
            continue;
        }
        println!(
            "    {:>5}..{:<5} bytes  {:>9}  {:>5.1}/op  {:>4.0}%",
            1usize << i,
            (1usize << (i + 1)) - 1,
            n,
            n as f64 / ops as f64,
            100.0 * n as f64 / allocs as f64
        );
    }

    println!();
    println!(
        "OPBENCH cached_ns={cached:.1} clone_ns={clone_cost:.1} ap_ns={per_op:.1} \
ap_ms={} atoms={atoms} allocs_per_op={:.1}",
        elapsed.as_millis(),
        allocs as f64 / ops as f64
    );
}
