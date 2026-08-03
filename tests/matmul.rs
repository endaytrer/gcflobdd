use gcflobdd::gcflobdd::GcflobddT;
use gcflobdd::gcflobdd::context::Context;
use gcflobdd::grammar::Grammar;
use std::cell::RefCell;

type Matrix<'grammar> = GcflobddT<'grammar, i64>;

/// The balanced grammar over `2^level` variables, i.e. a `2^(2^(level-1))`
/// square matrix -- the non-general CFLOBDD shape.
fn gen_balanced_grammar(level: usize) -> Vec<String> {
    let mut rules = (1..level)
        .rev()
        .map(|i| format!("S{} -> S{} S{}", i, i - 1, i - 1))
        .collect::<Vec<_>>();
    rules.push("S0 -> a a".to_string());
    rules
}

fn size_to_readable(size: usize) -> String {
    let mut size = size as f64;
    let mut unit = 0;
    let prefixes = ["B", "KiB", "MiB", "GiB"];
    while size >= 1024.0 && unit + 1 < prefixes.len() {
        size /= 1024.0;
        unit += 1;
    }
    format!("{:.2}{}", size, prefixes[unit])
}

fn timed<'grammar>(
    label: &str,
    context: &RefCell<Context<'grammar>>,
    op: impl FnOnce() -> Matrix<'grammar>,
) -> Matrix<'grammar> {
    let start = std::time::Instant::now();
    let result = op();
    let elapsed = start.elapsed();
    println!(
        "  {:<14} {:>8} ms   {:>8} nodes   {:>10}",
        label,
        elapsed.as_millis(),
        context.borrow().node_count(),
        size_to_readable(context.borrow().size_estimate())
    );
    result
}

/// Deterministic xorshift PRNG so runs are reproducible.
fn prng(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// Multiply a random dense matrix by itself and check every entry against the
/// naive product.
fn dense_check(level: usize) {
    let grammar = Grammar::new(&gen_balanced_grammar(level)).unwrap();
    let vector_grammar = grammar.halved();
    let context = RefCell::new(Context::default());
    let n = 1usize << (1usize << (level - 1));
    println!("Dense {n}x{n} check (level {level}):");

    let mut state = 0x2468_ace0_1357_9bdfu64;
    let a: Vec<Vec<i64>> = (0..n)
        .map(|_| (0..n).map(|_| (prng(&mut state) % 7) as i64 - 3).collect())
        .collect();
    let v: Vec<i64> = (0..n).map(|_| (prng(&mut state) % 7) as i64 - 3).collect();

    let start = std::time::Instant::now();
    let da = Matrix::from_matrix(&a, &grammar, &context);
    let dv = Matrix::from_vector(&v, &vector_grammar, &context);
    println!("  built in {} ms", start.elapsed().as_millis());

    let product = timed("A * A", &context, || da.mk_matmul(&da, &context));
    let applied = timed("A * v", &context, || da.mk_matvec(&dv, &context));

    let start = std::time::Instant::now();
    for i in 0..n {
        for j in 0..n {
            let expected: i64 = (0..n).map(|k| a[i][k] * a[k][j]).sum();
            assert_eq!(product.entry(i, j), expected, "entry ({i}, {j})");
        }
        let expected: i64 = (0..n).map(|k| a[i][k] * v[k]).sum();
        assert_eq!(applied.component(i), expected, "component {i}");
    }
    println!(
        "  verified against the dense oracle in {} ms",
        start.elapsed().as_millis()
    );
}

/// Structured operands at a dimension no dense representation could reach.
fn structured(level: usize) {
    let grammar = Grammar::new(&gen_balanced_grammar(level)).unwrap();
    let vector_grammar = grammar.halved();
    let context = RefCell::new(Context::default());
    let dimension_bits = 1usize << (level - 1);
    println!("Structured, 2^{dimension_bits} x 2^{dimension_bits} (level {level}):");

    let identity = Matrix::mk_identity(1, 0, &grammar, &context);
    let ones = Matrix::mk_constant(1, &grammar, &context);
    // J - I: the all-ones matrix off the diagonal, zero on it.
    let hollow = ones.mk_op_pair_map(&identity, |a, b| a - b, &context);

    let squared = timed("I * I", &context, || {
        identity.mk_matmul(&identity, &context)
    });
    assert_eq!(squared, identity);
    let left = timed("(J-I) * I", &context, || {
        hollow.mk_matmul(&identity, &context)
    });
    assert_eq!(left, hollow);
    let right = timed("I * (J-I)", &context, || {
        identity.mk_matmul(&hollow, &context)
    });
    assert_eq!(right, hollow);

    // (J-I)^2 = (N-2)J + I, so it only stays inside i64 for modest dimensions.
    if dimension_bits <= 30 {
        let n = 1i64 << dimension_bits;
        let product = timed("(J-I)^2", &context, || hollow.mk_matmul(&hollow, &context));
        assert_eq!(product.entry(0, 0), n - 1);
        assert_eq!(product.entry(0, 1), n - 2);
    }

    // Matrix-vector, at the same dimensions.
    let ones_vector = Matrix::mk_constant(1, &vector_grammar, &context);
    let basis = Matrix::mk_basis_vector(0, 1, 0, &vector_grammar, &context);

    let applied = timed("I * 1", &context, || {
        identity.mk_matvec(&ones_vector, &context)
    });
    assert_eq!(applied, ones_vector);
    // (J-I) * e_0 is one everywhere but at component 0.
    let column = timed("(J-I) * e_0", &context, || {
        hollow.mk_matvec(&basis, &context)
    });
    assert_eq!(column.component(0), 0);
    assert_eq!(column.component(1), 1);
    // (J-I) * 1 = (N-1) * 1, again only within i64 for modest dimensions.
    if dimension_bits <= 30 {
        let n = 1i64 << dimension_bits;
        let summed = timed("(J-I) * 1", &context, || {
            hollow.mk_matvec(&ones_vector, &context)
        });
        assert_eq!(summed.component(0), n - 1);
    }

    let before = context.borrow().node_count();
    context.borrow_mut().gc();
    println!(
        "  gc: {} -> {} nodes",
        before,
        context.borrow().node_count()
    );
}

fn main() {
    // The dense check is the algorithm's worst case -- a random matrix has no
    // structure to share -- so it stays small by default; --dense-level 4 (a
    // 256x256) takes seconds in release and much longer unoptimized.
    let mut level = 8;
    let mut dense_level = 3;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--level" if i + 1 < args.len() => {
                level = args[i + 1].parse().unwrap();
                i += 1;
            }
            "--dense-level" if i + 1 < args.len() => {
                dense_level = args[i + 1].parse().unwrap();
                i += 1;
            }
            other => {
                println!("Usage: {} [--level <L>] [--dense-level <L>]", args[0]);
                println!("  a level-L balanced grammar denotes a 2^(2^(L-1)) square matrix");
                println!("unknown argument: {other}");
                return;
            }
        }
        i += 1;
    }

    assert!(level >= 1 && dense_level >= 1, "level must be at least 1");
    dense_check(dense_level);
    let mut levels = vec![2, 4, 6, level];
    levels.sort_unstable();
    levels.dedup();
    for l in levels {
        structured(l);
    }
}
