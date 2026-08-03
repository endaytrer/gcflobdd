use super::*;
use crate::gcflobdd::context::Context;
use std::cell::RefCell;

/// Deterministic xorshift PRNG so the tests are reproducible.
fn prng(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// Matrix-shaped grammars and the dimension each describes. The balanced family
/// only reaches dimensions `2, 4, 16, 256`, so the binary-but-unbalanced shapes
/// fill in the gaps and check that the two groupings need not be equal.
fn matrix_grammars() -> Vec<(Grammar, usize)> {
    vec![
        // 2x2: the base case on its own.
        (Grammar::new(&["S0 -> a a".to_string()]).unwrap(), 2),
        // 4x4, balanced.
        (
            Grammar::new(&["S1 -> S0 S0".to_string(), "S0 -> a a".to_string()]).unwrap(),
            4,
        ),
        // 8x8, unbalanced: a 4-variable grouping beside a 2-variable one.
        (
            Grammar::new(&[
                "S -> A B".to_string(),
                "A -> C C".to_string(),
                "C -> a a".to_string(),
                "B -> a a".to_string(),
            ])
            .unwrap(),
            8,
        ),
        // 8x8, unbalanced the other way round.
        (
            Grammar::new(&[
                "S -> B A".to_string(),
                "A -> C C".to_string(),
                "C -> a a".to_string(),
                "B -> a a".to_string(),
            ])
            .unwrap(),
            8,
        ),
        // 16x16, balanced.
        (
            Grammar::new(&[
                "S2 -> S1 S1".to_string(),
                "S1 -> S0 S0".to_string(),
                "S0 -> a a".to_string(),
            ])
            .unwrap(),
            16,
        ),
    ]
}

fn dense_mul<T: MatMulValue>(a: &[Vec<T>], b: &[Vec<T>]) -> Vec<Vec<T>> {
    let n = a.len();
    (0..n)
        .map(|i| {
            (0..n)
                .map(|j| {
                    let mut acc = T::zero_like(&a[0][0]);
                    for k in 0..n {
                        acc = acc.add(&a[i][k].mul(&b[k][j]));
                    }
                    acc
                })
                .collect()
        })
        .collect()
}

fn assert_entries<T: MatMulValue + std::fmt::Debug>(
    product: &GcflobddT<'_, T>,
    expected: &[Vec<T>],
    what: &str,
) {
    for (i, row) in expected.iter().enumerate() {
        for (j, value) in row.iter().enumerate() {
            assert_eq!(product.entry(i, j), *value, "{what}: entry ({i}, {j})");
        }
    }
}

fn random_matrix(n: usize, state: &mut u64, modulus: i64) -> Vec<Vec<i64>> {
    (0..n)
        .map(|_| {
            (0..n)
                .map(|_| (prng(state) % (2 * modulus as u64 + 1)) as i64 - modulus)
                .collect()
        })
        .collect()
}

fn constant_matrix(n: usize, value: i64) -> Vec<Vec<i64>> {
    vec![vec![value; n]; n]
}

fn dense_identity(n: usize) -> Vec<Vec<i64>> {
    (0..n)
        .map(|i| (0..n).map(|j| if i == j { 1 } else { 0 }).collect())
        .collect()
}

#[test]
fn from_matrix_round_trips() {
    let mut state = 0x0123_4567_89ab_cdefu64;
    for (grammar, n) in matrix_grammars() {
        let context = RefCell::new(Context::default());
        let dense = random_matrix(n, &mut state, 3);
        let m = GcflobddT::from_matrix(&dense, &grammar, &context);
        assert_entries(&m, &dense, "round trip");
    }
}

#[test]
fn matmul_matches_dense_oracle() {
    let mut state = 0xfeed_face_dead_beefu64;
    for (grammar, n) in matrix_grammars() {
        let context = RefCell::new(Context::default());
        for _ in 0..4 {
            let a = random_matrix(n, &mut state, 4);
            let b = random_matrix(n, &mut state, 4);
            let product = GcflobddT::from_matrix(&a, &grammar, &context)
                .mk_matmul(&GcflobddT::from_matrix(&b, &grammar, &context), &context);
            assert_entries(&product, &dense_mul(&a, &b), &format!("random {n}x{n}"));
        }
    }
}

#[test]
fn matmul_handles_structured_operands() {
    let mut state = 0x5eed_1234_5678_9abcu64;
    for (grammar, n) in matrix_grammars() {
        let context = RefCell::new(Context::default());
        let random = random_matrix(n, &mut state, 5);

        // Sparse: a single non-zero entry exercises the zero propagation.
        let mut one_hot = constant_matrix(n, 0);
        one_hot[n / 2][1] = 3;
        // A whole zero block, so the short circuit fires below the top node.
        let mut half_zero = random.clone();
        for row in half_zero.iter_mut().take(n / 2) {
            for value in row.iter_mut().take(n / 2) {
                *value = 0;
            }
        }

        for (name, a) in [
            ("zero", constant_matrix(n, 0)),
            ("ones", constant_matrix(n, 1)),
            ("identity", dense_identity(n)),
            ("one hot", one_hot),
            ("half zero", half_zero),
        ] {
            let da = GcflobddT::from_matrix(&a, &grammar, &context);
            let db = GcflobddT::from_matrix(&random, &grammar, &context);
            assert_entries(
                &da.mk_matmul(&db, &context),
                &dense_mul(&a, &random),
                &format!("{name} * random ({n}x{n})"),
            );
            assert_entries(
                &db.mk_matmul(&da, &context),
                &dense_mul(&random, &a),
                &format!("random * {name} ({n}x{n})"),
            );
        }
    }
}

#[test]
fn matmul_is_associative() {
    let mut state = 0xabad_1dea_0000_1111u64;
    for (grammar, n) in matrix_grammars() {
        let context = RefCell::new(Context::default());
        let a = GcflobddT::from_matrix(&random_matrix(n, &mut state, 2), &grammar, &context);
        let b = GcflobddT::from_matrix(&random_matrix(n, &mut state, 2), &grammar, &context);
        let c = GcflobddT::from_matrix(&random_matrix(n, &mut state, 2), &grammar, &context);

        let left = a.mk_matmul(&b, &context).mk_matmul(&c, &context);
        let right = a.mk_matmul(&b.mk_matmul(&c, &context), &context);
        // Diagram equality, not just entry equality: equal matrices must have
        // the same canonical representation.
        assert_eq!(left, right, "associativity at {n}x{n}");
    }
}

#[test]
fn identity_is_neutral() {
    let mut state = 0x0f0f_0f0f_1234_5678u64;
    for (grammar, n) in matrix_grammars() {
        let context = RefCell::new(Context::default());
        let a = GcflobddT::from_matrix(&random_matrix(n, &mut state, 3), &grammar, &context);
        let identity = GcflobddT::mk_identity(1i64, 0i64, &grammar, &context);

        // The directly built identity is the same diagram as the tabulated one.
        assert_eq!(
            identity,
            GcflobddT::from_matrix(&dense_identity(n), &grammar, &context),
            "identity at {n}x{n}"
        );
        assert_eq!(a.mk_matmul(&identity, &context), a, "A * I at {n}x{n}");
        assert_eq!(identity.mk_matmul(&a, &context), a, "I * A at {n}x{n}");
    }
}

#[test]
fn cancelling_product_is_exactly_zero() {
    // Every entry of A * B cancels to zero. The result must be the constant
    // zero diagram, not a diagram whose exits happen to evaluate to zero: a
    // coefficient that cancels has to disappear, and must not be confused with
    // a structurally zero term.
    for (grammar, n) in matrix_grammars() {
        let context = RefCell::new(Context::default());
        let a = constant_matrix(n, 1);
        let b: Vec<Vec<i64>> = (0..n)
            .map(|i| (0..n).map(|_| if i % 2 == 0 { 1 } else { -1 }).collect())
            .collect();
        let product = GcflobddT::from_matrix(&a, &grammar, &context)
            .mk_matmul(&GcflobddT::from_matrix(&b, &grammar, &context), &context);

        assert_entries(&product, &dense_mul(&a, &b), &format!("cancelling {n}x{n}"));
        assert_eq!(
            product,
            GcflobddT::mk_constant(0i64, &grammar, &context),
            "cancelling product at {n}x{n} must be the constant zero diagram"
        );
    }
}

#[test]
fn matmul_over_f64() {
    let mut state = 0x1111_2222_3333_4444u64;
    for (grammar, n) in matrix_grammars() {
        let context = RefCell::new(Context::default());
        // Small integers held as f64, so the comparison stays exact.
        let to_f64 = |m: Vec<Vec<i64>>| -> Vec<Vec<f64>> {
            m.into_iter()
                .map(|row| row.into_iter().map(|v| v as f64).collect())
                .collect()
        };
        let a = to_f64(random_matrix(n, &mut state, 3));
        let b = to_f64(random_matrix(n, &mut state, 3));
        let product = GcflobddT::from_matrix(&a, &grammar, &context)
            .mk_matmul(&GcflobddT::from_matrix(&b, &grammar, &context), &context);
        assert_entries(&product, &dense_mul(&a, &b), &format!("f64 {n}x{n}"));
    }
}

#[cfg(feature = "complex")]
#[test]
fn matmul_over_complex() {
    use rug::Complex;
    let grammar = Grammar::new(&["S1 -> S0 S0".to_string(), "S0 -> a a".to_string()]).unwrap();
    let context = RefCell::new(Context::default());
    let c = |re: i32, im: i32| Complex::with_val(64, (re, im));

    // i * P, where P is a permutation that is its own inverse.
    let permutation = [[1, 0, 0, 0], [0, 1, 0, 0], [0, 0, 0, 1], [0, 0, 1, 0]];
    let a: Vec<Vec<Complex>> = permutation
        .iter()
        .map(|row| row.iter().map(|&v| c(0, v)).collect())
        .collect();
    let product = GcflobddT::from_matrix(&a, &grammar, &context)
        .mk_matmul(&GcflobddT::from_matrix(&a, &grammar, &context), &context);
    // (i P)^2 = -P^2 = -I.
    assert_entries(&product, &dense_mul(&a, &a), "complex 4x4");
    assert_eq!(product.entry(0, 0), c(-1, 0));
    assert_eq!(product.entry(0, 1), c(0, 0));
    assert_eq!(product.entry(3, 3), c(-1, 0));
}

/// The balanced grammar for `2^level` variables, i.e. a `2^(2^(level-1))`
/// square matrix. Mirrors `gen_balanced_grammar` in the integration tests.
fn balanced_grammar(level: usize) -> Grammar {
    let mut rules = (1..level)
        .rev()
        .map(|i| format!("S{} -> S{} S{}", i, i - 1, i - 1))
        .collect::<Vec<_>>();
    rules.push("S0 -> a a".to_string());
    Grammar::new(&rules).unwrap()
}

#[test]
fn identity_scales_to_huge_matrices() {
    // 2^16 variables, i.e. a 2^32768-square matrix. Nothing may depend on the
    // dimension: the identity is logarithmic and so is its square.
    let grammar = balanced_grammar(16);
    let context = RefCell::new(Context::default());
    let identity = GcflobddT::mk_identity(1i64, 0i64, &grammar, &context);
    let squared = identity.mk_matmul(&identity, &context);
    assert_eq!(squared, identity);

    // I * (every entry 2) has every entry 2.
    let doubled = squared.mk_matmul(&GcflobddT::mk_constant(2i64, &grammar, &context), &context);
    assert_eq!(doubled.entry(0, 0), 2);
    assert_eq!(doubled.entry(3, 7), 2);

    assert!(
        context.borrow().node_count() < 500,
        "node count {} should stay logarithmic",
        context.borrow().node_count()
    );
}

#[test]
fn matmul_survives_gc() {
    let mut state = 0x9999_8888_7777_6666u64;
    let grammar = balanced_grammar(3); // 8 variables -> 16x16
    let context = RefCell::new(Context::default());
    let a = random_matrix(16, &mut state, 3);
    let b = random_matrix(16, &mut state, 3);

    let product = {
        let da = GcflobddT::from_matrix(&a, &grammar, &context);
        let db = GcflobddT::from_matrix(&b, &grammar, &context);
        da.mk_matmul(&db, &context)
    };
    context.borrow_mut().gc();
    assert_entries(&product, &dense_mul(&a, &b), "after gc");

    // Multiplying again once the caches have been dropped must still agree.
    let squared = product.mk_matmul(&product, &context);
    let expected = dense_mul(&a, &b);
    assert_entries(
        &squared,
        &dense_mul(&expected, &expected),
        "square after gc",
    );
}
