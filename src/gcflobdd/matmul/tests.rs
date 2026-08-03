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

// ---------------------------------------------------------------------------
// Matrix-vector multiplication.
// ---------------------------------------------------------------------------

fn dense_matvec<T: MatMulValue>(m: &[Vec<T>], v: &[T]) -> Vec<T> {
    m.iter()
        .map(|row| {
            let mut acc = T::zero_like(&v[0]);
            for (a, b) in row.iter().zip(v) {
                acc = acc.add(&a.mul(b));
            }
            acc
        })
        .collect()
}

fn assert_components<T: MatMulValue + std::fmt::Debug>(
    product: &GcflobddT<'_, T>,
    expected: &[T],
    what: &str,
) {
    for (i, value) in expected.iter().enumerate() {
        assert_eq!(product.component(i), *value, "{what}: component {i}");
    }
}

fn random_vector(n: usize, state: &mut u64, modulus: i64) -> Vec<i64> {
    (0..n)
        .map(|_| (prng(state) % (2 * modulus as u64 + 1)) as i64 - modulus)
        .collect()
}

/// Every matrix grammar paired with the vector grammar its matrices act on.
fn matvec_grammars() -> Vec<(Grammar, Grammar, usize)> {
    matrix_grammars()
        .into_iter()
        .map(|(matrix, n)| {
            let vector = matrix.halved();
            assert_eq!(vector.num_vars() * 2, matrix.num_vars());
            (matrix, vector, n)
        })
        .collect()
}

#[test]
fn halved_grammar_preserves_sharing() {
    // A balanced matrix grammar must halve to a balanced vector grammar: the
    // two groupings of each rule stay the *same* node, or node tables lose all
    // their sharing.
    let matrix = balanced_grammar(4); // 16 variables -> 256x256
    let vector = matrix.halved();
    assert_eq!(vector.num_vars(), 8);

    let context = RefCell::new(Context::default());
    // 2^8 components, so a dense build is still cheap; it round-trips only if
    // the halved tree really addresses 8 big-endian index bits.
    let entries: Vec<i64> = (0..256).map(|i| (i as i64) % 5).collect();
    let v = GcflobddT::from_vector(&entries, &vector, &context);
    for (i, value) in entries.iter().enumerate() {
        assert_eq!(v.component(i), *value);
    }
}

#[test]
fn matvec_matches_dense_oracle() {
    let mut state = 0xc0ff_ee00_1234_5678u64;
    for (matrix_grammar, vector_grammar, n) in matvec_grammars() {
        let context = RefCell::new(Context::default());
        for _ in 0..4 {
            let m = random_matrix(n, &mut state, 4);
            let v = random_vector(n, &mut state, 4);
            let product = GcflobddT::from_matrix(&m, &matrix_grammar, &context).mk_matvec(
                &GcflobddT::from_vector(&v, &vector_grammar, &context),
                &context,
            );
            assert_components(&product, &dense_matvec(&m, &v), &format!("random {n}x{n}"));
        }
    }
}

#[test]
fn matvec_handles_structured_operands() {
    let mut state = 0x1357_9bdf_2468_ace0u64;
    for (matrix_grammar, vector_grammar, n) in matvec_grammars() {
        let context = RefCell::new(Context::default());
        let random_m = random_matrix(n, &mut state, 5);
        let random_v = random_vector(n, &mut state, 5);

        let mut one_hot = vec![0i64; n];
        one_hot[n / 2] = 3;
        // A zero block in the matrix, so the short circuit fires below the top.
        let mut half_zero = random_m.clone();
        for row in half_zero.iter_mut().take(n / 2) {
            for value in row.iter_mut().take(n / 2) {
                *value = 0;
            }
        }

        let matrices = [
            ("zero", constant_matrix(n, 0)),
            ("ones", constant_matrix(n, 1)),
            ("identity", dense_identity(n)),
            ("half zero", half_zero),
            ("random", random_m),
        ];
        let vectors = [
            ("zero", vec![0i64; n]),
            ("ones", vec![1i64; n]),
            ("one hot", one_hot),
            ("random", random_v),
        ];
        for (m_name, m) in &matrices {
            for (v_name, v) in &vectors {
                let product = GcflobddT::from_matrix(m, &matrix_grammar, &context).mk_matvec(
                    &GcflobddT::from_vector(v, &vector_grammar, &context),
                    &context,
                );
                assert_components(
                    &product,
                    &dense_matvec(m, v),
                    &format!("{m_name} * {v_name} ({n}x{n})"),
                );
            }
        }
    }
}

#[test]
fn identity_leaves_vectors_alone() {
    let mut state = 0x2222_3333_4444_5555u64;
    for (matrix_grammar, vector_grammar, n) in matvec_grammars() {
        let context = RefCell::new(Context::default());
        let v = GcflobddT::from_vector(&random_vector(n, &mut state, 3), &vector_grammar, &context);
        let identity = GcflobddT::mk_identity(1i64, 0i64, &matrix_grammar, &context);
        // Diagram equality: I * v must reproduce v's canonical representation.
        assert_eq!(identity.mk_matvec(&v, &context), v, "I * v at {n}");
    }
}

#[test]
fn matvec_agrees_with_matmul() {
    // Broadcasting v across the columns of a matrix V makes M * V a matrix
    // whose every column is M * v, which cross-checks the two recursions
    // against each other rather than against the same dense oracle.
    let mut state = 0x7777_1111_2222_3333u64;
    for (matrix_grammar, vector_grammar, n) in matvec_grammars() {
        let context = RefCell::new(Context::default());
        let m = random_matrix(n, &mut state, 3);
        let v = random_vector(n, &mut state, 3);
        let broadcast: Vec<Vec<i64>> = v.iter().map(|value| vec![*value; n]).collect();

        let dm = GcflobddT::from_matrix(&m, &matrix_grammar, &context);
        let by_matmul = dm.mk_matmul(
            &GcflobddT::from_matrix(&broadcast, &matrix_grammar, &context),
            &context,
        );
        let by_matvec = dm.mk_matvec(
            &GcflobddT::from_vector(&v, &vector_grammar, &context),
            &context,
        );

        for i in 0..n {
            let expected = by_matvec.component(i);
            for j in 0..n {
                assert_eq!(by_matmul.entry(i, j), expected, "row {i}, column {j}");
            }
        }
    }
}

#[test]
fn basis_vectors_select_columns() {
    let mut state = 0x8888_9999_aaaa_bbbbu64;
    for (matrix_grammar, vector_grammar, n) in matvec_grammars() {
        let context = RefCell::new(Context::default());
        let m = random_matrix(n, &mut state, 4);
        let dm = GcflobddT::from_matrix(&m, &matrix_grammar, &context);

        for k in 0..n {
            let basis = GcflobddT::mk_basis_vector(k, 1i64, 0i64, &vector_grammar, &context);
            // e_k itself...
            for i in 0..n {
                assert_eq!(
                    basis.component(i),
                    (i == k) as i64,
                    "e_{k} component {i} (n={n})"
                );
            }
            // ... and the same diagram as the tabulated version.
            let mut dense = vec![0i64; n];
            dense[k] = 1;
            assert_eq!(
                basis,
                GcflobddT::from_vector(&dense, &vector_grammar, &context),
                "e_{k} (n={n})"
            );
            // M * e_k is column k of M.
            let column = dm.mk_matvec(&basis, &context);
            for (i, row) in m.iter().enumerate() {
                assert_eq!(column.component(i), row[k], "column {k}, row {i} (n={n})");
            }
        }
    }
}

#[test]
fn cancelling_matvec_is_exactly_zero() {
    for (matrix_grammar, vector_grammar, n) in matvec_grammars() {
        let context = RefCell::new(Context::default());
        // Alternating +-1 against an all-ones matrix: every component cancels.
        let m = constant_matrix(n, 1);
        let v: Vec<i64> = (0..n).map(|i| if i % 2 == 0 { 1 } else { -1 }).collect();
        let product = GcflobddT::from_matrix(&m, &matrix_grammar, &context).mk_matvec(
            &GcflobddT::from_vector(&v, &vector_grammar, &context),
            &context,
        );

        assert_components(&product, &dense_matvec(&m, &v), &format!("cancelling {n}"));
        assert_eq!(
            product,
            GcflobddT::mk_constant(0i64, &vector_grammar, &context),
            "a cancelling product must be the constant zero vector at {n}"
        );
    }
}

#[test]
fn matvec_over_f64() {
    let mut state = 0xdddd_eeee_ffff_0000u64;
    for (matrix_grammar, vector_grammar, n) in matvec_grammars() {
        let context = RefCell::new(Context::default());
        // Small integers held as f64, so the comparison stays exact.
        let m: Vec<Vec<f64>> = random_matrix(n, &mut state, 3)
            .into_iter()
            .map(|row| row.into_iter().map(|v| v as f64).collect())
            .collect();
        let v: Vec<f64> = random_vector(n, &mut state, 3)
            .into_iter()
            .map(|v| v as f64)
            .collect();
        let product = GcflobddT::from_matrix(&m, &matrix_grammar, &context).mk_matvec(
            &GcflobddT::from_vector(&v, &vector_grammar, &context),
            &context,
        );
        assert_components(&product, &dense_matvec(&m, &v), &format!("f64 {n}"));
    }
}

#[test]
fn matvec_scales_to_huge_matrices() {
    // 2^16 variables: a 2^32768-square matrix against a 2^32768-long vector.
    let matrix_grammar = balanced_grammar(16);
    let vector_grammar = matrix_grammar.halved();
    let context = RefCell::new(Context::default());

    let identity = GcflobddT::mk_identity(1i64, 0i64, &matrix_grammar, &context);
    let ones = GcflobddT::mk_constant(1i64, &vector_grammar, &context);
    assert_eq!(identity.mk_matvec(&ones, &context), ones);

    // (J - I) * e_0 is 1 everywhere except at component 0. Building J - I needs
    // no dense work, and e_0 is logarithmic, so the whole check is structural.
    let all_ones = GcflobddT::mk_constant(1i64, &matrix_grammar, &context);
    let hollow = all_ones.mk_op_pair_map(&identity, |a, b| a - b, &context);
    let basis = GcflobddT::mk_basis_vector(0, 1i64, 0i64, &vector_grammar, &context);
    let product = hollow.mk_matvec(&basis, &context);
    assert_eq!(product.component(0), 0);
    assert_eq!(product.component(1), 1);
    assert_eq!(product.component(12345), 1);
    // ... and it is exactly the complement of e_0.
    assert_eq!(
        product,
        GcflobddT::mk_basis_vector(0, 0i64, 1i64, &vector_grammar, &context)
    );

    assert!(
        context.borrow().node_count() < 500,
        "node count {} should stay logarithmic",
        context.borrow().node_count()
    );
}

#[test]
fn matvec_survives_gc() {
    let mut state = 0x4444_5555_6666_7777u64;
    let matrix_grammar = balanced_grammar(3); // 8 variables -> 16x16
    let vector_grammar = matrix_grammar.halved();
    let context = RefCell::new(Context::default());
    let m = random_matrix(16, &mut state, 3);
    let v = random_vector(16, &mut state, 3);

    let product = {
        let dm = GcflobddT::from_matrix(&m, &matrix_grammar, &context);
        let dv = GcflobddT::from_vector(&v, &vector_grammar, &context);
        dm.mk_matvec(&dv, &context)
    };
    context.borrow_mut().gc();
    assert_components(&product, &dense_matvec(&m, &v), "after gc");

    // Applying the matrix again once the caches have been dropped must agree.
    let twice = GcflobddT::from_matrix(&m, &matrix_grammar, &context).mk_matvec(&product, &context);
    assert_components(
        &twice,
        &dense_matvec(&m, &dense_matvec(&m, &v)),
        "second application after gc",
    );
}

// ---------------------------------------------------------------------------
// Kronecker product.
// ---------------------------------------------------------------------------

fn dense_kron<T: MatMulValue>(a: &[Vec<T>], b: &[Vec<T>]) -> Vec<Vec<T>> {
    let nb = b.len();
    (0..a.len() * nb)
        .map(|row| {
            (0..a.len() * nb)
                .map(|col| a[row / nb][col / nb].mul(&b[row % nb][col % nb]))
                .collect()
        })
        .collect()
}

fn dense_kron_vector<T: MatMulValue>(v: &[T], w: &[T]) -> Vec<T> {
    v.iter().flat_map(|a| w.iter().map(|b| a.mul(b))).collect()
}

/// The 2x2, 4x4 and 8x8 grammars, the last unbalanced, to build operands from.
fn kron_operand_grammars() -> Vec<(Grammar, usize)> {
    matrix_grammars()
        .into_iter()
        .filter(|(_, n)| *n <= 8)
        .collect()
}

#[test]
fn kron_matches_dense_oracle() {
    let mut state = 0x0abc_def0_1234_5678u64;
    for (ga, na) in kron_operand_grammars() {
        for (gb, nb) in kron_operand_grammars() {
            let combined = ga.concat(&gb);
            let context = RefCell::new(Context::default());

            let a = random_matrix(na, &mut state, 3);
            let b = random_matrix(nb, &mut state, 3);
            let product = GcflobddT::from_matrix(&a, &ga, &context).mk_kron(
                &GcflobddT::from_matrix(&b, &gb, &context),
                &combined,
                &context,
            );
            assert_entries(
                &product,
                &dense_kron(&a, &b),
                &format!("{na}x{na} (x) {nb}x{nb}"),
            );
        }
    }
}

#[test]
fn kron_of_vectors_matches_dense_oracle() {
    let mut state = 0x1a2b_3c4d_5e6f_7080u64;
    for (ga, na) in kron_operand_grammars() {
        for (gb, nb) in kron_operand_grammars() {
            let (va, vb) = (ga.halved(), gb.halved());
            let combined = va.concat(&vb);
            let context = RefCell::new(Context::default());

            let v = random_vector(na, &mut state, 3);
            let w = random_vector(nb, &mut state, 3);
            let product = GcflobddT::from_vector(&v, &va, &context).mk_kron(
                &GcflobddT::from_vector(&w, &vb, &context),
                &combined,
                &context,
            );
            assert_components(
                &product,
                &dense_kron_vector(&v, &w),
                &format!("vector {na} (x) {nb}"),
            );
        }
    }
}

#[test]
fn kron_preserves_structural_identities() {
    for (ga, na) in kron_operand_grammars() {
        for (gb, nb) in kron_operand_grammars() {
            let combined = ga.concat(&gb);
            let context = RefCell::new(Context::default());

            // I_a (x) I_b is the identity of the combined space -- and must be
            // the very same diagram, not merely an equal matrix.
            let ia = GcflobddT::mk_identity(1i64, 0i64, &ga, &context);
            let ib = GcflobddT::mk_identity(1i64, 0i64, &gb, &context);
            assert_eq!(
                ia.mk_kron(&ib, &combined, &context),
                GcflobddT::mk_identity(1i64, 0i64, &combined, &context),
                "I_{na} (x) I_{nb}"
            );

            // e_i (x) e_j = e_(i * nb + j).
            let (va, vb) = (ga.halved(), gb.halved());
            let combined_vector = va.concat(&vb);
            for (i, j) in [(0, 0), (1, 0), (0, 1), (na - 1, nb - 1)] {
                let ei = GcflobddT::mk_basis_vector(i, 1i64, 0i64, &va, &context);
                let ej = GcflobddT::mk_basis_vector(j, 1i64, 0i64, &vb, &context);
                assert_eq!(
                    ei.mk_kron(&ej, &combined_vector, &context),
                    GcflobddT::mk_basis_vector(i * nb + j, 1i64, 0i64, &combined_vector, &context),
                    "e_{i} (x) e_{j} (nb={nb})"
                );
            }
        }
    }
}

#[test]
fn kron_satisfies_the_mixed_product_property() {
    // (A (x) B)(C (x) D) = (AC) (x) (BD), and the same against a vector. These
    // check the three constructions against each other rather than against a
    // shared dense oracle, and as diagram equality rather than entry by entry.
    let mut state = 0x9f9f_1e1e_2d2d_3c3cu64;
    for (ga, na) in kron_operand_grammars() {
        for (gb, nb) in kron_operand_grammars() {
            let combined = ga.concat(&gb);
            let (va, vb) = (ga.halved(), gb.halved());
            let combined_vector = va.concat(&vb);
            let context = RefCell::new(Context::default());

            let a = GcflobddT::from_matrix(&random_matrix(na, &mut state, 2), &ga, &context);
            let c = GcflobddT::from_matrix(&random_matrix(na, &mut state, 2), &ga, &context);
            let b = GcflobddT::from_matrix(&random_matrix(nb, &mut state, 2), &gb, &context);
            let d = GcflobddT::from_matrix(&random_matrix(nb, &mut state, 2), &gb, &context);

            let left = a
                .mk_kron(&b, &combined, &context)
                .mk_matmul(&c.mk_kron(&d, &combined, &context), &context);
            let right =
                a.mk_matmul(&c, &context)
                    .mk_kron(&b.mk_matmul(&d, &context), &combined, &context);
            assert_eq!(left, right, "(A (x) B)(C (x) D) at {na} (x) {nb}");

            // The vector side lives over `concat` of the halved grammars, which
            // mirrors `combined.halved()` structurally -- which is all that
            // `mk_matvec` requires of it.
            let v = GcflobddT::from_vector(&random_vector(na, &mut state, 2), &va, &context);
            let w = GcflobddT::from_vector(&random_vector(nb, &mut state, 2), &vb, &context);
            let applied = a
                .mk_kron(&b, &combined, &context)
                .mk_matvec(&v.mk_kron(&w, &combined_vector, &context), &context);
            let separately = a.mk_matvec(&v, &context).mk_kron(
                &b.mk_matvec(&w, &context),
                &combined_vector,
                &context,
            );
            assert_eq!(applied, separately, "(A (x) B)(v (x) w) at {na} (x) {nb}");
        }
    }
}

#[test]
fn kron_collapses_colliding_products() {
    // Values [1, 2] against [2, 1]: the pairs (0,1) and (1,0) both multiply to
    // 2, so the result has fewer exits than the pairs it was built from and
    // must still be the canonical diagram of that matrix.
    let ga = Grammar::new(&["S0 -> a a".to_string()]).unwrap();
    let gb = Grammar::new(&["S0 -> a a".to_string()]).unwrap();
    let combined = ga.concat(&gb);
    let context = RefCell::new(Context::default());

    let a = vec![vec![1i64, 2], vec![2, 1]];
    let b = vec![vec![2i64, 1], vec![1, 2]];
    let product = GcflobddT::from_matrix(&a, &ga, &context).mk_kron(
        &GcflobddT::from_matrix(&b, &gb, &context),
        &combined,
        &context,
    );

    let expected = dense_kron(&a, &b);
    assert_entries(&product, &expected, "colliding products");
    assert_eq!(
        product,
        GcflobddT::from_matrix(&expected, &combined, &context),
        "a collapsed product must equal the tabulated matrix's diagram"
    );

    // An operand carrying a zero collapses further: every product against it is
    // the same value.
    let zeroed = vec![vec![0i64, 0], vec![0, 0]];
    let with_zero = GcflobddT::from_matrix(&a, &ga, &context).mk_kron(
        &GcflobddT::from_matrix(&zeroed, &gb, &context),
        &combined,
        &context,
    );
    assert_eq!(
        with_zero,
        GcflobddT::mk_constant(0i64, &combined, &context),
        "A (x) 0 must be the constant zero diagram"
    );
}

#[test]
fn kron_folds_to_huge_operators() {
    // Doubling 16 times: a 2^65536-square Walsh matrix. Each fold adds a
    // constant number of nodes, so the whole thing stays tiny.
    const FOLDS: usize = 16;
    let mut grammars = vec![Grammar::new(&["S0 -> a a".to_string()]).unwrap()];
    for i in 0..FOLDS {
        let doubled = grammars[i].concat(&grammars[i]);
        grammars.push(doubled);
    }
    let context = RefCell::new(Context::default());

    let hadamard = vec![vec![1i64, 1], vec![1, -1]];
    let mut walsh = GcflobddT::from_matrix(&hadamard, &grammars[0], &context);
    let mut counts = Vec::new();
    for grammar in grammars.iter().skip(1) {
        walsh = walsh.mk_kron(&walsh, grammar, &context);
        counts.push(context.borrow().node_count());
    }

    // H^(x)2 is the 4x4 Walsh matrix; the entry at (r, c) is the parity of
    // r & c, which holds at every level.
    let two_fold = GcflobddT::from_matrix(&hadamard, &grammars[0], &context);
    let two_fold = two_fold.mk_kron(&two_fold, &grammars[1], &context);
    for r in 0..4usize {
        for c in 0..4usize {
            let sign = if (r & c).count_ones() % 2 == 0 { 1 } else { -1 };
            assert_eq!(two_fold.entry(r, c), sign, "H^(x)2 at ({r}, {c})");
        }
    }
    // The same parity rule at the top of the fold, where the matrix has
    // 2^65536 rows.
    assert_eq!(walsh.entry(0, 0), 1);
    assert_eq!(walsh.entry(1, 1), -1);
    assert_eq!(walsh.entry(3, 5), -1); // 3 & 5 = 1, odd parity

    // Growth per fold must be constant, not proportional to the dimension.
    let growth = counts.last().unwrap() - counts.first().unwrap();
    assert!(
        growth < 10 * FOLDS,
        "node count grew by {growth} over {FOLDS} folds: {counts:?}"
    );

    // The fold still composes with the other operations.
    let vector_grammar = grammars[FOLDS].halved();
    let basis = GcflobddT::mk_basis_vector(0, 1i64, 0i64, &vector_grammar, &context);
    let column = walsh.mk_matvec(&basis, &context);
    assert_eq!(column.component(0), 1);
    assert_eq!(column.component(7), 1); // the first column of a Walsh matrix is all ones
}

#[test]
fn kron_survives_gc() {
    let mut state = 0xfeed_0000_beef_1111u64;
    let ga = balanced_grammar(2); // 4 variables -> 4x4
    let gb = Grammar::new(&["S0 -> a a".to_string()]).unwrap(); // 2x2
    let combined = ga.concat(&gb);
    let context = RefCell::new(Context::default());

    let a = random_matrix(4, &mut state, 3);
    let b = random_matrix(2, &mut state, 3);
    let product = {
        let da = GcflobddT::from_matrix(&a, &ga, &context);
        let db = GcflobddT::from_matrix(&b, &gb, &context);
        da.mk_kron(&db, &combined, &context)
    };
    context.borrow_mut().gc();

    let expected = dense_kron(&a, &b);
    assert_entries(&product, &expected, "after gc");
    // ... and it still multiplies once the caches have been dropped.
    let squared = product.mk_matmul(&product, &context);
    assert_entries(
        &squared,
        &dense_mul(&expected, &expected),
        "square after gc",
    );
}
