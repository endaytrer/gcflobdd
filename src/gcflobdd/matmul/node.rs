use std::{cell::RefCell, rc::Rc};

use smallvec::smallvec;

use crate::{
    gcflobdd::{
        connection::{Connection, ConnectionT},
        context::Context,
        matmul::map::MatMulMap,
        node::{GcflobddNode, GcflobddNodeType, InternalNode},
        return_map::{ReturnMap, inverse_lookup},
    },
    grammar::{GrammarNode, GrammarNodeType},
    utils::{hash_cache::Rch, new_hash_map},
};
use crate::gcflobdd::return_map::ExitVec;
use crate::gcflobdd::connection::ConnectionLayer;

/// A node paired with one [`MatMulMap`] per exit: the symbolic result of a
/// (sub-)multiplication, before any value is substituted. The values are behind
/// an `Rc` so that a cache hit is cheap to clone.
pub(in crate::gcflobdd) type Valued<'grammar> = ConnectionT<'grammar, Rc<Vec<MatMulMap>>>;

fn valued<'grammar>(
    entry_point: Rch<GcflobddNode<'grammar>>,
    values: Vec<MatMulMap>,
) -> Valued<'grammar> {
    debug_assert_eq!(entry_point.num_exits, values.len());
    Valued {
        entry_point,
        return_map: Rc::new(values),
    }
}

/// The all-zero (sub-)matrix: one exit carrying the empty combination.
fn zero_valued<'grammar>(
    grammar: &'grammar Rc<GrammarNode>,
    context: &RefCell<Context<'grammar>>,
) -> Valued<'grammar> {
    valued(
        GcflobddNode::mk_no_distinction(grammar, context),
        vec![MatMulMap::zero()],
    )
}

/// The two groupings of a matrix-shaped grammar node: the first covers the grid
/// coordinate `(Rhi, Chi)`, the second the position `(Rlo, Clo)` inside a block.
pub(super) fn split(grammar: &Rc<GrammarNode>) -> (&Rc<GrammarNode>, &Rc<GrammarNode>) {
    match &grammar.node {
        GrammarNodeType::Internal(children) => match &children[..] {
            [g1, g2] => (g1, g2),
            _ => panic!(
                "matmul requires binary groupings, found a rule with {} symbols on the right",
                children.len()
            ),
        },
        _ => panic!("matmul requires binary groupings, found a BDD or terminal grouping"),
    }
}

/// The exit indices of the four entries of a 2x2 matrix, row-major: variable 0
/// is the row bit and variable 1 the column bit.
fn read_2x2(node: &GcflobddNode) -> [usize; 4] {
    [
        node.evaluate(&[false, false]),
        node.evaluate(&[false, true]),
        node.evaluate(&[true, false]),
        node.evaluate(&[true, true]),
    ]
}

/// The A-connection and B-connections of `node`.
///
/// A `DontCare` node has no stored connections, so its expansion (an A and a
/// single B, both `DontCare` one grouping down) is synthesised here; that is
/// what the C++ implementation gets by materialising `NoDistinctionNode`.
fn decompose<'grammar>(
    node: &Rch<GcflobddNode<'grammar>>,
    g1: &'grammar Rc<GrammarNode>,
    g2: &'grammar Rc<GrammarNode>,
    context: &RefCell<Context<'grammar>>,
) -> (Connection<'grammar>, ConnectionLayer<'grammar>) {
    match &node.node {
        GcflobddNodeType::Internal(internal) => match &internal.connections[..] {
            [a_layer, b_layer] => match &a_layer[..] {
                [a] => (a.clone(), b_layer.clone()),
                _ => unreachable!("the first connection layer holds exactly one connection"),
            },
            _ => panic!("matmul requires binary groupings"),
        },
        GcflobddNodeType::DontCare => (
            Connection::new(
                GcflobddNode::mk_no_distinction(g1, context),
                smallvec![0],
                context,
            ),
            smallvec![Connection::new(
                GcflobddNode::mk_no_distinction(g2, context),
                smallvec![0],
                context,
            )],
        ),
        GcflobddNodeType::Bdd(_) => panic!("matmul does not support BDD groupings"),
        GcflobddNodeType::Fork => unreachable!("a fork spans one variable, never a matrix"),
    }
}

/// Push the known-zero exit `z` of a node one grouping down.
///
/// Returns the A-target exit that selects the all-zero block (if any) and, for
/// each B-connection, the child exit that carries the zero value. Passing these
/// into the recursive calls is what lets the short circuit fire on subtrees of
/// a sparse operand.
fn locate_zeros(
    a: &Connection,
    b: &[Connection],
    z: Option<usize>,
) -> (Option<usize>, Vec<Option<usize>>) {
    let Some(z) = z else {
        return (None, vec![None; b.len()]);
    };
    let b_zeros = b
        .iter()
        .map(|connection| inverse_lookup(&connection.return_map, &z))
        .collect::<Vec<_>>();
    let a_zero = (0..a.entry_point.num_exits).find(|&t| {
        let block = a.return_map[t];
        b[block].entry_point.num_exits == 1 && b_zeros[block] == Some(0)
    });
    (a_zero, b_zeros)
}

/// Collapse exits that ended up carrying the same value.
///
/// Lifting maps into a parent's exit space can make two distinct exits equal,
/// which would leave a non-canonical diagram behind; this is the `Reduce()` the
/// algorithm notes put at the end of the recursion, and the node-level twin of
/// [`GcflobddT::map`](crate::gcflobdd::GcflobddT::map).
fn normalize<'grammar>(
    entry_point: &Rch<GcflobddNode<'grammar>>,
    values: Vec<MatMulMap>,
    context: &RefCell<Context<'grammar>>,
) -> Valued<'grammar> {
    let mut new_values = Vec::with_capacity(values.len());
    let mut seen = new_hash_map();
    let mut reduce_map = ExitVec::with_capacity(values.len());
    for value in values {
        let next = new_values.len();
        reduce_map.push(*seen.entry(value.clone()).or_insert_with(|| {
            new_values.push(value);
            next
        }));
    }
    let num_exits = new_values.len();
    valued(
        GcflobddNode::reduce(entry_point, reduce_map.into(), num_exits, context),
        new_values,
    )
}

/// Add two symbolic diagrams over the same variables, exit value by exit value.
///
/// This is the node-level twin of
/// [`GcflobddT::mk_op`](crate::gcflobdd::GcflobddT::mk_op): pair the two
/// diagrams, add the values of each pair that is actually reachable, then
/// collapse. The
/// [`mk_op_pair_map`](crate::gcflobdd::GcflobddT::mk_op_pair_map) route -- the
/// faster one for booleans -- would instead evaluate the operator on the full
/// `lhs_exits x rhs_exits` grid to build a reduce matrix, and most of that grid
/// is unreachable here. Adding two [`MatMulMap`]s allocates, and these diagrams
/// routinely have hundreds of exits, so paying only for reachable pairs matters
/// a great deal.
fn add_valued<'grammar>(
    lhs: &Valued<'grammar>,
    rhs: &Valued<'grammar>,
    context: &RefCell<Context<'grammar>>,
) -> Valued<'grammar> {
    let ConnectionT {
        entry_point,
        return_map,
    } = GcflobddNode::pair_product(&lhs.entry_point, &rhs.entry_point, context);
    let values = return_map
        .iter()
        .map(|(j, k)| lhs.return_map[*j].add(&rhs.return_map[*k]))
        .collect();
    normalize(&entry_point, values, context)
}

/// Carry out one plan: multiply the block pairs it names, weight them, and add
/// them up.
///
/// `multiply(i, j)` performs the recursive call for the `i`th left block and
/// the `j`th right one; the caller supplies it because that is the only part
/// that differs between a matrix and a vector right-hand side.
fn realize_block<'grammar>(
    plan: &MatMulMap,
    grammar: &'grammar Rc<GrammarNode>,
    lhs: &[Connection<'grammar>],
    rhs: &[Connection<'grammar>],
    mut multiply: impl FnMut(usize, usize) -> Valued<'grammar>,
    context: &RefCell<Context<'grammar>>,
) -> Valued<'grammar> {
    if plan.is_zero() {
        return zero_valued(grammar, context);
    }
    let mut acc: Option<Valued<'grammar>> = None;
    for (i, j, coeff) in plan.iter() {
        let sub = multiply(i, j);
        // Lift the sub-result out of the children's exit spaces and into this
        // node's operands', then weight it.
        let lifted = sub
            .return_map
            .iter()
            .map(|m| m.lift(&lhs[i].return_map, &rhs[j].return_map).scale(coeff))
            .collect();
        let term = normalize(&sub.entry_point, lifted, context);
        // Adding an all-zero block changes nothing; skipping keeps sparse
        // operands cheap.
        if term.return_map.len() == 1 && term.return_map[0].is_zero() {
            continue;
        }
        acc = Some(match acc {
            None => term,
            Some(previous) => add_valued(&previous, &term, context),
        });
    }
    acc.unwrap_or_else(|| zero_valued(grammar, context))
}

/// Build a node over `grammar` from `aa` -- a diagram whose exits are classes
/// of cells -- and the block realized for each of those classes.
///
/// Folds every block's values into one exit list in first-appearance order,
/// keeps the B-connections distinct, and reduces the A-node to match when
/// deduping collapsed classes.
fn assemble<'grammar>(
    grammar: &'grammar Rc<GrammarNode>,
    aa_entry_point: &Rch<GcflobddNode<'grammar>>,
    blocks: Vec<Valued<'grammar>>,
    context: &RefCell<Context<'grammar>>,
) -> Valued<'grammar> {
    let mut values: Vec<MatMulMap> = Vec::new();
    let mut seen_value = new_hash_map();
    let mut b_connections: ConnectionLayer<'grammar> = ConnectionLayer::new();
    let mut seen_connection = new_hash_map();
    let mut reduce_map = ExitVec::with_capacity(blocks.len());

    for block in blocks {
        let mut return_map = ReturnMap::with_capacity(block.return_map.len());
        for value in block.return_map.iter() {
            let next = values.len();
            return_map.push(*seen_value.entry(value.clone()).or_insert_with(|| {
                values.push(value.clone());
                next
            }));
        }
        // Entry points and return maps are both interned, so pointer equality
        // is structural equality.
        let connection = Connection::new(block.entry_point, return_map, context);
        let key = (
            Rc::as_ptr(&connection.entry_point) as usize,
            Rc::as_ptr(&connection.return_map) as usize,
        );
        let next = b_connections.len();
        reduce_map.push(*seen_connection.entry(key).or_insert_with(|| {
            b_connections.push(connection);
            next
        }));
    }

    if values.len() == 1 {
        return valued(GcflobddNode::mk_no_distinction(grammar, context), values);
    }
    // `reduce` returns the node unchanged for an identity map, and the
    // `DontCare` node for a single class.
    let num_blocks = b_connections.len();
    let a_connection = Connection::new(
        GcflobddNode::reduce(aa_entry_point, reduce_map.into(), num_blocks, context),
        (0..num_blocks).collect(),
        context,
    );
    let node = context.borrow_mut().add_gcflobdd_node(GcflobddNode {
        num_exits: values.len(),
        grammar,
        node: GcflobddNodeType::Internal(InternalNode {
            connections: vec![smallvec![a_connection], b_connections],
        }),
    });
    valued(node, values)
}

/// Multiply the matrices denoted by `n1` and `n2`, symbolically.
///
/// `z1` / `z2` are the exit indices of the two operands that are known to carry
/// the value zero, if any; they only ever prune work.
///
/// The result's exit values are [`MatMulMap`]s over pairs of `n1`'s and `n2`'s
/// *own* exits, so the caller can lift them into whatever space it needs and
/// the top node can substitute real values in one final pass.
pub(super) fn matmul_node<'grammar>(
    n1: &Rch<GcflobddNode<'grammar>>,
    n2: &Rch<GcflobddNode<'grammar>>,
    z1: Option<usize>,
    z2: Option<usize>,
    context: &RefCell<Context<'grammar>>,
) -> Valued<'grammar> {
    debug_assert!(Rc::ptr_eq(n1.grammar, n2.grammar));

    // 0. Memoize on structure, never on values.
    if let Some(cached) = context.borrow().get_matmul_cache(n1, n2, z1, z2) {
        return cached;
    }

    let grammar = n1.grammar;
    let (g1, g2) = split(grammar);

    // 1. Provably-zero short circuit: one operand is the all-zero matrix.
    let ans = if (n1.num_exits == 1 && z1 == Some(0)) || (n2.num_exits == 1 && z2 == Some(0)) {
        zero_valued(grammar, context)
    } else if matches!(g1.node, GrammarNodeType::Terminal) {
        // 2. Base case: two variables, i.e. a 2x2 matrix.
        debug_assert!(matches!(g2.node, GrammarNodeType::Terminal));
        let a = read_2x2(n1);
        let b = read_2x2(n2);
        // A known-zero operand contributes nothing, so the pair is never
        // created; that is what keeps zero exits out of every map, and hence
        // what makes propagating `z` down sound.
        let term = |i: usize, j: usize| {
            if Some(a[i]) == z1 || Some(b[j]) == z2 {
                MatMulMap::zero()
            } else {
                MatMulMap::single(a[i], b[j])
            }
        };
        let entries = [
            term(0, 0).add(&term(1, 2)), // A00*B00 + A01*B10
            term(0, 1).add(&term(1, 3)), // A00*B01 + A01*B11
            term(2, 0).add(&term(3, 2)), // A10*B00 + A11*B10
            term(2, 1).add(&term(3, 3)), // A10*B01 + A11*B11
        ];

        // Dedup in the order (0,0), (0,1), (1,0), (1,1) -- which is exactly the
        // canonical exit order of a two-variable node.
        let mut values: Vec<MatMulMap> = Vec::with_capacity(4);
        let mut classes = [0usize; 4];
        for (slot, entry) in classes.iter_mut().zip(entries) {
            *slot = values.iter().position(|v| *v == entry).unwrap_or_else(|| {
                values.push(entry);
                values.len() - 1
            });
        }

        if values.len() == 1 {
            valued(GcflobddNode::mk_no_distinction(grammar, context), values)
        } else {
            let row = |r: usize| {
                if classes[2 * r] == classes[2 * r + 1] {
                    Connection::new(
                        GcflobddNode::mk_no_distinction(g2, context),
                        smallvec![classes[2 * r]],
                        context,
                    )
                } else {
                    Connection::new(
                        GcflobddNode::mk_distinction(0, g2, context),
                        smallvec![classes[2 * r], classes[2 * r + 1]],
                        context,
                    )
                }
            };
            let (a_connection, b_connections): (_, ConnectionLayer) =
                if classes[0] == classes[2] && classes[1] == classes[3] {
                    (
                        Connection::new(
                            GcflobddNode::mk_no_distinction(g1, context),
                            smallvec![0],
                            context,
                        ),
                        smallvec![row(0)],
                    )
                } else {
                    (
                        Connection::new(
                            GcflobddNode::mk_distinction(0, g1, context),
                            smallvec![0, 1],
                            context,
                        ),
                        smallvec![row(0), row(1)],
                    )
                };
            let node = context.borrow_mut().add_gcflobdd_node(GcflobddNode {
                num_exits: values.len(),
                grammar,
                node: GcflobddNodeType::Internal(InternalNode {
                    connections: vec![smallvec![a_connection], b_connections],
                }),
            });
            valued(node, values)
        }
    } else {
        // 3. Recursive case.
        let (a1, b1s) = decompose(n1, g1, g2, context);
        let (a2, b2s) = decompose(n2, g1, g2, context);
        let (a_zero1, b_zeros1) = locate_zeros(&a1, &b1s, z1);
        let (a_zero2, b_zeros2) = locate_zeros(&a2, &b2s, z2);

        // 3a. Multiply the grids: one recursive call covers every grid cell,
        // and its exits are the equivalence classes of cells sharing a plan.
        let aa = matmul_node(&a1.entry_point, &a2.entry_point, a_zero1, a_zero2, context);

        // 3b. Realize one block per distinct grid-cell class. The plans' keys
        // live in the A-targets' exit space, so lift them into the
        // B-connection indices they select.
        let blocks = aa
            .return_map
            .iter()
            .map(|cell| {
                let plan = cell.lift(&a1.return_map, &a2.return_map);
                realize_block(
                    &plan,
                    g2,
                    &b1s,
                    &b2s,
                    |i, j| {
                        matmul_node(
                            &b1s[i].entry_point,
                            &b2s[j].entry_point,
                            b_zeros1[i],
                            b_zeros2[j],
                            context,
                        )
                    },
                    context,
                )
            })
            .collect();

        assemble(grammar, &aa.entry_point, blocks, context)
    };

    context
        .borrow_mut()
        .set_matmul_cache(n1, n2, z1, z2, ans.clone());
    ans
}

/// The Kronecker product of `n1` and `n2`, symbolically.
///
/// `grammar` must be the concatenation of the two operands' grammars, so the
/// result runs `n1` over the first block of variables and `n2` over the second
/// -- which, in the interleaved order, is exactly what `A (x) B` means. There
/// is no recursion and nothing to cache: the operands are embedded as they
/// stand, and the work is one connection per exit of `n1`.
///
/// Exit `i * n2.num_exits + j` pairs exit `i` of `n1` with exit `j` of `n2`,
/// which is already the canonical first-appearance order -- the exits of the
/// first B-connection come first, then the second's, and so on.
pub(super) fn kron_node<'grammar>(
    n1: &Rch<GcflobddNode<'grammar>>,
    n2: &Rch<GcflobddNode<'grammar>>,
    grammar: &'grammar Rc<GrammarNode>,
    context: &RefCell<Context<'grammar>>,
) -> Valued<'grammar> {
    let (na, nb) = (n1.num_exits, n2.num_exits);

    let a_connection = Connection::new(n1.clone(), (0..na).collect(), context);
    let b_connections: ConnectionLayer = (0..na)
        .map(|i| Connection::new(n2.clone(), (0..nb).map(|j| i * nb + j).collect(), context))
        .collect();
    let node = context.borrow_mut().add_gcflobdd_node(GcflobddNode {
        num_exits: na * nb,
        grammar,
        node: GcflobddNodeType::Internal(InternalNode {
            connections: vec![smallvec![a_connection], b_connections],
        }),
    });

    // Every exit is one plain product; equal products collapse when the caller
    // substitutes real values, which is also what reduces the whole diagram to
    // a `DontCare` node when they all coincide.
    let values = (0..na)
        .flat_map(|i| (0..nb).map(move |j| MatMulMap::single(i, j)))
        .collect();
    valued(node, values)
}

/// Multiply the matrix denoted by `m` by the vector denoted by `v`,
/// symbolically.
///
/// `m` is over a matrix grammar and `v` over its
/// [halved](crate::grammar::Grammar::halved) counterpart, so `v` carries half
/// as many variables and the result is a *vector* diagram over `v`'s grammar.
/// `zm` / `zv` are the exits known to carry zero, if any; they only prune work.
///
/// This is the matrix recurrence with the column coordinate dropped:
///
/// ```text
///     yblock(Rhi) = sum over Chi of  B1[ A1(Rhi, Chi) ] * Bv[ Av(Chi) ]
/// ```
///
/// which is itself a matrix-vector product -- of the grid `A1` by the vector
/// `Av` -- so one recursive call still yields the plan for every `Rhi` at once,
/// in the same deferred semiring. The result's exit values are
/// [`MatMulMap`]s over pairs of `m`'s and `v`'s own exits.
pub(super) fn matvec_node<'grammar>(
    m: &Rch<GcflobddNode<'grammar>>,
    v: &Rch<GcflobddNode<'grammar>>,
    zm: Option<usize>,
    zv: Option<usize>,
    context: &RefCell<Context<'grammar>>,
) -> Valued<'grammar> {
    debug_assert_eq!(m.grammar.num_vars, 2 * v.grammar.num_vars);

    // 0. Memoize on structure, never on values.
    if let Some(cached) = context.borrow().get_matvec_cache(m, v, zm, zv) {
        return cached;
    }

    let matrix_grammar = m.grammar;
    let vector_grammar = v.grammar;

    // 1. Provably-zero short circuit: one operand is all zeros.
    let ans = if (m.num_exits == 1 && zm == Some(0)) || (v.num_exits == 1 && zv == Some(0)) {
        zero_valued(vector_grammar, context)
    } else if matches!(vector_grammar.node, GrammarNodeType::Terminal) {
        // 2. Base case: a 2x2 matrix times a 2-element vector.
        let a = read_2x2(m);
        let b = [v.evaluate(&[false]), v.evaluate(&[true])];
        let term = |i: usize, j: usize| {
            if Some(a[i]) == zm || Some(b[j]) == zv {
                MatMulMap::zero()
            } else {
                MatMulMap::single(a[i], b[j])
            }
        };
        let p0 = term(0, 0).add(&term(1, 1)); // M00*v0 + M01*v1
        let p1 = term(2, 0).add(&term(3, 1)); // M10*v0 + M11*v1

        if p0 == p1 {
            valued(
                GcflobddNode::mk_no_distinction(vector_grammar, context),
                vec![p0],
            )
        } else {
            // A fork's exits are already in the canonical order: index 0, then 1.
            valued(
                GcflobddNode::mk_distinction(0, vector_grammar, context),
                vec![p0, p1],
            )
        }
    } else {
        // 3. Recursive case. The matrix splits into a grid of blocks and the
        // vector into a vector of sub-vectors, over corresponding groupings.
        let (g1, g2) = split(matrix_grammar);
        let (h1, h2) = split(vector_grammar);
        let (am, bms) = decompose(m, g1, g2, context);
        let (av, bvs) = decompose(v, h1, h2, context);
        let (a_zero_m, b_zeros_m) = locate_zeros(&am, &bms, zm);
        let (a_zero_v, b_zeros_v) = locate_zeros(&av, &bvs, zv);

        // 3a. The grid times the sub-vector index: one recursive call, whose
        // exits are the classes of Rhi that share a plan.
        let aa = matvec_node(
            &am.entry_point,
            &av.entry_point,
            a_zero_m,
            a_zero_v,
            context,
        );

        // 3b. Realize one sub-vector per class. Lifting moves the plans' keys
        // from the A-targets' exit spaces into the B-connection indices they
        // select -- matrix blocks on one side, vector blocks on the other.
        let blocks = aa
            .return_map
            .iter()
            .map(|cell| {
                let plan = cell.lift(&am.return_map, &av.return_map);
                realize_block(
                    &plan,
                    h2,
                    &bms,
                    &bvs,
                    |i, j| {
                        matvec_node(
                            &bms[i].entry_point,
                            &bvs[j].entry_point,
                            b_zeros_m[i],
                            b_zeros_v[j],
                            context,
                        )
                    },
                    context,
                )
            })
            .collect();

        assemble(vector_grammar, &aa.entry_point, blocks, context)
    };

    context
        .borrow_mut()
        .set_matvec_cache(m, v, zm, zv, ans.clone());
    ans
}
