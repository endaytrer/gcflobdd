use std::{cell::RefCell, rc::Rc};

use crate::{
    gcflobdd::{
        connection::{Connection, ConnectionT},
        context::Context,
        matmul::map::MatMulMap,
        node::{GcflobddNode, GcflobddNodeType, InternalNode},
        return_map::inverse_lookup,
    },
    grammar::{GrammarNode, GrammarNodeType},
    utils::{hash_cache::Rch, new_hash_map},
};

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
) -> (Connection<'grammar>, Vec<Connection<'grammar>>) {
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
                vec![0],
                context,
            ),
            vec![Connection::new(
                GcflobddNode::mk_no_distinction(g2, context),
                vec![0],
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
    let mut reduce_map = Vec::with_capacity(values.len());
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
                        vec![classes[2 * r]],
                        context,
                    )
                } else {
                    Connection::new(
                        GcflobddNode::mk_distinction(0, g2, context),
                        vec![classes[2 * r], classes[2 * r + 1]],
                        context,
                    )
                }
            };
            let (a_connection, b_connections) =
                if classes[0] == classes[2] && classes[1] == classes[3] {
                    (
                        Connection::new(
                            GcflobddNode::mk_no_distinction(g1, context),
                            vec![0],
                            context,
                        ),
                        vec![row(0)],
                    )
                } else {
                    (
                        Connection::new(
                            GcflobddNode::mk_distinction(0, g1, context),
                            vec![0, 1],
                            context,
                        ),
                        vec![row(0), row(1)],
                    )
                };
            let node = context.borrow_mut().add_gcflobdd_node(GcflobddNode {
                num_exits: values.len(),
                grammar,
                node: GcflobddNodeType::Internal(InternalNode {
                    connections: vec![vec![a_connection], b_connections],
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

        let mut values: Vec<MatMulMap> = Vec::new();
        let mut seen_value = new_hash_map();
        let mut b_connections: Vec<Connection<'grammar>> = Vec::new();
        let mut seen_connection = new_hash_map();
        let mut reduce_map = Vec::with_capacity(aa.return_map.len());

        // 3b. Realize one block per distinct grid-cell class.
        for cell in aa.return_map.iter() {
            // The plan's keys live in the A-targets' exit space; lift them into
            // the B-connection indices they select.
            let plan = cell.lift(&a1.return_map, &a2.return_map);
            let block = if plan.is_zero() {
                zero_valued(g2, context)
            } else {
                let mut acc: Option<Valued<'grammar>> = None;
                for (i, j, coeff) in plan.iter() {
                    let sub = matmul_node(
                        &b1s[i].entry_point,
                        &b2s[j].entry_point,
                        b_zeros1[i],
                        b_zeros2[j],
                        context,
                    );
                    // Lift the sub-result out of the children's exit spaces and
                    // into this node's operands', then weight it.
                    let lifted = sub
                        .return_map
                        .iter()
                        .map(|m| m.lift(&b1s[i].return_map, &b2s[j].return_map).scale(coeff))
                        .collect();
                    let term = normalize(&sub.entry_point, lifted, context);
                    // Adding an all-zero block changes nothing; skipping keeps
                    // sparse operands cheap.
                    if term.return_map.len() == 1 && term.return_map[0].is_zero() {
                        continue;
                    }
                    acc = Some(match acc {
                        None => term,
                        Some(previous) => add_valued(&previous, &term, context),
                    });
                }
                acc.unwrap_or_else(|| zero_valued(g2, context))
            };

            // Fold the block's values into this node's exit list...
            let mut return_map = Vec::with_capacity(block.return_map.len());
            for value in block.return_map.iter() {
                let next = values.len();
                return_map.push(*seen_value.entry(value.clone()).or_insert_with(|| {
                    values.push(value.clone());
                    next
                }));
            }
            // ... and keep the B-connections distinct. Entry points and return
            // maps are both interned, so pointer equality is structural.
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
            valued(GcflobddNode::mk_no_distinction(grammar, context), values)
        } else {
            // Deduping B-connections collapses grid-cell classes, so the A-node
            // is reduced to match. `reduce` returns the node unchanged for an
            // identity map, and the `DontCare` node for a single class.
            let num_blocks = b_connections.len();
            let a_connection = Connection::new(
                GcflobddNode::reduce(&aa.entry_point, reduce_map.into(), num_blocks, context),
                (0..num_blocks).collect(),
                context,
            );
            let node = context.borrow_mut().add_gcflobdd_node(GcflobddNode {
                num_exits: values.len(),
                grammar,
                node: GcflobddNodeType::Internal(InternalNode {
                    connections: vec![vec![a_connection], b_connections],
                }),
            });
            valued(node, values)
        }
    };

    context
        .borrow_mut()
        .set_matmul_cache(n1, n2, z1, z2, ans.clone());
    ans
}
