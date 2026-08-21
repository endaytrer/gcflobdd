//! Direct construction of a controlled gate.
//!
//! `|0><0|_c (x) I  +  |1><1|_c (x) U_t` is the definition, and building it that
//! way -- two Kronecker towers over the whole register, then a matrix addition
//! -- costs a pair-product over both towers and rebuilds every level for every
//! gate. On a circuit that places one controlled gate per qubit that dominates
//! everything else, including the matrix multiplies it feeds.
//!
//! This module builds the operator's node in one downward pass instead, the way
//! the reference C++ CFLOBDD's `MkCNOTNode` does, and memoises it on
//! `(grouping, role)`.
//!
//! # Why the recursion carries tags rather than numbers
//!
//! The *structure* of a controlled gate depends only on where the control and
//! target sit -- not on `U`, and not on the amplitude type. So the recursion
//! labels each exit with a [`Tag`] saying what that exit *means*, and
//! [`GcflobddT::mk_controlled`](super::GcflobddT::mk_controlled) substitutes the
//! numbers once, at the top. One cached node therefore serves a CNOT, a
//! controlled phase, `f64` and a 100-digit float alike, and two exits merge
//! exactly when their values coincide -- which is how a CNOT comes out with two
//! exits where a general `U` needs six.
//!
//! # Reduced form
//!
//! A node is only canonical if every connection's return map is injective:
//! [`GcflobddNode::reduce`] keeps first appearances and pushes the merge down
//! into the child, so a parent that hands a child two exits it cannot tell apart
//! is not in reduced form. That is the one place this construction has to be
//! careful, and [`Role::ControlIs`] is the answer -- see [`role_below`].

use std::cell::RefCell;
use std::rc::Rc;

use crate::gcflobdd::connection::Connection;
use crate::gcflobdd::context::Context;
use crate::gcflobdd::matmul::identity_node;
use crate::gcflobdd::matmul::node::split;
use crate::gcflobdd::node::{GcflobddNode, GcflobddNodeType, InternalNode};
use crate::grammar::{GrammarNode, GrammarNodeType};
use crate::utils::hash_cache::Rch;

/// What one exit of a block's node means.
///
/// "Diagonal" is always with respect to the block's own qubits: a block that is
/// the identity contributes a factor of 1 on the diagonal and 0 off it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(in crate::gcflobdd) enum Tag {
    /// Diagonal throughout -- the block acts as the identity.
    Identity,
    /// Diagonal apart from carrying the control, whose bit is this one.
    Control(bool),
    /// The identity apart from the target, whose `(row, column)` pair is
    /// `2 * row + column` -- an index into the gate, row-major.
    Target(usize),
    /// No assignment through here contributes anything.
    Zero,
}

/// What a block is asked to compute. Qubit indices are local to the block.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(in crate::gcflobdd) enum Role {
    /// The identity, because the control and target both lie elsewhere -- or
    /// because a parent has already resolved them.
    Identity,
    /// Holds the control; the rest of the block is the identity.
    Control(usize),
    /// Holds the control, whose bit is pinned to this value.
    ControlIs(bool, usize),
    /// Holds the target; the rest of the block is the identity.
    Target(usize),
    /// Holds both.
    Both(usize, usize),
}

impl Role {
    fn control(&self) -> Option<usize> {
        match *self {
            Role::Control(c) | Role::ControlIs(_, c) | Role::Both(c, _) => Some(c),
            Role::Identity | Role::Target(_) => None,
        }
    }

    fn target(&self) -> Option<usize> {
        match *self {
            Role::Target(t) | Role::Both(_, t) => Some(t),
            Role::Identity | Role::Control(_) | Role::ControlIs(..) => None,
        }
    }
}

/// The node for `role` over `grammar`, and what each of its exits means.
///
/// Memoised on `(grouping, role)`, which is what makes a whole layer of
/// controlled gates cheap: consecutive gates differ only in where the control
/// sits, so every block that holds neither it nor the target -- most of them --
/// is one cache hit.
pub(in crate::gcflobdd) fn controlled_node<'grammar>(
    grammar: &'grammar Rc<GrammarNode>,
    role: Role,
    context: &RefCell<Context<'grammar>>,
) -> (Rch<GcflobddNode<'grammar>>, Vec<Tag>) {
    if let Some(cached) = context.borrow().get_controlled_cache(grammar, role) {
        return cached;
    }
    let answer = match role {
        // The identity is already built directly, at every level, so defer to
        // it rather than re-derive it: sharing that node is what lets a matmul
        // prune against the operand it recognises. It goes through the cache
        // like everything else, and must -- `identity_node` recurses to the
        // leaves on every call, and most of the groupings a controlled gate
        // touches are this one.
        Role::Identity => (
            identity_node(grammar, context),
            vec![Tag::Identity, Tag::Zero],
        ),
        _ => match &grammar.node {
            GrammarNodeType::Internal(children) if children.len() == 2 => {
                match (&children[0].node, &children[1].node) {
                    (GrammarNodeType::Terminal, GrammarNodeType::Terminal) => {
                        one_qubit_node(grammar, role, context)
                    }
                    _ => split_node(grammar, role, context),
                }
            }
            _ => panic!("a controlled gate needs a binary matrix grammar"),
        },
    };
    context
        .borrow_mut()
        .set_controlled_cache(grammar, role, answer.clone());
    answer
}

/// The base case: a grouping of one qubit, `S -> a a`, whose two variables are
/// that qubit's row bit and column bit.
///
/// Tabulating it hands the canonical exit numbering to
/// [`GcflobddNode::from_table`], whose table order *is* the node's traversal
/// order, rather than asserting one here.
fn one_qubit_node<'grammar>(
    grammar: &'grammar Rc<GrammarNode>,
    role: Role,
    context: &RefCell<Context<'grammar>>,
) -> (Rch<GcflobddNode<'grammar>>, Vec<Tag>) {
    // Indexed by `2 * row + column`.
    let table = match role {
        Role::Identity => unreachable!("handled by identity_node"),
        Role::Control(0) => [
            Tag::Control(false),
            Tag::Zero,
            Tag::Zero,
            Tag::Control(true),
        ],
        // Only the pinned bit's own diagonal entry survives; the other diagonal
        // entry is the control taking the value this branch has ruled out.
        Role::ControlIs(false, 0) => [Tag::Control(false), Tag::Zero, Tag::Zero, Tag::Zero],
        Role::ControlIs(true, 0) => [Tag::Zero, Tag::Zero, Tag::Zero, Tag::Control(true)],
        Role::Target(0) => [
            Tag::Target(0),
            Tag::Target(1),
            Tag::Target(2),
            Tag::Target(3),
        ],
        Role::Both(..) => unreachable!("one qubit cannot be both the control and the target"),
        _ => unreachable!("a one-qubit grouping only has qubit 0"),
    };
    GcflobddNode::from_table(grammar, &table, context)
}

/// A grouping that splits into two, `A` covering the leading qubits.
fn split_node<'grammar>(
    grammar: &'grammar Rc<GrammarNode>,
    role: Role,
    context: &RefCell<Context<'grammar>>,
) -> (Rch<GcflobddNode<'grammar>>, Vec<Tag>) {
    let (g1, g2) = split(grammar);
    let boundary = g1.num_vars / 2;
    let (a_node, a_tags) = controlled_node(g1, role_above(role, boundary), context);

    let mut exits: Vec<Tag> = Vec::new();
    let mut intern = |tag: Tag| -> usize {
        exits
            .iter()
            .position(|seen| *seen == tag)
            .unwrap_or_else(|| {
                exits.push(tag);
                exits.len() - 1
            })
    };
    // Walking `A`'s exits in order, and each one's sub-node in its own exit
    // order, is exactly the node's traversal order -- so numbering the result's
    // exits by first appearance numbers them canonically.
    let b_connections = a_tags
        .iter()
        .map(|tag_a| {
            let (b_node, b_tags) = match tag_a {
                // Nothing below a dead branch can revive it.
                Tag::Zero => (
                    GcflobddNode::mk_no_distinction(g2, context),
                    vec![Tag::Zero],
                ),
                _ => controlled_node(g2, role_below(role, boundary, *tag_a), context),
            };
            let return_map = b_tags
                .iter()
                .map(|tag_b| intern(combine(*tag_a, *tag_b, role.target().is_some())))
                .collect();
            Connection::new(b_node, return_map, context)
        })
        .collect();

    let a_connection = Connection::new(a_node, (0..a_tags.len()).collect(), context);
    let node = context.borrow_mut().add_gcflobdd_node(GcflobddNode {
        num_exits: exits.len(),
        grammar,
        node: GcflobddNodeType::Internal(InternalNode {
            connections: vec![vec![a_connection], b_connections],
        }),
    });
    (node, exits)
}

/// `role` restricted to the leading half, whose qubits are `0..boundary`.
fn role_above(role: Role, boundary: usize) -> Role {
    let here = |q: usize| q < boundary;
    match role {
        Role::Identity => Role::Identity,
        Role::Control(c) if here(c) => Role::Control(c),
        Role::ControlIs(b, c) if here(c) => Role::ControlIs(b, c),
        Role::Target(t) if here(t) => Role::Target(t),
        Role::Both(c, t) => match (here(c), here(t)) {
            (true, true) => Role::Both(c, t),
            // Split up: this half carries whichever one it holds, and the other
            // half is asked for the rest once this one has reported.
            (true, false) => Role::Control(c),
            (false, true) => Role::Target(t),
            (false, false) => Role::Identity,
        },
        _ => Role::Identity,
    }
}

/// What the trailing half must compute, given what the leading half reported.
///
/// The `Target` arm is the subtle one. Once the target has been seen and the
/// control has not, the trailing half decides the outcome: control 0 means the
/// whole operator was the identity, so the target's entry has to have been on
/// the diagonal, and control 1 means the entry stands. When the entry is *off*
/// the diagonal those two branches are "zero" and "the entry" -- distinguishable
/// -- but the control-0 branch and an outright mismatch are both zero, and a
/// connection whose return map sends two exits to one is not reduced. Asking for
/// the pinned node instead of the free one merges them one level down, where
/// they belong.
fn role_below(role: Role, boundary: usize, reported: Tag) -> Role {
    let below = |q: usize| (q >= boundary).then(|| q - boundary);
    let control = role.control().and_then(&below);
    let target = role.target().and_then(&below);
    match reported {
        // The leading half was the identity, so nothing has been resolved yet.
        Tag::Identity => match (control, target) {
            (Some(c), Some(t)) => Role::Both(c, t),
            (Some(c), None) => match role {
                Role::ControlIs(b, _) => Role::ControlIs(b, c),
                _ => Role::Control(c),
            },
            (None, Some(t)) => Role::Target(t),
            (None, None) => Role::Identity,
        },
        // Control 0: the operator is the identity everywhere, target included.
        Tag::Control(false) => Role::Identity,
        Tag::Control(true) => match target {
            Some(t) => Role::Target(t),
            None => Role::Identity,
        },
        Tag::Target(entry) => match control {
            Some(c) if entry == 0 || entry == 3 => Role::Control(c),
            Some(c) => Role::ControlIs(true, c),
            None => Role::Identity,
        },
        Tag::Zero => unreachable!("a dead branch is never descended into"),
    }
}

/// What a block means once its two halves have both reported.
///
/// `holds_target` says whether the target is one of this block's own qubits. It
/// is what separates "the control is 0" from "this block is the identity": while
/// the target is still outside, the control's value has to be carried up to
/// whichever block holds it, and once the target is inside there is nothing left
/// for it to gate.
fn combine(above: Tag, below: Tag, holds_target: bool) -> Tag {
    let combined = match (above, below) {
        (Tag::Zero, _) | (_, Tag::Zero) => Tag::Zero,
        (Tag::Identity, other) | (other, Tag::Identity) => other,
        // The control was 0, so the operator is the identity: the target's entry
        // counts only if it sits on the diagonal, where it *is* the identity.
        (Tag::Control(false), Tag::Target(entry)) | (Tag::Target(entry), Tag::Control(false)) => {
            if entry == 0 || entry == 3 {
                Tag::Identity
            } else {
                Tag::Zero
            }
        }
        (Tag::Control(true), Tag::Target(entry)) | (Tag::Target(entry), Tag::Control(true)) => {
            Tag::Target(entry)
        }
        _ => unreachable!("a controlled gate has one control and one target"),
    };
    match combined {
        Tag::Control(false) if holds_target => Tag::Identity,
        // Control 1 with the target inside would mean the target went
        // unconsumed, which `role_below` does not allow.
        Tag::Control(true) if holds_target => {
            unreachable!("a block holding the target must resolve the control")
        }
        other => other,
    }
}
