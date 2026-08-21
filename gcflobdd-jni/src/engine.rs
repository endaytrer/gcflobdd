//! Pure-Rust engine backing the JNI layer.
//!
//! This module contains no `jni` types so it can be unit-tested without a JVM.
//! It reproduces the small subset of the `jdd.bdd.BDD` int-handle contract that
//! APKeep relies on:
//!
//! * canonical integer handles — equal formulas map to the same `i32` id
//!   (APKeep compares atomic predicates with `==`);
//! * the constants `0 = false`, `1 = true` are pre-seeded and pinned;
//! * `createVar()` saturates (pins) both the variable and its negation, exactly
//!   like jdd, so an unreferenced `not(var)` is never garbage-collected;
//! * `ref`/`deref` maintain a per-handle reference count and `gc()` reclaims
//!   unreferenced, unpinned handles (then compacts the underlying `Context`).
//!
//! The engine owns a single `Context` and is **not** thread-safe (it uses
//! `Rc`/`RefCell`), so one engine lives per thread and handles never cross
//! threads — which matches APKeep's single static engine.

use std::cell::RefCell;
use std::collections::HashMap;

use gcflobdd::gcflobdd::Gcflobdd;
use gcflobdd::gcflobdd::context::Context;
use gcflobdd::grammar::Grammar;

/// Grammar configuration for the 104-bit TCP/IP five-tuple. Both layouts keep
/// the same left-to-right leaf order — srcip(32)·dstip(32)·srcport(16)·
/// dstport(16)·proto(8) — so `create_var()`'s sequential index matches the
/// field layout for either configuration (and matches jdd/NDD).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrammarConfig {
    /// Config 1 (NDD-like): one ordinary-BDD leaf per field.
    FieldGrouped,
    /// Config 2 (aligned-balanced): a balanced binary tree recursed to single
    /// bits, with every coarse split forced onto a field boundary. Each field
    /// gets its own symbols, so equal-width subtrees are distinct grammar nodes.
    AlignedBalanced,
    /// Config 3 (aligned-balanced, width-shared): the same tree shape and the
    /// same field boundaries as [`Self::AlignedBalanced`], but every subtree of
    /// a given width is *one* grammar node shared by all five fields. Node
    /// identity in a GCFLOBDD is keyed on the grammar node's address, so this is
    /// what lets a src_ip subdiagram share nodes with a dst_port one -- the
    /// analogue of NDD's right-aligned shared variable pool.
    AlignedBalancedShared,
}

impl GrammarConfig {
    /// Decode the integer config selector passed from Java.
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            0 => Some(Self::FieldGrouped),
            1 => Some(Self::AlignedBalanced),
            2 => Some(Self::AlignedBalancedShared),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Grammar builders
// ---------------------------------------------------------------------------

/// Config 1: field-grouped BDD leaves, in NDD's field order.
fn build_field_grouped() -> Grammar {
    Grammar::new(&["S -> BDD(32) BDD(32) BDD(16) BDD(16) BDD(8)".to_string()])
        .expect("field-grouped grammar is well-formed")
}

/// Port of `tests/n_queens.rs::gen_aligned_balanced_grammar_recursive`.
/// Every field width we use (32, 16, 8) is a power of two, so this always takes
/// the balanced `num_a == num_b` branch, producing a perfect binary tree whose
/// top rule (`{prefix}{n}`) is listed first.
fn gen_aligned_balanced_recursive(
    num_exits: usize,
    symbol_prefix: &str,
    terminal_symbol: &str,
) -> Vec<(usize, String)> {
    assert!(num_exits > 1, "num_exits must be greater than 1");
    if num_exits == 2 {
        return vec![(2, format!("{terminal_symbol} {terminal_symbol}"))];
    }
    if num_exits == 3 {
        return [
            vec![(3, format!("{symbol_prefix}2 {terminal_symbol}"))],
            gen_aligned_balanced_recursive(2, symbol_prefix, terminal_symbol),
        ]
        .concat();
    }
    let num_a = num_exits / 2 + num_exits % 2;
    let num_b = num_exits / 2;
    if num_a == num_b {
        let mut productions = gen_aligned_balanced_recursive(num_a, symbol_prefix, terminal_symbol);
        productions.insert(
            0,
            (
                num_exits,
                format!(
                    "{symbol_prefix}{0} {symbol_prefix}{0}",
                    productions[0].0
                ),
            ),
        );
        return productions;
    }
    let a_productions = gen_aligned_balanced_recursive(num_a, symbol_prefix, terminal_symbol);
    let b_productions = gen_aligned_balanced_recursive(num_b, symbol_prefix, terminal_symbol);
    let mut symbols_map = vec![None; num_a + 1];
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
            format!("{symbol_prefix}{num_a} {symbol_prefix}{num_b}"),
        )))
        .rev()
        .collect()
}

/// Emit the production-rule strings for one field's aligned-balanced sub-tree,
/// e.g. `["SI32 -> SI16 SI16", "SI16 -> SI8 SI8", …, "SI2 -> a a"]`.
fn gen_aligned_balanced(num_exits: usize, symbol_prefix: &str, terminal_symbol: &str) -> Vec<String> {
    gen_aligned_balanced_recursive(num_exits, symbol_prefix, terminal_symbol)
        .into_iter()
        .map(|(sz, rule)| format!("{symbol_prefix}{sz} -> {rule}"))
        .collect()
}

/// Config 2: aligned-balanced, single-bit, field-respecting. Coarse rules split
/// only on field boundaries; each field is then a perfect binary tree of single
/// bits. A distinct prefix per field avoids symbol clashes between equal-width
/// fields. Rules are concatenated root-first, coarse-before-fine, so the
/// reverse parser resolves every symbol.
fn build_aligned_balanced() -> Grammar {
    let coarse = vec![
        "S -> A B".to_string(),
        "A -> SI32 DI32".to_string(), // srcip | dstip     (64 = 32 | 32)
        "B -> SP16 C".to_string(),    // srcport | rest    (40 = 16 | 24)
        "C -> DP16 PR8".to_string(),  // dstport | proto   (24 = 16 | 8)
    ];
    let rules = [
        coarse,
        gen_aligned_balanced(32, "SI", "a"),
        gen_aligned_balanced(32, "DI", "a"),
        gen_aligned_balanced(16, "SP", "a"),
        gen_aligned_balanced(16, "DP", "a"),
        gen_aligned_balanced(8, "PR", "a"),
    ]
    .concat();
    Grammar::new(&rules).expect("aligned-balanced grammar is well-formed")
}

/// Config 3: aligned-balanced with width-shared subtrees. Same shape as
/// [`build_aligned_balanced`] -- the coarse rules split only on field boundaries
/// and every field is a perfect binary tree of single bits -- but a subtree of a
/// given width is named once (`W32`, `W16`, ...) and reused by every field of
/// that width, so the five fields share grammar nodes instead of each owning a
/// private copy.
fn build_aligned_balanced_shared() -> Grammar {
    Grammar::new(&[
        "S -> A B".to_string(),    // 104 = 64 | 40
        "A -> W32 W32".to_string(), // srcip | dstip     (64 = 32 | 32)
        "B -> W16 C".to_string(),   // srcport | rest    (40 = 16 | 24)
        "C -> W16 W8".to_string(),  // dstport | proto   (24 = 16 | 8)
        "W32 -> W16 W16".to_string(),
        "W16 -> W8 W8".to_string(),
        "W8 -> W4 W4".to_string(),
        "W4 -> W2 W2".to_string(),
        "W2 -> a a".to_string(),
    ])
    .expect("width-shared aligned-balanced grammar is well-formed")
}

fn build_grammar(config: GrammarConfig) -> Grammar {
    match config {
        GrammarConfig::FieldGrouped => build_field_grouped(),
        GrammarConfig::AlignedBalanced => build_aligned_balanced(),
        GrammarConfig::AlignedBalancedShared => build_aligned_balanced_shared(),
    }
}

// ---------------------------------------------------------------------------
// Handle registry / engine
// ---------------------------------------------------------------------------

/// The reserved handle for the constant-false function.
pub const FALSE: i32 = 0;
/// The reserved handle for the constant-true function.
pub const TRUE: i32 = 1;

struct Slot {
    g: Gcflobdd<'static>,
    refcount: i32,
    /// Pinned handles (constants, variables, and variable negations) are never
    /// garbage-collected regardless of reference count — matching jdd's
    /// saturated nodes.
    pinned: bool,
}

pub struct Engine {
    grammar: &'static Grammar,
    context: RefCell<Context<'static>>,
    /// Handle `i32` → slot; `None` marks a freed (reusable) index.
    slab: Vec<Option<Slot>>,
    /// Reusable handle indices produced by `gc`.
    free: Vec<usize>,
    /// Canonical formula → handle. Keyed by GCFLOBDD's per-`Context` canonical
    /// `Eq`/`Hash`, this is what makes equal formulas share one handle id.
    intern: HashMap<Gcflobdd<'static>, i32>,
    /// Number of variables handed out by `create_var` so far (0..num_vars).
    next_var: usize,
    num_vars: usize,
}

impl Engine {
    pub fn new(config: GrammarConfig) -> Self {
        // Leak the grammar to obtain a `&'static Grammar`: the engine's
        // `Context` and every `Gcflobdd` borrow from it for the engine's whole
        // life. One small allocation per engine (engines are few) — freeing it
        // is unsound while any handle is live, so we intentionally leak it.
        let grammar: &'static Grammar = Box::leak(Box::new(build_grammar(config)));
        let num_vars = grammar.num_vars();
        let context = Context::new();

        let g_false = Gcflobdd::mk_false(grammar, &context);
        let g_true = Gcflobdd::mk_true(grammar, &context);

        let mut intern = HashMap::default();
        intern.insert(g_false.clone(), FALSE);
        intern.insert(g_true.clone(), TRUE);

        let slab = vec![
            Some(Slot { g: g_false, refcount: 0, pinned: true }),
            Some(Slot { g: g_true, refcount: 0, pinned: true }),
        ];

        Self {
            grammar,
            context,
            slab,
            free: Vec::new(),
            intern,
            next_var: 0,
            num_vars,
        }
    }

    pub fn num_vars(&self) -> usize {
        self.num_vars
    }

    fn slot(&self, h: i32) -> Option<&Slot> {
        self.slab.get(h as usize).and_then(|s| s.as_ref())
    }

    fn resolve(&self, h: i32) -> &Gcflobdd<'static> {
        &self
            .slot(h)
            .expect("handle refers to a live GCFLOBDD node")
            .g
    }

    /// Look up an existing handle for `g`, or allocate a fresh (unpinned,
    /// refcount-0) one. Reproduces jdd's "equal BDD ⇒ equal node id" contract.
    fn intern(&mut self, g: Gcflobdd<'static>) -> i32 {
        if let Some(&h) = self.intern.get(&g) {
            return h;
        }
        let idx = self.free.pop().unwrap_or(self.slab.len());
        let h = idx as i32;
        let slot = Some(Slot { g: g.clone(), refcount: 0, pinned: false });
        if idx == self.slab.len() {
            self.slab.push(slot);
        } else {
            self.slab[idx] = slot;
        }
        self.intern.insert(g, h);
        h
    }

    fn pin(&mut self, h: i32) {
        if let Some(slot) = self.slab.get_mut(h as usize).and_then(|s| s.as_mut()) {
            slot.pinned = true;
        }
    }

    // --- boolean ops -------------------------------------------------------

    pub fn and(&mut self, a: i32, b: i32) -> i32 {
        let ga = self.resolve(a).clone();
        let gb = self.resolve(b).clone();
        let res = ga.mk_and(&gb, &self.context);
        self.intern(res)
    }

    pub fn or(&mut self, a: i32, b: i32) -> i32 {
        let ga = self.resolve(a).clone();
        let gb = self.resolve(b).clone();
        let res = ga.mk_or(&gb, &self.context);
        self.intern(res)
    }

    pub fn not(&mut self, a: i32) -> i32 {
        let res = self.resolve(a).clone().mk_not();
        self.intern(res)
    }

    /// `a AND NOT b`. The hot operation in atomic-predicate splitting; done here
    /// it costs one JNI crossing and one cached `mk_and` instead of two.
    pub fn diff(&mut self, a: i32, b: i32) -> i32 {
        let ga = self.resolve(a).clone();
        let nb = self.resolve(b).clone().mk_not();
        let res = ga.mk_and(&nb, &self.context);
        self.intern(res)
    }

    pub fn xor(&mut self, a: i32, b: i32) -> i32 {
        let ga = self.resolve(a).clone();
        let gb = self.resolve(b).clone();
        let res = ga.mk_xor(&gb, &self.context);
        self.intern(res)
    }

    // --- variables ---------------------------------------------------------

    /// Declare the next variable (flat index `next_var`), returning its handle.
    /// Both the variable and its negation are pinned, matching jdd's
    /// `saturate(var); saturate(nvar)`.
    pub fn create_var(&mut self) -> i32 {
        assert!(
            self.next_var < self.num_vars,
            "createVar called more than num_vars ({}) times",
            self.num_vars
        );
        let i = self.next_var;
        self.next_var += 1;
        let proj = Gcflobdd::mk_projection(i, self.grammar, &self.context);
        let nproj = proj.mk_not();
        let h = self.intern(proj);
        self.pin(h);
        let nh = self.intern(nproj);
        self.pin(nh);
        h
    }

    // --- reference counting / gc ------------------------------------------

    pub fn add_ref(&mut self, h: i32) -> i32 {
        if let Some(slot) = self.slab.get_mut(h as usize).and_then(|s| s.as_mut())
            && !slot.pinned
        {
            slot.refcount += 1;
        }
        h
    }

    pub fn deref(&mut self, h: i32) -> i32 {
        if let Some(slot) = self.slab.get_mut(h as usize).and_then(|s| s.as_mut())
            && !slot.pinned
            && slot.refcount > 0
        {
            slot.refcount -= 1;
        }
        h
    }

    pub fn get_ref(&self, h: i32) -> i32 {
        match self.slot(h) {
            Some(slot) if slot.pinned => i32::MAX,
            Some(slot) => slot.refcount,
            None => 0,
        }
    }

    /// Reclaim every unpinned handle whose reference count has dropped to zero,
    /// then compact the underlying `Context`. Returns the resulting node count.
    pub fn gc(&mut self) -> i32 {
        let dead: Vec<usize> = self
            .slab
            .iter()
            .enumerate()
            .filter_map(|(i, slot)| match slot {
                Some(s) if !s.pinned && s.refcount <= 0 => Some(i),
                _ => None,
            })
            .collect();
        for i in dead {
            if let Some(slot) = self.slab[i].take() {
                self.intern.remove(&slot.g); // drops the Gcflobdd (releases Rcs)
            }
            self.free.push(i);
        }
        self.context.borrow_mut().gc();
        self.context.borrow().node_count() as i32
    }

    // --- queries -----------------------------------------------------------

    pub fn is_valid(&self, h: i32) -> bool {
        self.slot(h).is_some()
    }

    /// Raw number of satisfying assignments (jdd `satCount` semantics). The core
    /// returns `log2(count)`; we exponentiate back into a `double`. `2^104`
    /// fits comfortably in an `f64`.
    pub fn sat_count(&self, h: i32) -> f64 {
        2.0_f64.powf(self.resolve(h).sat_count())
    }

    /// One satisfying assignment as a `Vec` of length `num_vars`, indexed by
    /// flat variable index: `1` = true, `0` = false, `-1` = don't-care.
    /// Matches jdd `oneSat(bdd, buffer)`. An unsatisfiable function yields all
    /// don't-cares.
    pub fn one_sat(&self, h: i32) -> Vec<i32> {
        let mut buf = vec![-1i32; self.num_vars];
        if let Some(path) = self.resolve(h).find_one_satisfiable_assignment() {
            for (i, v) in path.into_iter().enumerate() {
                if i >= buf.len() {
                    break;
                }
                buf[i] = match v {
                    Some(true) => 1,
                    Some(false) => 0,
                    None => -1,
                };
            }
        }
        buf
    }

    /// Global reachable-node count of the shared `Context` (GCFLOBDD has no
    /// cheap per-handle node count; this is the diagnostic APKeep needs).
    pub fn node_count(&self) -> i32 {
        self.context.borrow().node_count() as i32
    }

    /// Nodes reachable from *this diagram's* root, and the connections leaving
    /// them -- this crate's own counting convention (one edge per connection, no
    /// return-map entries). Unlike [`Self::node_count`] this is per-handle.
    pub fn diagram_size(&self, h: i32) -> (usize, usize) {
        self.resolve(h).count_nodes_and_edges()
    }

    /// The same diagram measured the way the reference C++ CFLOBDD counts:
    /// two edges per connection plus the entries of every distinct return map.
    /// See BENCHMARKS.md, "Reading the size column".
    pub fn conv_size(&self, h: i32) -> (usize, usize) {
        self.resolve(h).count_cflobdd_convention()
    }

    /// Rough heap footprint of the shared `Context`, in bytes.
    pub fn memory_usage(&self) -> i64 {
        self.context.borrow().size_estimate() as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grammars_have_104_vars() {
        assert_eq!(build_field_grouped().num_vars(), 104);
        assert_eq!(build_aligned_balanced().num_vars(), 104);
        assert_eq!(build_aligned_balanced_shared().num_vars(), 104);
    }

    /// Every selector Java can pass must decode, and each must be its own layout.
    #[test]
    fn every_config_selector_decodes() {
        assert_eq!(GrammarConfig::from_i32(0), Some(GrammarConfig::FieldGrouped));
        assert_eq!(GrammarConfig::from_i32(1), Some(GrammarConfig::AlignedBalanced));
        assert_eq!(
            GrammarConfig::from_i32(2),
            Some(GrammarConfig::AlignedBalancedShared)
        );
        assert_eq!(GrammarConfig::from_i32(3), None);
        assert_eq!(GrammarConfig::from_i32(-1), None);
    }

    /// The two aligned-balanced layouts agree on *what* they represent -- same
    /// 104 variables in the same order -- and differ only in grammar-node
    /// sharing. Semantics must match exactly; the shared layout is what makes
    /// the diagram smaller, because a src_ip subtree and a dst_port subtree of
    /// the same width become the same node.
    #[test]
    fn shared_and_unshared_aligned_balanced_agree() {
        let mut a = Engine::new(GrammarConfig::AlignedBalanced);
        let mut b = Engine::new(GrammarConfig::AlignedBalancedShared);
        let mut counts = Vec::new();
        for engine in [&mut a, &mut b] {
            let vars: Vec<i32> = (0..104).map(|_| engine.create_var()).collect();
            // one bit from each field: srcip[0], dstip[0], srcport[0], dstport[0], proto[0]
            let mut acc = TRUE;
            for &i in &[0usize, 32, 64, 80, 96] {
                acc = engine.and(acc, vars[i]);
            }
            assert_eq!(engine.sat_count(acc), 2.0_f64.powi(104 - 5));
            counts.push(engine.diagram_size(acc));
        }
        let (unshared_nodes, unshared_edges) = counts[0];
        let (shared_nodes, shared_edges) = counts[1];
        assert!(
            shared_nodes < unshared_nodes && shared_edges < unshared_edges,
            "width-sharing should shrink the diagram: unshared {:?}, shared {:?}",
            counts[0],
            counts[1]
        );
    }

    /// `diff` is `and(a, not b)` and `xor` is the real thing, both canonical.
    #[test]
    fn diff_and_xor_match_their_definitions() {
        let mut e = Engine::new(GrammarConfig::AlignedBalancedShared);
        let x = e.create_var();
        let y = e.create_var();
        let nx = e.not(x);
        let ny = e.not(y);

        let expected_diff = e.and(x, ny);
        assert_eq!(e.diff(x, y), expected_diff);
        assert_eq!(e.diff(x, x), FALSE);
        assert_eq!(e.diff(x, FALSE), x);

        let a = e.and(x, ny);
        let b = e.and(nx, y);
        let expected_xor = e.or(a, b);
        assert_eq!(e.xor(x, y), expected_xor);
        assert_eq!(e.xor(x, x), FALSE);
        assert_eq!(e.xor(x, FALSE), x);
    }

    /// Per-handle sizes are per *handle*, not the global context count.
    #[test]
    fn diagram_size_is_per_handle() {
        let mut e = Engine::new(GrammarConfig::AlignedBalancedShared);
        let (tn, te) = e.diagram_size(TRUE);
        assert!(tn >= 1, "the constant still has a root node");

        let x = e.create_var();
        let (xn, xe) = e.diagram_size(x);
        assert!(xn > tn || xe > te, "a projection is bigger than a constant");

        // The reference convention counts more of the same diagram, never less.
        let (cn, ce) = e.conv_size(x);
        assert_eq!(cn, xn, "node counts are convention-free");
        assert!(ce >= xe, "the reference counts two edges per connection");

        // ... and the global count is a different, larger number.
        assert!(e.node_count() as usize >= xn);
    }

    #[test]
    fn constants_are_pinned_zero_and_one() {
        let engine = Engine::new(GrammarConfig::FieldGrouped);
        assert_eq!(engine.sat_count(FALSE), 0.0);
        assert_eq!(engine.sat_count(TRUE), 2.0_f64.powi(104));
        assert_eq!(engine.get_ref(FALSE), i32::MAX);
        assert_eq!(engine.get_ref(TRUE), i32::MAX);
    }

    #[test]
    fn not_of_constants() {
        let mut engine = Engine::new(GrammarConfig::FieldGrouped);
        assert_eq!(engine.not(FALSE), TRUE);
        assert_eq!(engine.not(TRUE), FALSE);
    }

    #[test]
    fn canonical_ids_and_boolean_identities() {
        for config in [
            GrammarConfig::FieldGrouped,
            GrammarConfig::AlignedBalanced,
            GrammarConfig::AlignedBalancedShared,
        ] {
            let mut engine = Engine::new(config);
            let v0 = engine.create_var();
            let v1 = engine.create_var();

            // and(v, not v) == false, or(v, not v) == true
            let nv0 = engine.not(v0);
            assert_eq!(engine.and(v0, nv0), FALSE);
            assert_eq!(engine.or(v0, nv0), TRUE);

            // Identity laws collapse to the operands / constants.
            assert_eq!(engine.and(TRUE, v0), v0);
            assert_eq!(engine.or(FALSE, v0), v0);
            assert_eq!(engine.and(FALSE, v0), FALSE);
            assert_eq!(engine.or(TRUE, v0), TRUE);

            // Equal formulas built two ways share one canonical id.
            let a = engine.and(v0, v1);
            let b = engine.and(v1, v0);
            assert_eq!(a, b);
        }
    }

    #[test]
    fn sat_count_of_projection() {
        let mut engine = Engine::new(GrammarConfig::FieldGrouped);
        let v0 = engine.create_var();
        // A single-variable projection is satisfied by exactly half the space.
        assert_eq!(engine.sat_count(v0), 2.0_f64.powi(103));
    }

    #[test]
    fn one_sat_reports_forced_variable() {
        let mut engine = Engine::new(GrammarConfig::FieldGrouped);
        let v0 = engine.create_var();
        let assignment = engine.one_sat(v0);
        assert_eq!(assignment.len(), 104);
        // f = x0 forces x0 = true on any satisfying path; the rest are free.
        assert_eq!(assignment[0], 1);

        // Unsatisfiable => all don't-cares.
        assert!(engine.one_sat(FALSE).iter().all(|&b| b == -1));
    }

    #[test]
    fn gc_reclaims_unreferenced_but_keeps_pinned() {
        let mut engine = Engine::new(GrammarConfig::FieldGrouped);
        let v0 = engine.create_var();
        let nv0 = engine.not(v0); // pinned by create_var
        let v1 = engine.create_var();

        // An unreferenced temporary.
        let tmp = engine.or(v0, v1);
        assert!(engine.is_valid(tmp));

        engine.gc();

        // Pinned handles survive; the unreferenced temporary is reclaimed.
        assert!(engine.is_valid(v0));
        assert!(engine.is_valid(nv0));
        assert!(engine.is_valid(v1));
        assert!(engine.is_valid(FALSE));
        assert!(engine.is_valid(TRUE));
        assert!(!engine.is_valid(tmp));

        // A ref'd temporary survives gc.
        let kept = engine.and(v0, v1);
        engine.add_ref(kept);
        engine.gc();
        assert!(engine.is_valid(kept));
    }
}
