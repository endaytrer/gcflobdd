# gcflobdd (compile-time grammar)

A GCFLOBDD engine whose grammar is fixed **at compile time**. A grammar is
declared with the `declare_grammar!` macro; every grammar level becomes a
distinct, statically-typed node type, and each such type hash-conses itself into
its own thread-local interning table.

```rust
use gcflobdd::declare_grammar;
use gcflobdd::grammar::declarative::{GhddGrammar, Unit};

declare_grammar! {
    Pair => Unit, Unit;     // NUM_VARS = 2
    Quad => Pair, Pair;     // NUM_VARS = 4
}

assert_eq!(Quad::NUM_VARS, 4);
let _g = Quad::mk_distinction(0);
```

## Layout

- `gcflobdd-macros/` — the `declare_grammar!` procedural macro. For each rule it
  emits a marker `struct`, its `GhddGrammar`/`RecursiveGrammar` impls, and a
  local **connection wrapper** per distinct connection type plus a **grouping
  wrapper** per grammar — each owning its interning table and operation caches
  (orphan/coherence rules forbid attaching those to the foreign generic node
  types from a downstream crate).
- `src/gcflobdd/connection_graph.rs` — the node model: the `Grouping` and
  `Connection` traits (both hash-consed via `intern`), `OpCached` (per-type
  `pair_product`/`pair_map`/`reduce` caches keyed on `Rc` pointer identity), and
  the generic `Connection1`/`ConnectionK`/`RecursiveGrouping` building blocks.
- `src/gcflobdd/typed.rs` — `GcflobddT<G, T>`, the user-facing diagram: a
  grammar `G`'s entry grouping plus a typed return map (`Vec<T>`). The boolean
  specialization `Gcflobdd<G>` provides `mk_projection`/`mk_and`/`mk_or`/`mk_not`/…
  and `find_one_satisfiable_assignment`. It carries no per-diagram op cache — the
  grouping/connection caches already memoize the work.
- `src/grammar/declarative.rs` — the compile-time grammar traits (`GhddGrammar`,
  `RecursiveGrammar`) and the built-in `Unit` / `BddGrammar<N>` grammars.
- `src/utils/` — `HashCached`/`Rch` (hash-cached `Rc`) and the `HashSet`/`HashMap`
  aliases that follow the `fx-hash` feature.

## Features

- `fx-hash` (default) — use `rustc-hash` for the interning tables and caches.

## Status

Construction (`mk_distinction`/`mk_no_distinction`), hash-consing, and the three
core operations — `pair_product`, `reduce`, and `pair_map` (Apply, implemented as
`pair_product` + relabel + `reduce`) — work for recursive grammars, each cached on
the per-type operation tables. On top of those, the typed wrapper
`GcflobddT<G, T>` (`src/gcflobdd/typed.rs`) gives the boolean `Gcflobdd<G>` with
the usual connectives, `find_one_satisfiable_assignment`, and `find_one_path_to`;
`tests/n_queens.rs` solves NQueens over `build.rs`-generated grammars (n = 8..=14).
BDD groupings (`BddGrouping<N>`), a per-diagram operation cache, and a per-type GC
for the pointer-keyed caches remain unimplemented.
