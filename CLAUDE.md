# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Rust implementation of **GCFLOBDD** (Generalized Context-Free-Language Ordered Binary Decision Diagrams) — a BDD variant where variable orderings are structured by a user-supplied context-free grammar, generalizing CFLOBDDs. The crate exposes both a boolean (`Gcflobdd`) and integer (`GcflobddInt`) decision-diagram surface, plus a plain `Bdd` used as a leaf/building block. The crate is published as `lib`, `staticlib`, and `cdylib`.

### Relation to the C++ CFLOBDD project

This crate is a Rust reimplementation and generalization of the C++ CFLOBDD / Weighted-CFLOBDD research library at `~/projects/cflobdd` (main source under `~/projects/cflobdd/CFLOBDD/`). The "G" adds arbitrary user-supplied grammars; the original uses a fixed CFLOBDD structure. The Rust rewrite targets memory safety and improved performance.

When features here have analogues in the C++ project (weighted edges, return-maps, matrix/vector ops, quantum-algorithm tests like GHZ / BV / Grover / QFT / Shor, terminal types like `float_boost` / `complex_fb_mul`), treat the C++ source as the reference implementation — but not a 1:1 port. The Rust layering (`Gcflobdd` → `ConnectionT` → `GcflobddNode` → `Bdd`, with a shared `Context` for hash-consing) corresponds loosely to the C++ user-facing / top-node / proto-CFLOBDD layers but does not match API-for-API.

## Build / Test / Run

```bash
# Build
cargo build
cargo build --release

# Library unit tests (inside src/**)
cargo test --lib
cargo test --lib <test_name>         # single test by name substring

# Integration benchmarks (each has its own main, harness = false)
cargo run --release --test n_queens -- --n 8 --grammar aligned-balanced
cargo run --release --test xor       -- --n 18
```

N-queens supports `--grammar {default,balanced,aligned-balanced,full-aligned-balanced,ndd,bdd}`. Choice of grammar dramatically changes node count / runtime — it's the primary knob when experimenting.

## Feature flags (in `Cargo.toml`)

- `default = ["fx-hash", "complex"]`
- `fx-hash` — use `rustc-hash` (FxHash) everywhere instead of `std` hashers. Nearly every module has `#[cfg(feature = "fx-hash")]` branches; keep both branches in sync when editing hash-map/hasher imports.
- `complex` — pulls in `rug` for arbitrary-precision complex arithmetic (`src/gcflobdd/complex/`).
- `sync` — enables `src/sync/` (parking_lot-based). Currently only scaffolding.
- `separate_reduce_map` — switches op implementation from `mk_op_pair_map` (fused pair-product + reduce) to `mk_op` (two separate passes). Selected at compile time via `define_bool_op!` / `define_int_op!` macros in `src/gcflobdd/mod.rs`.

## Architecture

### Layered structure

```
Gcflobdd / GcflobddInt       src/gcflobdd/mod.rs          (public API, boolean + integer ops)
   └─ ConnectionT            src/gcflobdd/connection.rs   (entry_point + return_map)
        └─ GcflobddNode      src/gcflobdd/node.rs         (DontCare | Fork | Internal | Bdd)
             └─ Bdd          src/gcflobdd/bdd/            (plain BDD used as a GCFLOBDD leaf)
Grammar                      src/grammar/mod.rs           (parsed CFG that shapes the node tree)
Context                      src/gcflobdd/context.rs      (hash-consing tables + op caches + GC)
utils::hash_cache::Rch<T>    src/utils/hash_cache.rs      (Rc<HashCached<T>>: precomputed hash)
```

- **`Grammar`** is parsed from production rules like `"S1 -> S0 S0"`, `"S0 -> a"`, or `"BDD(n)"` leaves. Rules are non-recursive and parsed in reverse (last rule first) so symbols resolve bottom-up. `Grammar::new_bdd(n)` builds a pure BDD grammar of `n` vars. The `Grammar` outlives all diagrams via the `'grammar` lifetime parameter that threads through the whole API.
- **`GcflobddNode`** mirrors the grammar tree: `Internal` holds a `Vec<Vec<Connection>>` (one layer per grammar child), `Bdd` leaves wrap a plain BDD, `DontCare` / `Fork` are the base 1-var shapes.
- **`Context`** is the single shared hash-cons table + memoization layer. It is always passed as `&RefCell<Context<'grammar>>`. All construction (`mk_projection`, `mk_true`, `mk_and`, ...) takes `&context`. Every node / return-map / reduce-matrix goes through `add_*` methods that canonicalize via `Rch<T>`.
- **`Rch<T>` = `Rc<HashCached<T>>`** — hashes are precomputed at insertion and pointer identity (`Rc::as_ptr`) is used as the cache key for op/pair-product/pair-map/reduction caches. This is a recent refactor (see commit `d09ac5d`); when adding caches prefer pointer-based keys over re-hashing.

### Boolean op pipeline

`define_bool_op!` / `define_int_op!` macros generate methods like `mk_and`, `mk_xor`, `mk_add`. Each one:
1. Checks `op_cache[O]` keyed by `(lhs, rhs)`.
2. Dispatches to either `mk_op_pair_map` (default, fused) or `mk_op` (two-pass, when `separate_reduce_map` is set).
3. Stores the result back into `op_cache[O]`.

`pair_product` + `map` (from `GcflobddT::map`) implement the generic combine-then-reduce path; `mk_op_pair_map` fuses them via a precomputed `reduce_matrix`.

### Garbage collection

`Context::gc()` clears all ephemeral caches, then walks `gcflobdd_node_table` / `bdd_node_table` removing entries with `Rc::strong_count == 1` (only referenced by the table itself), recursively. Call it between phases in long-running computations — both benchmark `main`s do this at the end. Any new cache added to `Context` must be cleared inside `gc()` before the strong-count sweep, otherwise live-but-unreachable nodes will be kept alive.

### Testing layout

- Library unit tests: `#[cfg(test)] mod tests` inside modules (`src/gcflobdd/tests.rs`, `src/gcflobdd/bdd/tests.rs`, grammar tests in `src/grammar/mod.rs`).
- Integration benchmarks in `tests/` declared with `harness = false` — they are standalone binaries, not `#[test]` functions. Invoke with `cargo run --test <name> -- <args>`, not `cargo test`.
