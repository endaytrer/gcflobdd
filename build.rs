//! Generates the compile-time NQueens grammars used by `tests/n_queens.rs`.
//!
//! The main engine builds the "default" NQueens grammar at runtime from strings
//! (`S2 -> S1 (×n); S1 -> a (×n)`). Here the grammar is compile-time, so we emit,
//! for each board size `n`, the equivalent pair of `declare_grammar!` rules:
//!
//! ```ignore
//! Q{n}Row   => Unit (×n);       // one row: n cell variables
//! Q{n}Board => Q{n}Row (×n);    // the board: n rows  →  n*n variables
//! ```
//!
//! All sizes go into a single `declare_grammar!` block so the macro can share the
//! connection wrappers for common component suffixes.

use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let sizes = 8..=14;
    //
    let mut rules = String::new();
    for n in sizes {
        let row = vec!["Unit"; n].join(", ");
        rules.push_str(&format!("    Q{n}Row => {row};\n"));
        let board = vec![format!("Q{n}Row"); n].join(", ");
        rules.push_str(&format!("    Q{n}Board => {board};\n"));
    }

    let out = format!("gcflobdd::declare_grammar! {{\n{rules}}}\n");

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR set by cargo");
    let dest = Path::new(&out_dir).join("queens_grammars.rs");
    fs::write(&dest, out).expect("write generated grammars");

    println!("cargo:rerun-if-changed=build.rs");
}
