//! C ABI bindings for the boolean GCFLOBDD surface.
//!
//! Safety model: a `GcflSession` owns a heap-pinned `Grammar` and `Context`.
//! `GcflBool` handles borrow (via transmuted `'static`) from that session.
//! All `GcflBool` handles derived from a session MUST be freed before the
//! session itself is freed. Passing a handle from one session to another
//! session's ops is undefined behavior.

use std::cell::RefCell;
use std::ffi::{CStr, c_char};

use crate::gcflobdd::Gcflobdd;
use crate::gcflobdd::context::Context;
use crate::grammar::Grammar;

pub struct GcflSession {
    context: Box<RefCell<Context<'static>>>,
    grammar: Box<Grammar>,
}

pub struct GcflBool {
    inner: Gcflobdd<'static>,
}

impl GcflSession {
    fn grammar_ref(&self) -> &'static Grammar {
        unsafe { std::mem::transmute::<&Grammar, &'static Grammar>(&*self.grammar) }
    }
}

fn boxed<T>(value: T) -> *mut T {
    Box::into_raw(Box::new(value))
}

fn into_bool_ptr(b: Gcflobdd<'static>) -> *mut GcflBool {
    boxed(GcflBool { inner: b })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_session_new_bdd(num_vars: usize) -> *mut GcflSession {
    let grammar = Box::new(Grammar::new_bdd(num_vars));
    let context = Box::new(RefCell::new(Context::default()));
    boxed(GcflSession { context, grammar })
}

/// Create a session from an array of NUL-terminated production-rule strings.
/// Returns NULL on parse failure or if any rule pointer is NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_session_new_from_rules(
    rules: *const *const c_char,
    n_rules: usize,
) -> *mut GcflSession {
    if rules.is_null() && n_rules != 0 {
        return std::ptr::null_mut();
    }
    let slice = unsafe { std::slice::from_raw_parts(rules, n_rules) };
    let mut owned = Vec::with_capacity(n_rules);
    for &p in slice {
        if p.is_null() {
            return std::ptr::null_mut();
        }
        let s = match unsafe { CStr::from_ptr(p) }.to_str() {
            Ok(s) => s.to_owned(),
            Err(_) => return std::ptr::null_mut(),
        };
        owned.push(s);
    }
    match Grammar::new(&owned) {
        Ok(g) => {
            let grammar = Box::new(g);
            let context = Box::new(RefCell::new(Context::default()));
            boxed(GcflSession { context, grammar })
        }
        Err(_) => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_session_free(session: *mut GcflSession) {
    if !session.is_null() {
        drop(unsafe { Box::from_raw(session) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_session_gc(session: *mut GcflSession) {
    let session = unsafe { &*session };
    session.context.borrow_mut().gc();
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_session_node_count(session: *const GcflSession) -> usize {
    unsafe { &*session }.context.borrow().node_count()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_bool_mk_true(session: *mut GcflSession) -> *mut GcflBool {
    let s = unsafe { &*session };
    into_bool_ptr(Gcflobdd::mk_true(s.grammar_ref(), &s.context))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_bool_mk_false(session: *mut GcflSession) -> *mut GcflBool {
    let s = unsafe { &*session };
    into_bool_ptr(Gcflobdd::mk_false(s.grammar_ref(), &s.context))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_bool_mk_projection(
    session: *mut GcflSession,
    var_index: usize,
) -> *mut GcflBool {
    let s = unsafe { &*session };
    into_bool_ptr(Gcflobdd::mk_projection(var_index, s.grammar_ref(), &s.context))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_bool_clone(bdd: *const GcflBool) -> *mut GcflBool {
    into_bool_ptr(unsafe { &*bdd }.inner.clone())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_bool_free(bdd: *mut GcflBool) {
    if !bdd.is_null() {
        drop(unsafe { Box::from_raw(bdd) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_bool_eq(a: *const GcflBool, b: *const GcflBool) -> bool {
    unsafe { &*a }.inner == unsafe { &*b }.inner
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_bool_not(bdd: *const GcflBool) -> *mut GcflBool {
    into_bool_ptr(unsafe { &*bdd }.inner.mk_not())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_bool_and(
    session: *mut GcflSession,
    lhs: *const GcflBool,
    rhs: *const GcflBool,
) -> *mut GcflBool {
    let s = unsafe { &*session };
    into_bool_ptr(unsafe { &*lhs }.inner.mk_and(&unsafe { &*rhs }.inner, &s.context))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_bool_or(
    session: *mut GcflSession,
    lhs: *const GcflBool,
    rhs: *const GcflBool,
) -> *mut GcflBool {
    let s = unsafe { &*session };
    into_bool_ptr(unsafe { &*lhs }.inner.mk_or(&unsafe { &*rhs }.inner, &s.context))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_bool_xor(
    session: *mut GcflSession,
    lhs: *const GcflBool,
    rhs: *const GcflBool,
) -> *mut GcflBool {
    let s = unsafe { &*session };
    into_bool_ptr(unsafe { &*lhs }.inner.mk_xor(&unsafe { &*rhs }.inner, &s.context))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_bool_nand(
    session: *mut GcflSession,
    lhs: *const GcflBool,
    rhs: *const GcflBool,
) -> *mut GcflBool {
    let s = unsafe { &*session };
    into_bool_ptr(unsafe { &*lhs }.inner.mk_nand(&unsafe { &*rhs }.inner, &s.context))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_bool_nor(
    session: *mut GcflSession,
    lhs: *const GcflBool,
    rhs: *const GcflBool,
) -> *mut GcflBool {
    let s = unsafe { &*session };
    into_bool_ptr(unsafe { &*lhs }.inner.mk_nor(&unsafe { &*rhs }.inner, &s.context))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_bool_xnor(
    session: *mut GcflSession,
    lhs: *const GcflBool,
    rhs: *const GcflBool,
) -> *mut GcflBool {
    let s = unsafe { &*session };
    into_bool_ptr(unsafe { &*lhs }.inner.mk_xnor(&unsafe { &*rhs }.inner, &s.context))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_bool_implies(
    session: *mut GcflSession,
    lhs: *const GcflBool,
    rhs: *const GcflBool,
) -> *mut GcflBool {
    let s = unsafe { &*session };
    into_bool_ptr(unsafe { &*lhs }.inner.mk_implies(&unsafe { &*rhs }.inner, &s.context))
}

/// Find a satisfying assignment.
///
/// On success writes the number of variables to `*out_len` and returns a
/// heap-allocated array of length `*out_len` where each byte is:
///   - `0` = must be false
///   - `1` = must be true
///   - `-1` (as int8) = don't care
/// The caller must release the array with `gcfl_assignment_free`.
/// On failure (unsatisfiable) returns NULL and writes 0 to `*out_len`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_bool_find_sat(
    bdd: *const GcflBool,
    out_len: *mut usize,
) -> *mut i8 {
    let bdd = unsafe { &*bdd };
    match bdd.inner.find_one_satisfiable_assignment() {
        None => {
            if !out_len.is_null() {
                unsafe { *out_len = 0 };
            }
            std::ptr::null_mut()
        }
        Some(assignment) => {
            let bytes: Vec<i8> = assignment
                .into_iter()
                .map(|v| match v {
                    None => -1i8,
                    Some(false) => 0,
                    Some(true) => 1,
                })
                .collect();
            let len = bytes.len();
            if !out_len.is_null() {
                unsafe { *out_len = len };
            }
            let boxed_slice = bytes.into_boxed_slice();
            Box::into_raw(boxed_slice) as *mut i8
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn gcfl_assignment_free(ptr: *mut i8, len: usize) {
    if ptr.is_null() {
        return;
    }
    let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
    drop(unsafe { Box::from_raw(slice as *mut [i8]) });
}
