use std::{cell::RefCell, hash::Hash, rc::Rc, thread::LocalKey};

use crate::utils::{
    HashMap, HashSet,
    hash_cache::{HashCached, Rch},
};

pub mod connection;

pub mod grouping;

/// Intern `value` into a per-type table: if an equal value is already present,
/// return the canonical `Rch`; otherwise insert and return the new one. After
/// interning, `Rc::as_ptr` is a valid identity for the value (used by the
/// operation caches). Mirrors `Context::add_*` from the old engine.
fn intern_in<T: Hash + Eq + 'static>(table: &RefCell<HashSet<Rch<T>>>, value: T) -> Rch<T> {
    let hashed = HashCached::new(value);
    let mut table = table.borrow_mut();
    if let Some(existing) = table.get(&hashed) {
        return existing.clone();
    }
    let rch = Rc::new(hashed);
    table.insert(rch.clone());
    rch
}

/// Result of `pair_product`: the product node plus, for each of its exits, the
/// `(lhs_exit, rhs_exit)` pair it came from.
pub type PairProductResult<T> = (Rch<T>, Vec<(usize, usize)>);
/// Result of `pair_map`: the mapped node plus its outer return map.
pub type PairMapResult<T> = (Rch<T>, Vec<usize>);

thread_local! {
    /// Global interning table for operation matrices (`Vec<usize>`), so that
    /// `pair_map` can key its cache on the matrix's pointer identity. Concrete
    /// (non-generic) element type, so a single thread-local suffices.
    static OP_MATRIX_TABLE: RefCell<HashSet<Rch<Vec<usize>>>> = RefCell::new(HashSet::default());
}

/// Hash-cons an operation matrix; see [`OP_MATRIX_TABLE`].
pub fn intern_op_matrix(matrix: Vec<usize>) -> Rch<Vec<usize>> {
    OP_MATRIX_TABLE.with(|table| intern_in(table, matrix))
}

/// Per-type operation caches, keyed by the `Rc::as_ptr` identity of the
/// (interned) operands — the analogue of the old central `Context` caches. The
/// storage accessors are provided per concrete/wrapper type (by
/// [`__gcflobdd_op_cache_storage`] / `declare_grammar!`); the get/set logic is
/// shared here.
pub trait OpCached: Hash + Eq + Sized + 'static {
    fn pair_product_cache()
    -> &'static LocalKey<RefCell<HashMap<(usize, usize), PairProductResult<Self>>>>;
    fn pair_map_cache()
    -> &'static LocalKey<RefCell<HashMap<(usize, usize, usize), PairMapResult<Self>>>>;
    fn reduction_cache() -> &'static LocalKey<RefCell<HashMap<(usize, Vec<usize>), Rch<Self>>>>;

    fn get_pair_product_cache(lhs: &Rch<Self>, rhs: &Rch<Self>) -> Option<PairProductResult<Self>> {
        let key = (Rc::as_ptr(lhs) as usize, Rc::as_ptr(rhs) as usize);
        Self::pair_product_cache().with(|c| c.borrow().get(&key).cloned())
    }
    fn set_pair_product_cache(lhs: &Rch<Self>, rhs: &Rch<Self>, result: PairProductResult<Self>) {
        let key = (Rc::as_ptr(lhs) as usize, Rc::as_ptr(rhs) as usize);
        Self::pair_product_cache().with(|c| {
            c.borrow_mut().insert(key, result);
        });
    }
    fn get_pair_map_cache(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        op_matrix: &Rch<Vec<usize>>,
    ) -> Option<PairMapResult<Self>> {
        let key = (
            Rc::as_ptr(lhs) as usize,
            Rc::as_ptr(rhs) as usize,
            Rc::as_ptr(op_matrix) as usize,
        );
        Self::pair_map_cache().with(|c| c.borrow().get(&key).cloned())
    }
    fn set_pair_map_cache(
        lhs: &Rch<Self>,
        rhs: &Rch<Self>,
        op_matrix: &Rch<Vec<usize>>,
        result: PairMapResult<Self>,
    ) {
        let key = (
            Rc::as_ptr(lhs) as usize,
            Rc::as_ptr(rhs) as usize,
            Rc::as_ptr(op_matrix) as usize,
        );
        Self::pair_map_cache().with(|c| {
            c.borrow_mut().insert(key, result);
        });
    }
    fn get_reduction_cache(node: &Rch<Self>, reduce_map: &[usize]) -> Option<Rch<Self>> {
        let key = (Rc::as_ptr(node) as usize, reduce_map.to_vec());
        Self::reduction_cache().with(|c| c.borrow().get(&key).cloned())
    }
    fn set_reduction_cache(node: &Rch<Self>, reduce_map: &[usize], result: Rch<Self>) {
        let key = (Rc::as_ptr(node) as usize, reduce_map.to_vec());
        Self::reduction_cache().with(|c| {
            c.borrow_mut().insert(key, result);
        });
    }
}

/// Define the three operation-cache thread-locals for a concrete type and
/// implement [`OpCached`] for it. Exported so `declare_grammar!`'s wrappers can
/// reuse the exact same storage shape as the in-crate concrete types.
#[macro_export]
macro_rules! __gcflobdd_op_cache_storage {
    ($ty:ty) => {
        impl $ty {
            thread_local! {
                static PAIR_PRODUCT_CACHE: ::std::cell::RefCell<$crate::utils::HashMap<
                    (usize, usize),
                    $crate::gcflobdd::PairProductResult<$ty>,
                >> = ::std::cell::RefCell::new($crate::utils::HashMap::default());
                static PAIR_MAP_CACHE: ::std::cell::RefCell<$crate::utils::HashMap<
                    (usize, usize, usize),
                    $crate::gcflobdd::PairMapResult<$ty>,
                >> = ::std::cell::RefCell::new($crate::utils::HashMap::default());
                static REDUCTION_CACHE: ::std::cell::RefCell<$crate::utils::HashMap<
                    (usize, ::std::vec::Vec<usize>),
                    $crate::utils::hash_cache::Rch<$ty>,
                >> = ::std::cell::RefCell::new($crate::utils::HashMap::default());
            }
        }
        impl $crate::gcflobdd::OpCached for $ty {
            fn pair_product_cache() -> &'static ::std::thread::LocalKey<
                ::std::cell::RefCell<
                    $crate::utils::HashMap<
                        (usize, usize),
                        $crate::gcflobdd::PairProductResult<Self>,
                    >,
                >,
            > {
                &Self::PAIR_PRODUCT_CACHE
            }
            fn pair_map_cache() -> &'static ::std::thread::LocalKey<
                ::std::cell::RefCell<
                    $crate::utils::HashMap<
                        (usize, usize, usize),
                        $crate::gcflobdd::PairMapResult<Self>,
                    >,
                >,
            > {
                &Self::PAIR_MAP_CACHE
            }
            fn reduction_cache() -> &'static ::std::thread::LocalKey<
                ::std::cell::RefCell<
                    $crate::utils::HashMap<
                        (usize, ::std::vec::Vec<usize>),
                        $crate::utils::hash_cache::Rch<Self>,
                    >,
                >,
            > {
                &Self::REDUCTION_CACHE
            }
        }
    };
}
