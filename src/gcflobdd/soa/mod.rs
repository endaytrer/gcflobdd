use static_assertions::const_assert;
use std::alloc::{Layout, alloc, realloc};
use std::collections::VecDeque;
use std::mem::MaybeUninit;
use std::ptr::null_mut;
use std::rc::Rc;

use crate::gcflobdd::bdd::Bdd;
use crate::gcflobdd::connection::Connection;
use crate::gcflobdd::node::{GcflobddNode, GcflobddNodeType, InternalNode};
use crate::grammar::GrammarNode;
use crate::utils::hash_cache::HashCached;

type FatPointer = (u64, u64, u64);

pub trait Soa: Default {
    /// Type to set its value.
    type ValueType;
    /// Type to get its value.
    type RefType<'a>
    where
        Self: 'a;
    /// Type to be gotten from the soa.
    type ViewType<'a>
    where
        Self: 'a;

    fn with_capacity(capacity: usize) -> Self;
    unsafe fn calloc(&mut self, capacity: usize);
    unsafe fn realloc(&mut self, old_capacity: usize, new_capacity: usize);
    unsafe fn get<'a>(&'a self, index: usize) -> Self::ViewType<'a>;
    unsafe fn set(&mut self, index: usize, node: Self::ValueType);
    /// Drops the value stored at `index` in-place. After this, the slot is logically
    /// uninitialised; a subsequent `set` at the same index does not need to (and
    /// must not) drop the previous contents.
    unsafe fn drop_at(&mut self, index: usize);
}

macro_rules! calloc {
    ($ty:ty, $cap:expr) => {
        unsafe { alloc(Layout::array::<$ty>($cap).unwrap_unchecked()) as *mut $ty }
    };
}

macro_rules! realloc {
    ($ty:ty, $ptr:expr, $old_cap:expr, $new_cap:expr) => {
        unsafe {
            realloc(
                $ptr as *mut u8,
                Layout::array::<$ty>($old_cap).unwrap_unchecked(),
                $new_cap * std::mem::size_of::<$ty>(),
            ) as *mut $ty
        }
    };
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum SoaGcflobddNodeType {
    DontCare,
    Fork,
    Internal,
    Bdd,
}
#[derive(Debug, Default)]
pub struct SoaGcflobddNode<'grammar> {
    /// components of nodes
    num_exits: *mut usize,
    grammar: *mut &'grammar Rc<GrammarNode>,
    node_type: *mut SoaGcflobddNodeType,
    /// A fat pointer to:
    /// - DontCare and Fork: does not apply (bytes are uninitialised — never read)
    /// - Internal: `Vec<Vec<Connection>>` (24 bytes, fills the slot)
    /// - Bdd: `Bdd` (8 bytes; remaining 16 bytes uninitialised)
    ///
    /// Stored as `MaybeUninit<FatPointer>` so that reading the slot as a raw
    /// `FatPointer` — whose bytes are only partially (or not at all) initialised
    /// for some variants — is not UB. Reads are funnelled through
    /// variant-specific pointer casts and only dereferenced when the
    /// corresponding `node_type` confirms the payload was written.
    union_pointer_0: *mut MaybeUninit<FatPointer>,
}

#[derive(Debug, Default)]
struct SoaRc<T> {
    inner: T,
    reference_count: *mut usize,
}
#[derive(Debug)]
struct RcView<T> {
    inner: T,
    reference_count: usize,
}

impl<T: Soa> Soa for SoaRc<T> {
    type ValueType = T::ValueType;
    type RefType<'a>
        = T::RefType<'a>
    where
        Self: 'a;
    type ViewType<'a>
        = RcView<T::ViewType<'a>>
    where
        Self: 'a;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: T::with_capacity(capacity),
            reference_count: calloc!(usize, capacity),
        }
    }

    unsafe fn calloc(&mut self, capacity: usize) {
        unsafe { self.inner.calloc(capacity) };
        self.reference_count = calloc!(usize, capacity);
    }
    unsafe fn realloc(&mut self, old_capacity: usize, new_capacity: usize) {
        unsafe { self.inner.realloc(old_capacity, new_capacity) };
        self.reference_count = realloc!(usize, self.reference_count, old_capacity, new_capacity);
    }

    unsafe fn get<'a>(&'a self, index: usize) -> Self::ViewType<'a> {
        RcView {
            inner: unsafe { self.inner.get(index) },
            reference_count: unsafe { self.reference_count.add(index).read() },
        }
    }

    unsafe fn set(&mut self, index: usize, node: Self::ValueType) {
        unsafe { self.inner.set(index, node) };
        unsafe { self.reference_count.add(index).write(1) };
    }

    unsafe fn drop_at(&mut self, index: usize) {
        unsafe { self.inner.drop_at(index) };
        // no need to drop for reference_count
    }
}

#[derive(Debug, Default)]
struct SoaHashSetValue<T> {
    inner: T,
    hash: *mut u64,
    next: *mut isize,
}

#[derive(Debug)]
pub struct HashSetValueView<T> {
    pub hash: u64,
    pub inner: T,
    pub next: isize,
}

impl<T: PartialEq> PartialEq for HashSetValueView<T> {
    // next is not compaired
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.inner == other.inner
    }
}
impl<T: Eq> Eq for HashSetValueView<T> {}

impl<SOA: Soa> Soa for SoaHashSetValue<SOA>
where
    SOA::ValueType: std::hash::Hash,
{
    type ValueType = HashCached<SOA::ValueType>;
    type RefType<'a>
        = HashCached<SOA::RefType<'a>>
    where
        Self: 'a;
    type ViewType<'a>
        = HashSetValueView<SOA::ViewType<'a>>
    where
        Self: 'a;
    fn with_capacity(capacity: usize) -> Self {
        debug_assert!(capacity > 0);
        Self {
            inner: SOA::with_capacity(capacity),
            next: unsafe { alloc(Layout::array::<isize>(capacity).unwrap()) as *mut isize },
            hash: unsafe { alloc(Layout::array::<u64>(capacity).unwrap()) as *mut u64 },
        }
    }
    unsafe fn calloc(&mut self, capacity: usize) {
        debug_assert!(capacity > 0);
        debug_assert_eq!(self.next, null_mut());
        unsafe { self.inner.calloc(capacity) };
        self.next = unsafe { alloc(Layout::array::<isize>(capacity).unwrap()) as *mut isize };
        self.hash = unsafe { alloc(Layout::array::<u64>(capacity).unwrap()) as *mut u64 };
    }
    unsafe fn realloc(&mut self, old_capacity: usize, new_capacity: usize) {
        debug_assert_ne!(self.next, null_mut());
        unsafe { self.inner.realloc(old_capacity, new_capacity) };
        self.next = unsafe {
            realloc(
                self.next as *mut u8,
                Layout::array::<isize>(old_capacity).unwrap_unchecked(),
                new_capacity * std::mem::size_of::<isize>(),
            ) as *mut isize
        };
        self.hash = unsafe {
            realloc(
                self.hash as *mut u8,
                Layout::array::<u64>(old_capacity).unwrap_unchecked(),
                new_capacity * std::mem::size_of::<u64>(),
            ) as *mut u64
        };
    }
    unsafe fn set(&mut self, index: usize, node: Self::ValueType) {
        let hash_code = node.hash_code();
        unsafe { self.inner.set(index, node.value) };
        // don't write next
        unsafe { self.hash.add(index).write(hash_code) };
    }
    unsafe fn get<'a>(&'a self, index: usize) -> Self::ViewType<'a> {
        Self::ViewType {
            hash: unsafe { self.hash.add(index).read() },
            inner: unsafe { self.inner.get(index) },
            next: unsafe { self.next.add(index).read() },
        }
    }
    unsafe fn drop_at(&mut self, index: usize) {
        // hash and next are Copy; only the inner row owns heap data.
        unsafe { self.inner.drop_at(index) };
    }
}

const_assert!(std::mem::size_of::<FatPointer>() == std::mem::size_of::<Vec<Vec<Connection>>>());
const_assert!(std::mem::size_of::<FatPointer>() == std::mem::size_of::<Bdd>() + 16);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefGcflobddNodeType<'a, 'grammar> {
    DontCare,
    Fork,
    Internal(&'a InternalNode<'grammar>),
    Bdd(&'a Bdd),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefGcflobddNode<'a, 'grammar> {
    pub(crate) num_exits: usize,
    pub(crate) grammar: &'grammar Rc<GrammarNode>,
    pub(crate) node: RefGcflobddNodeType<'a, 'grammar>,
}

impl<'a, 'grammar> From<&'a GcflobddNode<'grammar>> for RefGcflobddNode<'a, 'grammar> {
    fn from(node: &'a GcflobddNode<'grammar>) -> Self {
        let node_type = match &node.node {
            GcflobddNodeType::DontCare => RefGcflobddNodeType::DontCare,
            GcflobddNodeType::Fork => RefGcflobddNodeType::Fork,
            GcflobddNodeType::Internal(internal_node) => {
                RefGcflobddNodeType::Internal(internal_node)
            }
            GcflobddNodeType::Bdd(bdd) => RefGcflobddNodeType::Bdd(bdd),
        };
        Self {
            num_exits: 0,
            grammar: node.grammar,
            node: node_type,
        }
    }
}

impl<'a, 'grammar> PartialEq<GcflobddNode<'grammar>> for RefGcflobddNode<'a, 'grammar> {
    fn eq(&self, other: &GcflobddNode<'grammar>) -> bool {
        self.num_exits == other.num_exits
            && self.grammar == other.grammar
            && match (self.node, &other.node) {
                (RefGcflobddNodeType::DontCare, GcflobddNodeType::DontCare) => true,
                (RefGcflobddNodeType::Fork, GcflobddNodeType::Fork) => true,
                (
                    RefGcflobddNodeType::Internal(lhs_internal),
                    GcflobddNodeType::Internal(rhs_internal),
                ) => lhs_internal == rhs_internal,
                (RefGcflobddNodeType::Bdd(lhs_bdd), GcflobddNodeType::Bdd(rhs_bdd)) => {
                    lhs_bdd == rhs_bdd
                }
                _ => false,
            }
    }
}

impl<'grammar> Soa for SoaGcflobddNode<'grammar> {
    type ValueType = GcflobddNode<'grammar>;
    type RefType<'a>
        = RefGcflobddNode<'a, 'grammar>
    where
        Self: 'a;
    type ViewType<'a>
        = RefGcflobddNode<'a, 'grammar>
    where
        Self: 'a;
    fn with_capacity(capacity: usize) -> Self {
        debug_assert!(capacity > 0);
        Self {
            num_exits: calloc!(usize, capacity),
            grammar: calloc!(&'grammar Rc<GrammarNode>, capacity),
            node_type: calloc!(SoaGcflobddNodeType, capacity),
            union_pointer_0: calloc!(MaybeUninit<FatPointer>, capacity),
        }
    }

    unsafe fn calloc(&mut self, capacity: usize) {
        debug_assert!(capacity > 0);
        debug_assert_eq!(self.num_exits, null_mut());
        debug_assert_eq!(self.grammar, null_mut());
        debug_assert_eq!(self.node_type, null_mut());
        debug_assert_eq!(self.union_pointer_0, null_mut());
        self.num_exits = calloc!(usize, capacity);
        self.grammar = calloc!(&'grammar Rc<GrammarNode>, capacity);
        self.node_type = calloc!(SoaGcflobddNodeType, capacity);
        self.union_pointer_0 = calloc!(MaybeUninit<FatPointer>, capacity);
    }

    unsafe fn realloc(&mut self, old_capacity: usize, new_capacity: usize) {
        self.num_exits = realloc!(usize, self.num_exits, old_capacity, new_capacity);
        self.grammar = realloc!(
            &'grammar Rc<GrammarNode>,
            self.grammar,
            old_capacity,
            new_capacity
        );
        self.node_type = realloc!(
            SoaGcflobddNodeType,
            self.node_type,
            old_capacity,
            new_capacity
        );
        self.union_pointer_0 = realloc!(
            MaybeUninit<FatPointer>,
            self.union_pointer_0,
            old_capacity,
            new_capacity
        );
    }
    unsafe fn set(&mut self, index: usize, node: Self::ValueType) {
        unsafe { self.num_exits.add(index).write(node.num_exits) };
        unsafe { self.grammar.add(index).write(node.grammar) };
        match node.node {
            GcflobddNodeType::DontCare => {
                unsafe {
                    self.node_type
                        .add(index)
                        .write(SoaGcflobddNodeType::DontCare)
                };
                // don't need to write for DontCare
                // unsafe { self.union_pointer_0.add(index).write((0, 0, 0)) };
            }
            super::node::GcflobddNodeType::Fork => {
                unsafe { self.node_type.add(index).write(SoaGcflobddNodeType::Fork) };
                // don't need to write for Fork
                // unsafe { self.union_pointer_0.add(index).write((0, 0, 0)) };
            }
            super::node::GcflobddNodeType::Internal(internal_node) => {
                unsafe {
                    self.node_type
                        .add(index)
                        .write(SoaGcflobddNodeType::Internal)
                };
                unsafe {
                    (self.union_pointer_0.add(index) as *mut Vec<Vec<Connection>>)
                        .write(internal_node.connections)
                };
            }
            super::node::GcflobddNodeType::Bdd(bdd) => {
                unsafe { self.node_type.add(index).write(SoaGcflobddNodeType::Bdd) };
                unsafe { (self.union_pointer_0.add(index) as *mut Bdd).write(bdd) };
            }
        }
    }
    unsafe fn get<'a>(&'a self, index: usize) -> Self::ViewType<'a> {
        Self::ViewType {
            num_exits: unsafe { self.num_exits.add(index).read() },
            grammar: unsafe { self.grammar.add(index).read() },
            node: match unsafe { self.node_type.add(index).read() } {
                SoaGcflobddNodeType::DontCare => RefGcflobddNodeType::DontCare,
                SoaGcflobddNodeType::Fork => RefGcflobddNodeType::Fork,
                SoaGcflobddNodeType::Internal => RefGcflobddNodeType::Internal(unsafe {
                    (self.union_pointer_0.add(index) as *const InternalNode)
                        .as_ref()
                        .unwrap_unchecked()
                }),
                SoaGcflobddNodeType::Bdd => RefGcflobddNodeType::Bdd(unsafe {
                    (self.union_pointer_0.add(index) as *const Bdd)
                        .as_ref()
                        .unwrap_unchecked()
                }),
            },
        }
    }
    unsafe fn drop_at(&mut self, index: usize) {
        // reference_count, num_exits: usize — no drop.
        // grammar: &'grammar Rc<_> — borrow, no drop.
        // node_type: enum with no heap data — no drop.
        // Only the union payload may own heap data, and only for Internal / Bdd.
        let node_type = unsafe { self.node_type.add(index).read() };
        match node_type {
            SoaGcflobddNodeType::DontCare | SoaGcflobddNodeType::Fork => {}
            SoaGcflobddNodeType::Internal => unsafe {
                std::ptr::drop_in_place(self.union_pointer_0.add(index) as *mut InternalNode);
            },
            SoaGcflobddNodeType::Bdd => unsafe {
                std::ptr::drop_in_place(self.union_pointer_0.add(index) as *mut Bdd);
            },
        }
    }
}

#[derive(Debug, Default)]
struct SoaVec<SOA: Soa> {
    len: usize,
    capacity: usize,
    soa: SOA,
}

impl<SOA: Soa> SoaVec<SOA> {
    const INITIAL_CAPACITY: usize = 4;
    const GROWTH_FACTOR: usize = 2;
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_capacity(capacity: usize) -> Self {
        debug_assert!(capacity > 0);
        Self {
            len: 0,
            capacity,
            soa: SOA::with_capacity(capacity),
        }
    }
    fn grow(&mut self) {
        let new_cap = if self.capacity == 0 {
            Self::INITIAL_CAPACITY
        } else {
            self.capacity * Self::GROWTH_FACTOR
        };
        if self.capacity == new_cap {
            return;
        }
        if self.capacity == 0 {
            unsafe {
                self.soa.calloc(new_cap);
            }
        } else {
            unsafe {
                self.soa.realloc(self.capacity, new_cap);
            }
        }
        self.capacity = new_cap;
    }
    fn shrink_to_fit(&mut self) {
        if self.len == self.capacity {
            return;
        }
        unsafe {
            self.soa.realloc(self.capacity, self.len);
        }
        self.capacity = self.len;
    }
    fn push(&mut self, node: SOA::ValueType) {
        if self.len == self.capacity {
            self.grow();
        }
        unsafe {
            self.soa.set(self.len, node);
        };
        self.len += 1;
    }
    unsafe fn set(&mut self, index: usize, node: SOA::ValueType) {
        unsafe {
            self.soa.set(index, node);
        };
    }
    /// Returns a view of the slot at `index`.
    ///
    /// The returned view's lifetime `'any` is decoupled from the borrow of
    /// `&self`: the caller picks it. This lets a caller hold a view tied to a
    /// longer outer borrow (e.g. `&'a mut self`) while the transient immutable
    /// reborrow of `self` used to produce the view ends immediately, so a
    /// subsequent `&mut` operation on the same structure is permitted.
    ///
    /// # Safety
    /// The slot at `index` must be initialised, and the underlying storage
    /// must remain valid (not freed, reallocated, or overwritten) for the
    /// entire duration of `'any`.
    unsafe fn get<'any>(&self, index: usize) -> SOA::ViewType<'any> {
        let short: SOA::ViewType<'_> = unsafe { self.soa.get(index) };
        // SAFETY: `SOA::ViewType<'_>` and `SOA::ViewType<'any>` have identical
        // layout — Rust erases lifetimes before codegen. The caller guarantees
        // the underlying data outlives `'any`.
        unsafe {
            let md = std::mem::ManuallyDrop::new(short);
            std::ptr::read(&*md as *const SOA::ViewType<'_> as *const SOA::ViewType<'any>)
        }
    }
}

#[derive(Debug, Default)]
struct SoaAllocator<SOA: Soa> {
    soa: SoaVec<SOA>,
    free_list: VecDeque<usize>,
}

impl<SOA: Soa> SoaAllocator<SOA> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn malloc(&mut self, value: SOA::ValueType) -> usize {
        if let Some(index) = self.free_list.pop_front() {
            // Slot was drop_at'd when freed, so writing here is correct: no
            // previous occupant to drop.
            unsafe {
                self.soa.set(index, value);
            };
            index
        } else {
            self.soa.push(value);
            self.soa.len - 1
        }
    }
    pub fn free(&mut self, index: usize) {
        // Drop the stored value so its heap data is released, *then* recycle
        // the slot index. `malloc` counts on the slot being logically empty.
        unsafe {
            self.soa.soa.drop_at(index);
        }
        self.free_list.push_back(index);
    }
}

#[derive(Debug, Default)]
pub struct SoaAllocatorHashTable {
    buckets: Vec<isize>,
    mask: u64,
    len: usize,
    /// resize when len >= threshold
    threshold: usize,
    buckets_filled: usize,
}
impl SoaAllocatorHashTable {
    pub fn new() -> Self {
        Self {
            buckets: vec![-1, -1, -1, -1],
            mask: 3,
            len: 0,
            threshold: 3,
            buckets_filled: 0,
        }
    }
}

#[derive(Debug)]
pub struct SoaNodeTable<T: Soa>
where
    T::ValueType: std::hash::Hash,
{
    hash_table: SoaAllocatorHashTable,
    allocation: SoaAllocator<SoaHashSetValue<T>>,
}

impl<T: Soa> Default for SoaNodeTable<T>
where
    T::ValueType: std::hash::Hash,
{
    fn default() -> Self {
        Self {
            hash_table: SoaAllocatorHashTable::new(),
            allocation: SoaAllocator::new(),
        }
    }
}

impl<T: Soa> SoaNodeTable<T>
where
    T::ValueType: std::hash::Hash,
{
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.hash_table.len
    }

    /// Rehash in place. Module-private because correctness depends on the
    /// caller only ever doubling or halving the bucket count (the two
    /// callers, `grow` and `shrink`, do exactly this). Under that invariant,
    /// rehashed entries never land in a bucket index that is both `> i` and
    /// `< old_capacity`, so we can rewrite `buckets[i]` in place without
    /// corrupting unprocessed chains.
    unsafe fn resize(&mut self, new_capacity: usize) {
        debug_assert!(new_capacity.is_power_of_two());
        let old_capacity = self.hash_table.buckets.len();
        debug_assert!(
            new_capacity == old_capacity * 2 || new_capacity * 2 == old_capacity,
            "resize must double or halve the bucket count"
        );
        let new_mask = (new_capacity - 1) as u64;
        let mut new_buckets_filled = 0;

        if new_capacity > old_capacity {
            self.hash_table.buckets.resize(new_capacity, -1);
        }

        for i in 0..old_capacity {
            let mut entry = self.hash_table.buckets[i];
            self.hash_table.buckets[i] = -1;

            while entry != -1 {
                let candidate = unsafe { self.allocation.soa.get(entry as usize) };
                let next_entry = candidate.next;

                let bucket_idx = (candidate.hash & new_mask) as usize;
                unsafe {
                    self.allocation
                        .soa
                        .soa
                        .next
                        .add(entry as usize)
                        .write(self.hash_table.buckets[bucket_idx]);
                }
                if self.hash_table.buckets[bucket_idx] == -1 {
                    new_buckets_filled += 1;
                }
                self.hash_table.buckets[bucket_idx] = entry;

                entry = next_entry;
            }
        }

        if new_capacity < old_capacity {
            self.hash_table.buckets.truncate(new_capacity);
        }

        self.hash_table.mask = new_mask;
        // threshold should be capacity * 0.75
        self.hash_table.threshold = (new_capacity >> 1) | (new_capacity >> 2);
        self.hash_table.buckets_filled = new_buckets_filled;
    }

    fn grow(&mut self) {
        unsafe { self.resize(self.hash_table.buckets.len() * 2) };
    }

    fn shrink(&mut self) {
        let current_cap = self.hash_table.buckets.len();
        if current_cap > 4 {
            unsafe { self.resize(current_cap / 2) };
        }
    }
}
impl<T: Soa> SoaNodeTable<T>
where
    T::ValueType: std::hash::Hash,
{
    #[inline]
    pub unsafe fn get_view<'a>(&'a self, index: usize) -> HashSetValueView<T::ViewType<'a>> {
        unsafe { self.allocation.soa.get(index) }
    }
}
impl<T: Soa> SoaNodeTable<T>
where
    T::ValueType: std::hash::Hash,
{
    pub fn get_index<'a>(&'a self, hash: u64, value: T::RefType<'a>) -> Option<usize>
    where
        T::ViewType<'a>: PartialEq<T::RefType<'a>>,
    {
        let mut entry = self.hash_table.buckets[(hash & self.hash_table.mask) as usize];
        while entry != -1 {
            let candidate = unsafe { self.allocation.soa.get(entry as usize) };
            if candidate.hash == hash && candidate.inner == value {
                return Some(entry as usize);
            }
            entry = candidate.next;
        }
        None
    }
}
impl<T: Soa> SoaNodeTable<T>
where
    T::ValueType: std::hash::Hash,
{
    pub fn insert<'a>(&'a mut self, value: HashCached<T::ValueType>) -> usize
    where
        T::ViewType<'a>: PartialEq<T::ValueType>,
    {
        // Grow when 75% of buckets are filled
        let hash = value.hash_code();
        if self.hash_table.buckets_filled >= self.hash_table.threshold {
            self.grow();
        }

        let bucket_idx = (hash & self.hash_table.mask) as usize;
        let mut entry = self.hash_table.buckets[bucket_idx];

        if entry == -1 {
            let slot = self.allocation.malloc(value);
            self.hash_table.buckets[bucket_idx] = slot as isize;
            unsafe { self.allocation.soa.soa.next.add(slot).write(-1) };
            self.hash_table.buckets_filled += 1;
            self.hash_table.len += 1;
            return slot as usize;
        }

        let mut last_entry = entry;
        while entry != -1 {
            let candidate = unsafe { self.allocation.soa.get(entry as usize) };
            if candidate.hash == hash && candidate.inner == *value {
                return entry as usize;
            }
            last_entry = entry;
            entry = candidate.next;
        }

        let slot = self.allocation.malloc(value);

        unsafe {
            *(self.allocation.soa.soa.next.add(last_entry as usize) as *mut isize) = slot as isize;
            self.allocation.soa.soa.next.add(slot).write(-1);
        }

        self.hash_table.len += 1;
        slot as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    impl Soa for *mut usize {
        type ValueType = usize;
        type RefType<'a> = usize;
        type ViewType<'a> = usize;

        fn with_capacity(capacity: usize) -> Self {
            debug_assert!(capacity > 0);
            calloc!(usize, capacity)
        }

        unsafe fn calloc(&mut self, capacity: usize) {
            debug_assert!(capacity > 0);
            *self = calloc!(usize, capacity);
        }

        unsafe fn realloc(&mut self, old_capacity: usize, new_capacity: usize) {
            *self = realloc!(usize, *self, old_capacity, new_capacity);
        }

        unsafe fn set(&mut self, index: usize, node: Self::ValueType) {
            unsafe { self.add(index).write(node) };
        }
        unsafe fn get<'a>(&'a self, index: usize) -> Self::RefType<'a> {
            unsafe { self.add(index).read() }
        }
        unsafe fn drop_at(&mut self, _index: usize) {
            // usize is Copy — nothing to drop.
        }
    }
    impl Soa for (*mut usize, *mut usize) {
        type ValueType = (usize, usize);
        type RefType<'a> = (usize, usize);
        type ViewType<'a> = (usize, usize);

        fn with_capacity(capacity: usize) -> Self {
            debug_assert!(capacity > 0);
            unsafe {
                (
                    alloc(Layout::array::<usize>(capacity).unwrap()) as *mut usize,
                    alloc(Layout::array::<usize>(capacity).unwrap()) as *mut usize,
                )
            }
        }

        unsafe fn calloc(&mut self, capacity: usize) {
            debug_assert!(capacity > 0);
            *self = unsafe {
                (
                    alloc(Layout::array::<usize>(capacity).unwrap()) as *mut usize,
                    alloc(Layout::array::<usize>(capacity).unwrap()) as *mut usize,
                )
            };
        }

        unsafe fn realloc(&mut self, old_capacity: usize, new_capacity: usize) {
            *self = unsafe {
                (
                    realloc(
                        self.0 as *mut u8,
                        Layout::array::<usize>(old_capacity).unwrap_unchecked(),
                        new_capacity * std::mem::size_of::<usize>(),
                    ) as *mut usize,
                    realloc(
                        self.1 as *mut u8,
                        Layout::array::<usize>(old_capacity).unwrap_unchecked(),
                        new_capacity * std::mem::size_of::<usize>(),
                    ) as *mut usize,
                )
            };
        }

        unsafe fn set(&mut self, index: usize, node: Self::ValueType) {
            unsafe { self.0.add(index).write(node.0) };

            unsafe { self.1.add(index).write(node.1) };
        }
        unsafe fn get<'a>(&'a self, index: usize) -> Self::ViewType<'a> {
            (unsafe { self.0.add(index).read() }, unsafe {
                self.1.add(index).read()
            })
        }
        unsafe fn drop_at(&mut self, _index: usize) {
            // (usize, usize) is Copy — nothing to drop.
        }
    }

    #[test]
    fn test_vec() {
        let mut vec = SoaVec::<*mut usize>::with_capacity(4);
        vec.push(0);
        vec.push(1);
        vec.push(2);
        vec.push(3);
        assert_eq!(vec.len, 4);
        assert_eq!(vec.capacity, 4);
        vec.push(4);
        assert_eq!(vec.len, 5);
        assert_eq!(vec.capacity, 8);
        assert_eq!(unsafe { vec.get(0) }, 0);
        assert_eq!(unsafe { vec.get(1) }, 1);
        assert_eq!(unsafe { vec.get(2) }, 2);
        assert_eq!(unsafe { vec.get(3) }, 3);
        assert_eq!(unsafe { vec.get(4) }, 4);
    }

    #[test]
    fn test_compound_vec() {
        let mut vec = SoaVec::<(*mut usize, *mut usize)>::with_capacity(4);
        vec.push((0, 1));
        vec.push((1, 2));
        vec.push((2, 3));
        vec.push((3, 4));
        assert_eq!(vec.len, 4);
        assert_eq!(vec.capacity, 4);
        vec.push((4, 5));
        assert_eq!(vec.len, 5);
        assert_eq!(vec.capacity, 8);
        assert_eq!(unsafe { vec.get(0) }, (0, 1));
        assert_eq!(unsafe { vec.get(1) }, (1, 2));
        assert_eq!(unsafe { vec.get(2) }, (2, 3));
        assert_eq!(unsafe { vec.get(3) }, (3, 4));
        assert_eq!(unsafe { vec.get(4) }, (4, 5));
        vec.shrink_to_fit();
        assert_eq!(vec.len, 5);
        assert_eq!(vec.capacity, 5);
        assert_eq!(unsafe { vec.get(0) }, (0, 1));
        assert_eq!(unsafe { vec.get(1) }, (1, 2));
        assert_eq!(unsafe { vec.get(2) }, (2, 3));
        assert_eq!(unsafe { vec.get(3) }, (3, 4));
        assert_eq!(unsafe { vec.get(4) }, (4, 5));
    }

    #[test]
    fn test_hash_table() {
        let mut hash_table = SoaNodeTable::<(*mut usize, *mut usize)>::new();

        // Test basic set and get
        let val1 = HashCached::new((0, 1));
        let hash1 = val1.hash_code();
        let idx1 = hash_table.insert(val1);
        assert_eq!(idx1, 0);
        assert_eq!(hash_table.get_index(hash1, (0, 1)), Some(0));
        assert_eq!(unsafe { hash_table.allocation.soa.get(0) }.inner, (0, 1));

        // Test getting non-existent value
        assert_eq!(hash_table.get_index(12345, (9, 9)), None);

        // Test setting identical value (should return existing index)
        let val1_dup = HashCached::new((0, 1));
        let idx1_dup = hash_table.insert(val1_dup);
        assert_eq!(idx1_dup, 0);
        assert_eq!(hash_table.hash_table.len, 1);

        // Test triggering a resize (capacity grows)
        for i in 1..10 {
            let val = HashCached::new((i, i + 1));
            let hash = val.hash_code();
            let idx = hash_table.insert(val);
            assert_eq!(idx, i as usize);
            assert_eq!(hash_table.get_index(hash, (i, i + 1)), Some(i as usize));
        }

        assert_eq!(hash_table.hash_table.len, 10);
        assert!(hash_table.hash_table.buckets.len() > 4); // Initial capacity is 4

        // Verify old items still exist after resize
        for i in 0..10 {
            let val = HashCached::new((i, i + 1));
            let hash = val.hash_code();
            assert_eq!(hash_table.get_index(hash, (i, i + 1)), Some(i as usize));
        }

        // Add more items to test free list and reuse (if applicable through allocation)
        // Note: SoaNodeTable doesn't have a remove/free method exposed yet, but we test
        // the collision resolution path when elements have the same bucket but different hash/values.
    }

    #[test]
    fn test_vec_default_grows_from_zero() {
        let mut vec = SoaVec::<*mut usize>::new();
        assert_eq!(vec.len, 0);
        assert_eq!(vec.capacity, 0);
        vec.push(42);
        assert_eq!(vec.len, 1);
        assert_eq!(vec.capacity, SoaVec::<*mut usize>::INITIAL_CAPACITY);
        assert_eq!(unsafe { vec.get(0) }, 42);
    }

    #[test]
    fn test_vec_many_grows() {
        let mut vec = SoaVec::<*mut usize>::new();
        for i in 0..100usize {
            vec.push(i * 7 + 3);
        }
        assert_eq!(vec.len, 100);
        assert!(vec.capacity >= 100);
        for i in 0..100usize {
            assert_eq!(unsafe { vec.get(i) }, i * 7 + 3);
        }
    }

    #[test]
    fn test_vec_set_overwrites() {
        let mut vec = SoaVec::<*mut usize>::with_capacity(4);
        vec.push(10);
        vec.push(20);
        vec.push(30);
        unsafe { vec.set(1, 99) };
        assert_eq!(unsafe { vec.get(0) }, 10);
        assert_eq!(unsafe { vec.get(1) }, 99);
        assert_eq!(unsafe { vec.get(2) }, 30);
        assert_eq!(vec.len, 3);
    }

    #[test]
    fn test_vec_shrink_noop_when_full() {
        let mut vec = SoaVec::<*mut usize>::with_capacity(4);
        vec.push(1);
        vec.push(2);
        vec.push(3);
        vec.push(4);
        assert_eq!(vec.len, 4);
        assert_eq!(vec.capacity, 4);
        vec.shrink_to_fit(); // early return; len == capacity
        assert_eq!(vec.capacity, 4);
        for i in 0..4 {
            assert_eq!(unsafe { vec.get(i) }, (i + 1) as usize);
        }
    }

    #[test]
    fn test_allocation_malloc_unique_indices() {
        let mut alloc: SoaAllocator<*mut usize> = SoaAllocator::new();
        let i0 = alloc.malloc(100);
        let i1 = alloc.malloc(200);
        let i2 = alloc.malloc(300);
        assert_eq!(i0, 0);
        assert_eq!(i1, 1);
        assert_eq!(i2, 2);
        assert_eq!(unsafe { alloc.soa.get(i0) }, 100);
        assert_eq!(unsafe { alloc.soa.get(i1) }, 200);
        assert_eq!(unsafe { alloc.soa.get(i2) }, 300);
    }

    #[test]
    fn test_allocation_free_reuses_indices() {
        let mut alloc: SoaAllocator<*mut usize> = SoaAllocator::new();
        let i0 = alloc.malloc(10);
        let i1 = alloc.malloc(20);
        let i2 = alloc.malloc(30);
        assert_eq!((i0, i1, i2), (0, 1, 2));

        alloc.free(i1);
        alloc.free(i0);
        // free_list is FIFO (push_back / pop_front), so i1 should be reused first.
        let r0 = alloc.malloc(99);
        assert_eq!(r0, 1);
        assert_eq!(unsafe { alloc.soa.get(r0) }, 99);

        let r1 = alloc.malloc(88);
        assert_eq!(r1, 0);
        assert_eq!(unsafe { alloc.soa.get(r1) }, 88);

        // Free list is now empty; next malloc should extend.
        let r2 = alloc.malloc(77);
        assert_eq!(r2, 3);
        assert_eq!(unsafe { alloc.soa.get(r2) }, 77);

        // i2 untouched.
        assert_eq!(unsafe { alloc.soa.get(i2) }, 30);
    }

    #[test]
    fn test_allocation_free_drops_stored_value() {
        use std::cell::Cell;

        struct DropCounter {
            counter: Rc<Cell<usize>>,
        }
        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.counter.set(self.counter.get() + 1);
            }
        }

        #[derive(Debug)]
        struct DropCounterSoa(*mut DropCounter);
        impl Default for DropCounterSoa {
            fn default() -> Self {
                Self(null_mut())
            }
        }
        impl Soa for DropCounterSoa {
            type ValueType = DropCounter;
            type RefType<'a> = ();
            type ViewType<'a> = ();
            fn with_capacity(capacity: usize) -> Self {
                debug_assert!(capacity > 0);
                Self(calloc!(DropCounter, capacity))
            }
            unsafe fn calloc(&mut self, capacity: usize) {
                debug_assert!(capacity > 0);
                self.0 = calloc!(DropCounter, capacity);
            }
            unsafe fn realloc(&mut self, old_capacity: usize, new_capacity: usize) {
                self.0 = realloc!(DropCounter, self.0, old_capacity, new_capacity);
            }
            unsafe fn set(&mut self, index: usize, node: Self::ValueType) {
                unsafe { self.0.add(index).write(node) };
            }
            unsafe fn get<'a>(&'a self, _index: usize) -> Self::ViewType<'a> {}
            unsafe fn drop_at(&mut self, index: usize) {
                unsafe { std::ptr::drop_in_place(self.0.add(index)) };
            }
        }

        let counter = Rc::new(Cell::new(0usize));
        let mut alloc: SoaAllocator<DropCounterSoa> = SoaAllocator::new();
        let i0 = alloc.malloc(DropCounter {
            counter: counter.clone(),
        });
        let i1 = alloc.malloc(DropCounter {
            counter: counter.clone(),
        });
        assert_eq!(counter.get(), 0, "no drops yet");

        alloc.free(i0);
        assert_eq!(counter.get(), 1, "free(i0) must drop the stored value");

        // Reusing the slot must not double-drop.
        let reused = alloc.malloc(DropCounter {
            counter: counter.clone(),
        });
        assert_eq!(reused, i0);
        assert_eq!(
            counter.get(),
            1,
            "malloc into freed slot must not drop anything"
        );

        alloc.free(i1);
        alloc.free(reused);
        assert_eq!(counter.get(), 3);
    }

    #[test]
    fn test_hash_table_forced_collisions() {
        // All entries share the same `hash & mask` but carry distinct payloads, so
        // they must chain within the same bucket.
        let mut t = SoaNodeTable::<(*mut usize, *mut usize)>::new();
        // After first `set`, the table grows to 8 buckets (mask = 7).
        // Use hashes that all land in bucket 3 (mod 8) but also bucket 3 (mod 16, 32, ...).
        // The simplest way: pick hashes with only the low 3 bits set identically; the
        // high bits differ so future resizes spread them out.
        let shared_low = 3u64;
        let entries: Vec<_> = (0..8u64)
            .map(|i| {
                let hash = (i << 16) | shared_low;
                (hash, (i as usize, i as usize + 100))
            })
            .collect();

        for (hash, payload) in &entries {
            let idx = t.insert(HashCached::with_hash(*payload, *hash));
            assert_eq!(t.get_index(*hash, *payload), Some(idx));
        }

        // Every entry reachable:
        for (hash, payload) in &entries {
            assert!(t.get_index(*hash, *payload).is_some());
        }

        // And duplicate inserts return the existing slot:
        for (hash, payload) in &entries {
            let original = t.get_index(*hash, *payload).unwrap();
            let again = t.insert(HashCached::with_hash(*payload, *hash));
            assert_eq!(again, original);
        }

        assert_eq!(t.hash_table.len, entries.len());
    }

    #[test]
    fn test_hash_table_miss_with_matching_hash() {
        // Same hash as stored entry but different payload → get_index must return None.
        let mut t = SoaNodeTable::<(*mut usize, *mut usize)>::new();
        let h = 0xDEAD_BEEFu64;
        t.insert(HashCached::with_hash((1, 2), h));
        assert_eq!(t.get_index(h, (1, 2)), Some(0));
        assert_eq!(t.get_index(h, (1, 3)), None);
        assert_eq!(t.get_index(h, (2, 2)), None);
    }

    #[test]
    fn test_hash_table_many_inserts_survive_multiple_resizes() {
        let mut t = SoaNodeTable::<(*mut usize, *mut usize)>::new();
        const N: usize = 256;
        for i in 0..N {
            let v = (i, i.wrapping_mul(31));
            let hc = HashCached::new(v);
            let hash = hc.hash_code();
            let idx = t.insert(hc);
            assert_eq!(idx, i, "first insert of {:?} should get next slot", v);
            assert_eq!(t.get_index(hash, v), Some(i));
        }
        assert_eq!(t.hash_table.len, N);
        // Several resizes must have occurred beyond the initial 8-bucket grow.
        assert!(t.hash_table.buckets.len() >= 16);
        // buckets_filled must never exceed the number of non-empty buckets.
        let filled_actual = t.hash_table.buckets.iter().filter(|&&b| b != -1).count();
        assert_eq!(t.hash_table.buckets_filled, filled_actual);

        // Every entry still findable after all those resizes.
        for i in 0..N {
            let v = (i, i.wrapping_mul(31));
            let hash = HashCached::new(v).hash_code();
            assert_eq!(t.get_index(hash, v), Some(i));
        }

        // Duplicate inserts after many resizes still return the original index.
        for i in 0..N {
            let v = (i, i.wrapping_mul(31));
            let hash = HashCached::new(v).hash_code();
            let again = t.insert(HashCached::with_hash(v, hash));
            assert_eq!(again, i);
        }
        assert_eq!(t.hash_table.len, N);
    }

    #[test]
    fn test_hash_table_shrink_preserves_entries() {
        let mut t = SoaNodeTable::<(*mut usize, *mut usize)>::new();
        const N: usize = 64;
        for i in 0..N {
            let v = (i, i + 1);
            t.insert(HashCached::new(v));
        }
        let big_cap = t.hash_table.buckets.len();
        assert!(big_cap >= 16);

        // Manually shrink a few times (shrink() is private but reachable from within the module).
        t.shrink();
        t.shrink();
        assert!(t.hash_table.buckets.len() < big_cap);

        // All entries must still be findable, at their original slots.
        for i in 0..N {
            let v = (i, i + 1);
            let hash = HashCached::new(v).hash_code();
            assert_eq!(t.get_index(hash, v), Some(i));
        }

        // Invariant: buckets_filled matches reality.
        let filled_actual = t.hash_table.buckets.iter().filter(|&&b| b != -1).count();
        assert_eq!(t.hash_table.buckets_filled, filled_actual);
    }

    #[test]
    fn test_hash_table_shrink_floor_is_four() {
        let mut t = SoaNodeTable::<(*mut usize, *mut usize)>::new();
        // Empty table's buckets start at 4.
        assert_eq!(t.hash_table.buckets.len(), 4);
        t.shrink();
        assert_eq!(t.hash_table.buckets.len(), 4, "shrink must not go below 4");
    }

    #[test]
    fn test_hash_table_chain_tail_insertion_order() {
        // Verify new collisions are appended to the tail of an existing chain, not the head.
        let mut t = SoaNodeTable::<(*mut usize, *mut usize)>::new();
        // Fix shared low bits so all three land in the same bucket across small resizes.
        let h_a = 0b00001u64;
        let h_b = 0b10001u64;
        let h_c = 0b100001u64;
        let a = (1usize, 10usize);
        let b = (2usize, 20usize);
        let c = (3usize, 30usize);
        let ia = t.insert(HashCached::with_hash(a, h_a));
        let ib = t.insert(HashCached::with_hash(b, h_b));
        let ic = t.insert(HashCached::with_hash(c, h_c));

        // Walk the chain starting at the bucket head:
        let mut walk = Vec::new();
        let bucket = t.hash_table.buckets[(h_a & t.hash_table.mask) as usize];
        let mut e = bucket;
        while e != -1 {
            let row = unsafe { t.allocation.soa.get(e as usize) };
            walk.push(e as usize);
            e = row.next;
        }
        // Chain order must be insertion order: ia, ib, ic.
        assert_eq!(walk, vec![ia, ib, ic]);
    }

    #[test]
    fn test_hash_table_initial_threshold_matches_growth_policy() {
        // With an initial bucket count of 4, the threshold must match the same
        // `(cap>>1)|(cap>>2)` formula `resize` uses, i.e. 3. Otherwise the first
        // insert would either grow eagerly or never grow.
        let t = SoaNodeTable::<(*mut usize, *mut usize)>::new();
        assert_eq!(t.hash_table.buckets.len(), 4);
        assert_eq!(t.hash_table.threshold, 3);
    }

    #[test]
    fn test_hash_table_grows_only_after_threshold() {
        // Threshold = 3 → the first 3 unique-bucket inserts should fit in the
        // initial 4 buckets, and the fourth forces a grow.
        let mut t = SoaNodeTable::<(*mut usize, *mut usize)>::new();
        // Force each insert into a distinct bucket by picking hashes 0,1,2,3
        // under the initial mask = 3.
        for (hash, bucket) in [(0u64, 0usize), (1, 1), (2, 2)] {
            t.insert(HashCached::with_hash((bucket, bucket), hash));
            assert_eq!(t.hash_table.buckets.len(), 4);
        }
        // Fourth insert must push buckets_filled to 3, which hits the threshold
        // at the top of `set` and triggers grow BEFORE the insert lands.
        t.insert(HashCached::with_hash((3, 3), 3));
        assert_eq!(t.hash_table.buckets.len(), 8);
        assert_eq!(t.hash_table.len, 4);
    }
}
