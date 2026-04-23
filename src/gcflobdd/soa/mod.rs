use static_assertions::const_assert;
use std::alloc::{Layout, alloc, realloc};
use std::cell::UnsafeCell;
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

// Page-aware chunk size. Target: make each 8-byte per-element SoA array one memory page
// so the system allocator serves it via mmap (fewer syscalls, lazy-backed pages, no heap
// fragmentation). The 1-byte node_type array ends up sub-page but that's unavoidable
// without pushing the 24-byte union_pointer_0 out of L2.
#[cfg(target_os = "macos")]
pub const LOG_CHUNK: usize = 11; // 16 KiB page → 2048 elements, 8-byte arrays fill a page.
#[cfg(not(target_os = "macos"))]
pub const LOG_CHUNK: usize = 9; // 4 KiB page → 512 elements.
pub const CHUNK_SIZE: usize = 1 << LOG_CHUNK;
const CHUNK_MASK: usize = CHUNK_SIZE - 1;

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
pub(super) struct SoaHashSetValue<T> {
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

// ──────────────────────────────────────────────────────────────────────────────
// Pointer-stable chunked SoA storage.
//
// Each chunk is a heap-allocated SOA with fixed capacity `CHUNK_SIZE`. Chunks'
// heap addresses never change once allocated — so raw pointers into a chunk's
// backing arrays remain valid for the lifetime of the chunked vec.
//
// Appending a new slot is possible through a shared reference: the `inner`
// field sits inside `UnsafeCell`, and `push` takes `&self`. This is safe
// because:
//   1) it only writes to a fresh slot index >= `len`, which no outstanding
//      view references;
//   2) growing the chunk list (when the tail chunk is full) allocates a new
//      chunk — it never moves existing chunks' buffers;
//   3) the outer `Vec<*mut SOA>` can resize, but that only shuffles raw
//      pointer *values*, not the SOA structs at the addresses they point to.
//
// Invariant: single-threaded use. No concurrent `push` / mutation on the same
// `SoaChunkedVec` — this is enforced structurally (the whole crate is
// single-threaded; the `sync` feature scaffolding lives elsewhere).
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub(super) struct SoaChunkedVec<SOA: Soa> {
    inner: UnsafeCell<SoaChunkedVecInner<SOA>>,
}

#[derive(Debug)]
struct SoaChunkedVecInner<SOA: Soa> {
    /// Each chunk is `Box::into_raw(Box::new(SOA::with_capacity(CHUNK_SIZE)))`.
    /// The outer Vec may resize; that moves pointer *values*, not the SOA structs.
    chunks: Vec<*mut SOA>,
    len: usize,
}

impl<SOA: Soa> Default for SoaChunkedVec<SOA> {
    fn default() -> Self {
        Self {
            inner: UnsafeCell::new(SoaChunkedVecInner {
                chunks: Vec::new(),
                len: 0,
            }),
        }
    }
}

impl<SOA: Soa> SoaChunkedVec<SOA> {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        // SAFETY: single-threaded; `len` is a usize, no invariant crossed.
        unsafe { (*self.inner.get()).len }
    }

    /// Append `value` to the end, returning the new slot index.
    ///
    /// # Safety
    /// Single-threaded. No concurrent call to any method on `self`. Views into
    /// already-initialised slots (`< len` at the time they were produced) remain
    /// valid across this call.
    pub unsafe fn push(&self, value: SOA::ValueType) -> usize {
        let inner = unsafe { &mut *self.inner.get() };
        let slot = inner.len;
        let chunk_idx = slot >> LOG_CHUNK;
        let offset = slot & CHUNK_MASK;
        if chunk_idx == inner.chunks.len() {
            // Tail chunk full — allocate a new one.
            let new_chunk = Box::into_raw(Box::new(SOA::with_capacity(CHUNK_SIZE)));
            inner.chunks.push(new_chunk);
        }
        // SAFETY: chunks[chunk_idx] is a stable heap pointer to SOA. We write into
        // its arrays at `offset`; no outstanding view references this new slot
        // (it's past the old `len`).
        let chunk = inner.chunks[chunk_idx];
        unsafe { (*chunk).set(offset, value) };
        inner.len = slot + 1;
        slot
    }

    /// Overwrite an existing initialised slot. Requires `&mut` since the caller
    /// must guarantee no outstanding views reference the slot being overwritten.
    pub unsafe fn set(&mut self, index: usize, value: SOA::ValueType) {
        let inner = self.inner.get_mut();
        let chunk_idx = index >> LOG_CHUNK;
        let offset = index & CHUNK_MASK;
        let chunk = inner.chunks[chunk_idx];
        unsafe { (*chunk).set(offset, value) };
    }

    /// Returns a view of the slot at `index`. The view contains references into
    /// chunk heap buffers, which are stable for `'a = &'a self`.
    ///
    /// # Safety
    /// The slot at `index` must be initialised (`< self.len()` at some prior point,
    /// and not freed without re-initialisation).
    #[inline]
    pub unsafe fn get<'a>(&'a self, index: usize) -> SOA::ViewType<'a> {
        let inner = unsafe { &*self.inner.get() };
        let chunk_idx = index >> LOG_CHUNK;
        let offset = index & CHUNK_MASK;
        // SAFETY: chunk pointer is stable; `get` reads by raw pointer through the
        // chunk's SOA; `'a` outlives the returned view.
        unsafe { (*inner.chunks[chunk_idx]).get(offset) }
    }

    /// Raw pointer to the chunk owning `index`, plus the offset within that chunk.
    /// Callers may read/write SOA-private fields through raw pointer math.
    ///
    /// # Safety
    /// Caller must uphold field-level aliasing: any write must not overlap with a
    /// live reference returned by `get()`. In practice this is used only for
    /// hash-chain `next` / `hash` link updates during `insert`, which are distinct
    /// heap buffers from the `union_pointer_0` that views reference.
    #[inline]
    pub(super) unsafe fn chunk_ptr(&self, index: usize) -> (*mut SOA, usize) {
        let inner = unsafe { &*self.inner.get() };
        let chunk_idx = index >> LOG_CHUNK;
        let offset = index & CHUNK_MASK;
        (inner.chunks[chunk_idx], offset)
    }

    pub unsafe fn drop_at(&mut self, index: usize) {
        let inner = self.inner.get_mut();
        let chunk_idx = index >> LOG_CHUNK;
        let offset = index & CHUNK_MASK;
        let chunk = inner.chunks[chunk_idx];
        unsafe { (*chunk).drop_at(offset) };
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Allocator over a chunked SoA: append-only through `&self`, reuse via free-list
// only through `&mut self` (GC-only).
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub(super) struct SoaAllocator<SOA: Soa> {
    soa: SoaChunkedVec<SOA>,
    /// Free-list is only touched through `&mut self` (GC path). UnsafeCell would
    /// reintroduce the re-entrancy hazard the whole refactor was designed to
    /// avoid, so we keep plain ownership and a `&mut`-only API.
    free_list: VecDeque<usize>,
}

impl<SOA: Soa> SoaAllocator<SOA> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append-only allocation, callable through a shared reference.
    ///
    /// # Safety
    /// Single-threaded; no concurrent `append_only` on the same allocator.
    pub unsafe fn append_only(&self, value: SOA::ValueType) -> usize {
        // SAFETY: forwards to `SoaChunkedVec::push` which upholds the same invariants.
        unsafe { self.soa.push(value) }
    }

    /// Traditional malloc that reuses freed slots. Requires exclusive access —
    /// used only during GC-driven rebuilds (currently not wired up for the
    /// gcflobdd node table).
    pub fn malloc(&mut self, value: SOA::ValueType) -> usize {
        if let Some(index) = self.free_list.pop_front() {
            unsafe { self.soa.set(index, value) };
            index
        } else {
            unsafe { self.soa.push(value) }
        }
    }

    pub fn free(&mut self, index: usize) {
        unsafe { self.soa.drop_at(index) };
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

// ──────────────────────────────────────────────────────────────────────────────
// SoaNodeTable: hash-cons table built on chunked SoA storage.
//
// The hot path (`insert` / `get_index` / `get_view`) all work through a shared
// reference. Mutation routes through `UnsafeCell`-protected fields. Views
// returned by `get_view` reference heap buffers owned by individual chunks —
// those buffers are pointer-stable for the lifetime of the table.
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct SoaNodeTable<T: Soa>
where
    T::ValueType: std::hash::Hash,
{
    hash_table: UnsafeCell<SoaAllocatorHashTable>,
    allocation: SoaAllocator<SoaHashSetValue<T>>,
}

impl<T: Soa> Default for SoaNodeTable<T>
where
    T::ValueType: std::hash::Hash,
{
    fn default() -> Self {
        Self {
            hash_table: UnsafeCell::new(SoaAllocatorHashTable::new()),
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
        // SAFETY: single-threaded; reading a usize through the UnsafeCell.
        unsafe { (*self.hash_table.get()).len }
    }

    /// Rehash in place. Private — callers only double or halve the bucket count
    /// (`grow` / `shrink`). Under that invariant, rehashed entries never land in
    /// a bucket index both `> i` and `< old_capacity`, so rewriting `buckets[i]`
    /// in place cannot corrupt unprocessed chains.
    unsafe fn resize(&self, new_capacity: usize) {
        // SAFETY: single-threaded; no outstanding reference to `hash_table`.
        let hash_table = unsafe { &mut *self.hash_table.get() };
        debug_assert!(new_capacity.is_power_of_two());
        let old_capacity = hash_table.buckets.len();
        debug_assert!(
            new_capacity == old_capacity * 2 || new_capacity * 2 == old_capacity,
            "resize must double or halve the bucket count"
        );
        let new_mask = (new_capacity - 1) as u64;
        let mut new_buckets_filled = 0;

        if new_capacity > old_capacity {
            hash_table.buckets.resize(new_capacity, -1);
        }

        for i in 0..old_capacity {
            let mut entry = hash_table.buckets[i];
            hash_table.buckets[i] = -1;

            while entry != -1 {
                let candidate = unsafe { self.allocation.soa.get(entry as usize) };
                let next_entry = candidate.next;

                let bucket_idx = (candidate.hash & new_mask) as usize;
                // Rewire the chain's `next` pointer for this entry to the head
                // of the new bucket's chain.
                let (chunk, offset) = unsafe { self.allocation.soa.chunk_ptr(entry as usize) };
                unsafe {
                    (*chunk).next.add(offset).write(hash_table.buckets[bucket_idx]);
                }
                if hash_table.buckets[bucket_idx] == -1 {
                    new_buckets_filled += 1;
                }
                hash_table.buckets[bucket_idx] = entry;

                entry = next_entry;
            }
        }

        if new_capacity < old_capacity {
            hash_table.buckets.truncate(new_capacity);
        }

        hash_table.mask = new_mask;
        // threshold should be capacity * 0.75
        hash_table.threshold = (new_capacity >> 1) | (new_capacity >> 2);
        hash_table.buckets_filled = new_buckets_filled;
    }

    fn grow(&self) {
        // SAFETY: single-threaded.
        let current_cap = unsafe { (*self.hash_table.get()).buckets.len() };
        unsafe { self.resize(current_cap * 2) };
    }

    fn shrink(&mut self) {
        let current_cap = self.hash_table.get_mut().buckets.len();
        if current_cap > 4 {
            unsafe { self.resize(current_cap / 2) };
        }
    }
}
impl<T: Soa> SoaNodeTable<T>
where
    T::ValueType: std::hash::Hash,
{
    /// View a slot. The returned view borrows heap buffers owned by a chunk of
    /// the SoA; those buffers are pointer-stable for the lifetime of `&self`.
    ///
    /// # Safety
    /// `index` must refer to an initialised, non-freed slot.
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
        // SAFETY: single-threaded; read-only access to hash_table.
        let hash_table = unsafe { &*self.hash_table.get() };
        let mut entry = hash_table.buckets[(hash & hash_table.mask) as usize];
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
    /// Insert a value and return its slot index. Callable through a shared
    /// reference — views outstanding at call time remain valid because chunk
    /// buffers never move.
    ///
    /// # Safety
    /// Single-threaded; no concurrent `insert` on the same table.
    pub unsafe fn insert<'a>(&'a self, value: HashCached<T::ValueType>) -> usize
    where
        T::ViewType<'a>: PartialEq<T::ValueType>,
    {
        let hash = value.hash_code();

        // SAFETY: single-threaded; no outstanding reference to `hash_table`
        // (callers fetch `get_view` / `get_index` in prior, completed calls).
        let over_threshold = unsafe {
            let ht = &*self.hash_table.get();
            ht.buckets_filled >= ht.threshold
        };
        if over_threshold {
            self.grow();
        }

        // Re-read hash table state after possible grow.
        let hash_table = unsafe { &mut *self.hash_table.get() };
        let bucket_idx = (hash & hash_table.mask) as usize;
        let mut entry = hash_table.buckets[bucket_idx];

        if entry == -1 {
            let slot = unsafe { self.allocation.append_only(value) };
            hash_table.buckets[bucket_idx] = slot as isize;
            let (chunk, offset) = unsafe { self.allocation.soa.chunk_ptr(slot) };
            unsafe { (*chunk).next.add(offset).write(-1) };
            hash_table.buckets_filled += 1;
            hash_table.len += 1;
            return slot;
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

        let slot = unsafe { self.allocation.append_only(value) };

        let (chunk, offset) = unsafe { self.allocation.soa.chunk_ptr(last_entry as usize) };
        unsafe {
            *((*chunk).next.add(offset)) = slot as isize;
        }
        let (chunk, offset) = unsafe { self.allocation.soa.chunk_ptr(slot) };
        unsafe {
            (*chunk).next.add(offset).write(-1);
        }

        hash_table.len += 1;
        slot
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
    fn test_chunked_vec_single_chunk() {
        let vec = SoaChunkedVec::<*mut usize>::new();
        for i in 0..4 {
            assert_eq!(unsafe { vec.push(i * 10) }, i);
        }
        assert_eq!(vec.len(), 4);
        for i in 0..4 {
            assert_eq!(unsafe { vec.get(i) }, i * 10);
        }
    }

    #[test]
    fn test_chunked_vec_crosses_chunk_boundary() {
        // Push enough to force several new chunks. Verify every earlier slot is
        // still readable — i.e. pointers in the first chunk did not move when
        // later chunks were allocated.
        let vec = SoaChunkedVec::<*mut usize>::new();
        let total = CHUNK_SIZE * 3 + 7;
        for i in 0..total {
            assert_eq!(unsafe { vec.push(i * 7 + 3) }, i);
        }
        for i in 0..total {
            assert_eq!(unsafe { vec.get(i) }, i * 7 + 3);
        }
    }

    #[test]
    fn test_chunked_vec_push_through_shared_ref() {
        // Type-level check: push is callable through `&SoaChunkedVec`.
        fn push_twice(v: &SoaChunkedVec<*mut usize>) {
            unsafe {
                v.push(111);
                v.push(222);
            }
        }
        let vec = SoaChunkedVec::<*mut usize>::new();
        push_twice(&vec);
        assert_eq!(vec.len(), 2);
        assert_eq!(unsafe { vec.get(0) }, 111);
        assert_eq!(unsafe { vec.get(1) }, 222);
    }

    #[test]
    fn test_compound_chunked_vec() {
        let vec = SoaChunkedVec::<(*mut usize, *mut usize)>::new();
        for i in 0..(CHUNK_SIZE + 3) {
            assert_eq!(unsafe { vec.push((i, i + 1)) }, i);
        }
        for i in 0..(CHUNK_SIZE + 3) {
            assert_eq!(unsafe { vec.get(i) }, (i, i + 1));
        }
    }

    #[test]
    fn test_hash_table() {
        let hash_table = SoaNodeTable::<(*mut usize, *mut usize)>::new();

        // Test basic insert and get
        let val1 = HashCached::new((0, 1));
        let hash1 = val1.hash_code();
        let idx1 = unsafe { hash_table.insert(val1) };
        assert_eq!(idx1, 0);
        assert_eq!(hash_table.get_index(hash1, (0, 1)), Some(0));

        // Test getting non-existent value
        assert_eq!(hash_table.get_index(12345, (9, 9)), None);

        // Test inserting identical value (should return existing index)
        let val1_dup = HashCached::new((0, 1));
        let idx1_dup = unsafe { hash_table.insert(val1_dup) };
        assert_eq!(idx1_dup, 0);
        assert_eq!(hash_table.len(), 1);

        // Test triggering a resize (capacity grows)
        for i in 1..10 {
            let val = HashCached::new((i, i + 1));
            let hash = val.hash_code();
            let idx = unsafe { hash_table.insert(val) };
            assert_eq!(idx, i as usize);
            assert_eq!(hash_table.get_index(hash, (i, i + 1)), Some(i as usize));
        }

        assert_eq!(hash_table.len(), 10);
        // Verify old items still exist after resize
        for i in 0..10 {
            let val = HashCached::new((i, i + 1));
            let hash = val.hash_code();
            assert_eq!(hash_table.get_index(hash, (i, i + 1)), Some(i as usize));
        }
    }

    #[test]
    fn test_view_survives_many_inserts() {
        // Insert a value, capture a view, insert enough more to force several new
        // chunks, then re-validate the original view's underlying data.
        let table = SoaNodeTable::<(*mut usize, *mut usize)>::new();
        let seed = (42usize, 4242usize);
        let seed_idx = unsafe { table.insert(HashCached::new(seed)) };

        // Capture a view *before* the bulk inserts.
        let view_before = unsafe { table.get_view(seed_idx) };
        assert_eq!(view_before.inner, seed);

        // Push enough to cross several chunk boundaries.
        for i in 1..(CHUNK_SIZE * 3) {
            unsafe { table.insert(HashCached::new((i, i.wrapping_mul(7)))) };
        }

        // The view captured before was a Copy (hash/inner/next are all by-value
        // for this SOA's ViewType). A fresh view at the same index must still
        // yield the original payload — i.e. the chunk holding slot 0 is
        // untouched by all the later chunk allocations.
        let view_after = unsafe { table.get_view(seed_idx) };
        assert_eq!(view_after.inner, seed);
        assert_eq!(view_before.inner, view_after.inner);
    }

    #[test]
    fn test_view_survives_hash_table_resize() {
        // Force a hash-table bucket resize while holding a slot's payload and
        // verify the slot data is intact (resize only touches buckets/next, not
        // the stored inner values).
        let table = SoaNodeTable::<(*mut usize, *mut usize)>::new();
        let pinned = (7usize, 77usize);
        let pinned_idx = unsafe { table.insert(HashCached::new(pinned)) };

        for i in 1..64 {
            unsafe { table.insert(HashCached::new((i, i * 11))) };
        }

        let view = unsafe { table.get_view(pinned_idx) };
        assert_eq!(view.inner, pinned);
    }
}
