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
    type UnderlyingType;
    type ReferenceType: Copy;
    fn with_capacity(capacity: usize) -> Self;
    unsafe fn malloc(&mut self, capacity: usize);
    unsafe fn realloc(&mut self, old_capacity: usize, new_capacity: usize);
    /// set always takes a copy of the underlying type to correctly transfer ownership;
    unsafe fn set(&mut self, index: usize, node: Self::UnderlyingType);
    /// get always returns a copy of the underlying type, since it be referenced;
    unsafe fn get(&self, index: usize) -> Self::ReferenceType;
    /// Drops the value stored at `index` in-place. After this, the slot is logically
    /// uninitialised; a subsequent `set` at the same index does not need to (and
    /// must not) drop the previous contents.
    unsafe fn drop_at(&mut self, index: usize);
}

/// A packed version of GcflobddNode<'grammar>
pub struct RchGcflobddNode<'grammar> {
    /// only strong count
    reference_count: usize,
    inner: HashCached<GcflobddNode<'grammar>>,
    // next: usize
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum SoaNodeType {
    DontCare,
    Fork,
    Internal,
    Bdd,
}
#[derive(Debug, Default)]
struct SoaNode<'grammar> {
    /// components of Rc
    reference_count: *mut usize,

    /// components of nodes
    num_exits: *mut usize,
    grammar: *mut &'grammar Rc<GrammarNode>,
    node_type: *mut SoaNodeType,
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
struct SoaHashTableValue<SOA: Soa> {
    hash: *mut u64,
    inner: SOA,
    next: *mut isize,
}

#[derive(Debug)]
struct HashedReferenceType<SOA: Soa> {
    hash: u64,
    inner: SOA::ReferenceType,
    next: isize,
}

impl<SOA: Soa> Clone for HashedReferenceType<SOA> {
    fn clone(&self) -> Self {
        Self {
            hash: self.hash,
            inner: self.inner,
            next: self.next,
        }
    }
}
impl<SOA: Soa> Copy for HashedReferenceType<SOA> {}

impl<SOA: Soa> PartialEq for HashedReferenceType<SOA>
where
    SOA::ReferenceType: Eq,
{
    // next is not compaired
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.inner == other.inner
    }
}
impl<SOA: Soa> Eq for HashedReferenceType<SOA> where SOA::ReferenceType: Eq {}

impl<SOA: Soa> PartialEq<HashCached<SOA::UnderlyingType>> for HashedReferenceType<SOA> where SOA::UnderlyingType: std::hash::Hash, SOA::ReferenceType: PartialEq<SOA::UnderlyingType> {
    fn eq(&self, other: &HashCached<SOA::UnderlyingType>) -> bool {
        self.hash == other.hash_code() && self.inner == other.value
    }
}

impl<SOA: Soa> Soa for SoaHashTableValue<SOA> where SOA::UnderlyingType: std::hash::Hash {
    type UnderlyingType = HashCached<SOA::UnderlyingType>;
    type ReferenceType = HashedReferenceType<SOA>;
    fn with_capacity(capacity: usize) -> Self {
        debug_assert!(capacity > 0);
        Self {
            inner: SOA::with_capacity(capacity),
            next: unsafe { alloc(Layout::array::<isize>(capacity).unwrap()) as *mut isize },
            hash: unsafe { alloc(Layout::array::<u64>(capacity).unwrap()) as *mut u64 },
        }
    }
    unsafe fn malloc(&mut self, capacity: usize) {
        debug_assert!(capacity > 0);
        debug_assert_eq!(self.next, null_mut());
        unsafe { self.inner.malloc(capacity) };
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
    unsafe fn set(&mut self, index: usize, node: Self::UnderlyingType) {
        let hash_code = node.hash_code();
        unsafe { self.inner.set(index, node.value) };
        // don't write next
        unsafe { self.hash.add(index).write(hash_code) };
    }
    unsafe fn get(&self, index: usize) -> Self::ReferenceType {
        Self::ReferenceType {
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

#[derive(Debug, Clone, Copy)]
struct RefRchGcflobddNode<'grammar> {
    num_exits: usize,
    grammar: &'grammar Rc<GrammarNode>,
    node_type: SoaNodeType,
    /// See `SoaNode::union_pointer_0` — stored as `MaybeUninit` because some
    /// variants don't write all 24 bytes. Only reinterpreted through
    /// `node_type`-gated pointer casts.
    union_pointer_0: MaybeUninit<FatPointer>,
}
impl<'grammar> PartialEq for RefRchGcflobddNode<'grammar> {
    fn eq(&self, other: &Self) -> bool {
        self.num_exits == other.num_exits
            && self.grammar == other.grammar
            && match (self.node_type, other.node_type) {
                (SoaNodeType::DontCare, SoaNodeType::DontCare) => true,
                (SoaNodeType::Fork, SoaNodeType::Fork) => true,
                (SoaNodeType::Internal, SoaNodeType::Internal) => unsafe {
                    (self.union_pointer_0.as_ptr() as *const InternalNode)
                        .as_ref()
                        .unwrap_unchecked()
                        == (other.union_pointer_0.as_ptr() as *const InternalNode)
                            .as_ref()
                            .unwrap_unchecked()
                },
                (SoaNodeType::Bdd, SoaNodeType::Bdd) => unsafe {
                    (self.union_pointer_0.as_ptr() as *const Bdd)
                        .as_ref()
                        .unwrap_unchecked()
                        == (other.union_pointer_0.as_ptr() as *const Bdd)
                            .as_ref()
                            .unwrap_unchecked()
                },
                _ => false,
            }
    }
}
impl<'grammar> Eq for RefRchGcflobddNode<'grammar> {}

impl<'grammar> PartialEq<RchGcflobddNode<'grammar>> for RefRchGcflobddNode<'grammar> {
    fn eq(&self, other: &RchGcflobddNode<'grammar>) -> bool {
        self.num_exits == other.inner.num_exits
            && self.grammar == other.inner.grammar
            && match (self.node_type, &other.inner.node) {
                (SoaNodeType::DontCare, GcflobddNodeType::DontCare) => true,
                (SoaNodeType::Fork, GcflobddNodeType::Fork) => true,
                (SoaNodeType::Internal, GcflobddNodeType::Internal(internal_node)) => unsafe {
                    (self.union_pointer_0.as_ptr() as *const InternalNode)
                        .as_ref()
                        .unwrap_unchecked()
                        == internal_node
                },
                (SoaNodeType::Bdd, GcflobddNodeType::Bdd(bdd)) => unsafe {
                    (self.union_pointer_0.as_ptr() as *const Bdd)
                        .as_ref()
                        .unwrap_unchecked()
                        == bdd
                },
                _ => false,
            }
    }
}

impl<'grammar> Soa for SoaNode<'grammar> {
    type UnderlyingType = RchGcflobddNode<'grammar>;
    type ReferenceType = RefRchGcflobddNode<'grammar>;
    fn with_capacity(capacity: usize) -> Self {
        debug_assert!(capacity > 0);
        Self {
            reference_count: unsafe {
                alloc(Layout::array::<usize>(capacity).unwrap()) as *mut usize
            },
            num_exits: unsafe { alloc(Layout::array::<usize>(capacity).unwrap()) as *mut usize },
            grammar: unsafe {
                alloc(Layout::array::<&'grammar Rc<GrammarNode>>(capacity).unwrap())
                    as *mut &'grammar Rc<GrammarNode>
            },
            node_type: unsafe {
                alloc(Layout::array::<SoaNodeType>(capacity).unwrap()) as *mut SoaNodeType
            },
            union_pointer_0: unsafe {
                alloc(Layout::array::<MaybeUninit<FatPointer>>(capacity).unwrap())
                    as *mut MaybeUninit<FatPointer>
            },
        }
    }

    unsafe fn malloc(&mut self, capacity: usize) {
        debug_assert!(capacity > 0);
        debug_assert_eq!(self.reference_count, null_mut());
        debug_assert_eq!(self.num_exits, null_mut());
        debug_assert_eq!(self.grammar, null_mut());
        debug_assert_eq!(self.node_type, null_mut());
        debug_assert_eq!(self.union_pointer_0, null_mut());
        self.reference_count =
            unsafe { alloc(Layout::array::<usize>(capacity).unwrap_unchecked()) as *mut usize };
        self.num_exits =
            unsafe { alloc(Layout::array::<usize>(capacity).unwrap_unchecked()) as *mut usize };
        self.grammar = unsafe {
            alloc(Layout::array::<&'grammar Rc<GrammarNode>>(capacity).unwrap_unchecked())
                as *mut &'grammar Rc<GrammarNode>
        };
        self.node_type = unsafe {
            alloc(Layout::array::<SoaNodeType>(capacity).unwrap_unchecked()) as *mut SoaNodeType
        };
        self.union_pointer_0 = unsafe {
            alloc(Layout::array::<MaybeUninit<FatPointer>>(capacity).unwrap_unchecked())
                as *mut MaybeUninit<FatPointer>
        };
    }

    unsafe fn realloc(&mut self, old_capacity: usize, new_capacity: usize) {
        self.reference_count = unsafe {
            realloc(
                self.reference_count as *mut u8,
                Layout::array::<usize>(old_capacity).unwrap_unchecked(),
                new_capacity * std::mem::size_of::<usize>(),
            ) as *mut usize
        };
        self.num_exits = unsafe {
            realloc(
                self.num_exits as *mut u8,
                Layout::array::<usize>(old_capacity).unwrap_unchecked(),
                new_capacity * std::mem::size_of::<usize>(),
            ) as *mut usize
        };
        self.grammar = unsafe {
            realloc(
                self.grammar as *mut u8,
                Layout::array::<&'grammar Rc<GrammarNode>>(old_capacity).unwrap_unchecked(),
                new_capacity * std::mem::size_of::<&'grammar Rc<GrammarNode>>(),
            ) as *mut &'grammar Rc<GrammarNode>
        };
        self.node_type = unsafe {
            realloc(
                self.node_type as *mut u8,
                Layout::array::<SoaNodeType>(old_capacity).unwrap_unchecked(),
                new_capacity * std::mem::size_of::<SoaNodeType>(),
            ) as *mut SoaNodeType
        };
        self.union_pointer_0 = unsafe {
            realloc(
                self.union_pointer_0 as *mut u8,
                Layout::array::<MaybeUninit<FatPointer>>(old_capacity).unwrap_unchecked(),
                new_capacity * std::mem::size_of::<MaybeUninit<FatPointer>>(),
            ) as *mut MaybeUninit<FatPointer>
        };
    }
    unsafe fn set(&mut self, index: usize, node: Self::UnderlyingType) {
        unsafe { self.reference_count.add(index).write(node.reference_count) };
        unsafe { self.num_exits.add(index).write(node.inner.num_exits) };
        unsafe { self.grammar.add(index).write(node.inner.grammar) };
        match node.inner.value.node {
            GcflobddNodeType::DontCare => {
                unsafe { self.node_type.add(index).write(SoaNodeType::DontCare) };
                // don't need to write for DontCare
                // unsafe { self.union_pointer_0.add(index).write((0, 0, 0)) };
            }
            super::node::GcflobddNodeType::Fork => {
                unsafe { self.node_type.add(index).write(SoaNodeType::Fork) };
                // don't need to write for Fork
                // unsafe { self.union_pointer_0.add(index).write((0, 0, 0)) };
            }
            super::node::GcflobddNodeType::Internal(internal_node) => {
                unsafe { self.node_type.add(index).write(SoaNodeType::Internal) };
                unsafe {
                    (self.union_pointer_0.add(index) as *mut Vec<Vec<Connection>>)
                        .write(internal_node.connections)
                };
            }
            super::node::GcflobddNodeType::Bdd(bdd) => {
                unsafe { self.node_type.add(index).write(SoaNodeType::Bdd) };
                unsafe { (self.union_pointer_0.add(index) as *mut Bdd).write(bdd) };
            }
        }
    }
    unsafe fn get(&self, index: usize) -> Self::ReferenceType {
        RefRchGcflobddNode {
            num_exits: unsafe { self.num_exits.add(index).read() },
            grammar: unsafe { self.grammar.add(index).read() },
            node_type: unsafe { self.node_type.add(index).read() },
            union_pointer_0: unsafe { self.union_pointer_0.add(index).read() },
        }
    }
    unsafe fn drop_at(&mut self, index: usize) {
        // reference_count, num_exits: usize — no drop.
        // grammar: &'grammar Rc<_> — borrow, no drop.
        // node_type: enum with no heap data — no drop.
        // Only the union payload may own heap data, and only for Internal / Bdd.
        let node_type = unsafe { self.node_type.add(index).read() };
        match node_type {
            SoaNodeType::DontCare | SoaNodeType::Fork => {}
            SoaNodeType::Internal => unsafe {
                std::ptr::drop_in_place(
                    self.union_pointer_0.add(index) as *mut Vec<Vec<Connection>>,
                );
            },
            SoaNodeType::Bdd => unsafe {
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
                self.soa.malloc(new_cap);
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
    fn push(&mut self, node: SOA::UnderlyingType) {
        if self.len == self.capacity {
            self.grow();
        }
        unsafe {
            self.soa.set(self.len, node);
        };
        self.len += 1;
    }
    unsafe fn set(&mut self, index: usize, node: SOA::UnderlyingType) {
        unsafe {
            self.soa.set(index, node);
        };
    }
    unsafe fn get(&self, index: usize) -> SOA::ReferenceType {
        unsafe { self.soa.get(index) }
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
    pub fn malloc(&mut self, value: SOA::UnderlyingType) -> usize {
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

pub struct SoaNodeTable<T: Soa> where T::UnderlyingType: std::hash::Hash {
    hash_table: SoaAllocatorHashTable,
    allocation: SoaAllocator<SoaHashTableValue<T>>,
}

impl<T: Soa> SoaNodeTable<T> where T::UnderlyingType: std::hash::Hash {
    pub fn new() -> Self {
        Self {
            hash_table: SoaAllocatorHashTable::new(),
            allocation: SoaAllocator::new(),
        }
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
    T::ReferenceType: Eq,
    T::UnderlyingType: std::hash::Hash,
{
    pub fn get_index(&self, hash: u64, value: T::ReferenceType) -> Option<usize> {
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
    T::ReferenceType: PartialEq<T::UnderlyingType>,
    T::UnderlyingType: std::hash::Hash,
{
    pub fn set(&mut self, value: HashCached<T::UnderlyingType>) -> usize {
        // Grow when 75% of buckets are filled
        let hash = value.hash_code();
        if self.hash_table.buckets_filled >= self.hash_table.threshold {
            self.grow();
        }

        let bucket_idx = (hash & self.hash_table.mask) as usize;
        let mut entry = self.hash_table.buckets[bucket_idx];

        if entry == -1 {
            let slot = self
                .allocation
                .malloc(value);
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

        let slot = self
            .allocation
            .malloc(value);

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
        type UnderlyingType = usize;

        type ReferenceType = usize;

        fn with_capacity(capacity: usize) -> Self {
            debug_assert!(capacity > 0);
            unsafe { alloc(Layout::array::<usize>(capacity).unwrap()) as *mut usize }
        }

        unsafe fn malloc(&mut self, capacity: usize) {
            debug_assert!(capacity > 0);
            *self = unsafe { alloc(Layout::array::<usize>(capacity).unwrap()) as *mut usize };
        }

        unsafe fn realloc(&mut self, old_capacity: usize, new_capacity: usize) {
            *self = unsafe {
                realloc(
                    *self as *mut u8,
                    Layout::array::<usize>(old_capacity).unwrap_unchecked(),
                    new_capacity * std::mem::size_of::<usize>(),
                ) as *mut usize
            };
        }

        unsafe fn set(&mut self, index: usize, node: Self::UnderlyingType) {
            unsafe { self.add(index).write(node) };
        }
        unsafe fn get(&self, index: usize) -> Self::ReferenceType {
            unsafe { self.add(index).read() }
        }
        unsafe fn drop_at(&mut self, _index: usize) {
            // usize is Copy — nothing to drop.
        }
    }
    impl Soa for (*mut usize, *mut usize) {
        type UnderlyingType = (usize, usize);
        type ReferenceType = (usize, usize);

        fn with_capacity(capacity: usize) -> Self {
            debug_assert!(capacity > 0);
            unsafe {
                (
                    alloc(Layout::array::<usize>(capacity).unwrap()) as *mut usize,
                    alloc(Layout::array::<usize>(capacity).unwrap()) as *mut usize,
                )
            }
        }

        unsafe fn malloc(&mut self, capacity: usize) {
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

        unsafe fn set(&mut self, index: usize, node: Self::UnderlyingType) {
            unsafe { self.0.add(index).write(node.0) };

            unsafe { self.1.add(index).write(node.1) };
        }
        unsafe fn get(&self, index: usize) -> Self::ReferenceType {
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
        let idx1 = hash_table.set(val1);
        assert_eq!(idx1, 0);
        assert_eq!(hash_table.get_index(hash1, (0, 1)), Some(0));
        assert_eq!(unsafe { hash_table.allocation.soa.get(0) }.inner, (0, 1));

        // Test getting non-existent value
        assert_eq!(hash_table.get_index(12345, (9, 9)), None);

        // Test setting identical value (should return existing index)
        let val1_dup = HashCached::new((0, 1));
        let idx1_dup = hash_table.set(val1_dup);
        assert_eq!(idx1_dup, 0);
        assert_eq!(hash_table.hash_table.len, 1);

        // Test triggering a resize (capacity grows)
        for i in 1..10 {
            let val = HashCached::new((i, i + 1));
            let hash = val.hash_code();
            let idx = hash_table.set(val);
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
            type UnderlyingType = DropCounter;
            type ReferenceType = ();
            fn with_capacity(capacity: usize) -> Self {
                debug_assert!(capacity > 0);
                Self(unsafe {
                    alloc(Layout::array::<DropCounter>(capacity).unwrap()) as *mut DropCounter
                })
            }
            unsafe fn malloc(&mut self, capacity: usize) {
                debug_assert!(capacity > 0);
                self.0 = unsafe {
                    alloc(Layout::array::<DropCounter>(capacity).unwrap()) as *mut DropCounter
                };
            }
            unsafe fn realloc(&mut self, old_capacity: usize, new_capacity: usize) {
                self.0 = unsafe {
                    realloc(
                        self.0 as *mut u8,
                        Layout::array::<DropCounter>(old_capacity).unwrap_unchecked(),
                        new_capacity * std::mem::size_of::<DropCounter>(),
                    ) as *mut DropCounter
                };
            }
            unsafe fn set(&mut self, index: usize, node: Self::UnderlyingType) {
                unsafe { self.0.add(index).write(node) };
            }
            unsafe fn get(&self, _index: usize) -> Self::ReferenceType {}
            unsafe fn drop_at(&mut self, index: usize) {
                unsafe { std::ptr::drop_in_place(self.0.add(index)) };
            }
        }

        let counter = Rc::new(Cell::new(0usize));
        let mut alloc: SoaAllocator<DropCounterSoa> = SoaAllocator::new();
        let i0 = alloc.malloc(DropCounter { counter: counter.clone() });
        let i1 = alloc.malloc(DropCounter { counter: counter.clone() });
        assert_eq!(counter.get(), 0, "no drops yet");

        alloc.free(i0);
        assert_eq!(counter.get(), 1, "free(i0) must drop the stored value");

        // Reusing the slot must not double-drop.
        let reused = alloc.malloc(DropCounter { counter: counter.clone() });
        assert_eq!(reused, i0);
        assert_eq!(counter.get(), 1, "malloc into freed slot must not drop anything");

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
            let idx = t.set(HashCached::with_hash(*payload, *hash));
            assert_eq!(t.get_index(*hash, *payload), Some(idx));
        }

        // Every entry reachable:
        for (hash, payload) in &entries {
            assert!(t.get_index(*hash, *payload).is_some());
        }

        // And duplicate inserts return the existing slot:
        for (hash, payload) in &entries {
            let original = t.get_index(*hash, *payload).unwrap();
            let again = t.set(HashCached::with_hash(*payload, *hash));
            assert_eq!(again, original);
        }

        assert_eq!(t.hash_table.len, entries.len());
    }

    #[test]
    fn test_hash_table_miss_with_matching_hash() {
        // Same hash as stored entry but different payload → get_index must return None.
        let mut t = SoaNodeTable::<(*mut usize, *mut usize)>::new();
        let h = 0xDEAD_BEEFu64;
        t.set(HashCached::with_hash((1, 2), h));
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
            let idx = t.set(hc);
            assert_eq!(idx, i, "first insert of {:?} should get next slot", v);
            assert_eq!(t.get_index(hash, v), Some(i));
        }
        assert_eq!(t.hash_table.len, N);
        // Several resizes must have occurred beyond the initial 8-bucket grow.
        assert!(t.hash_table.buckets.len() >= 16);
        // buckets_filled must never exceed the number of non-empty buckets.
        let filled_actual = t
            .hash_table
            .buckets
            .iter()
            .filter(|&&b| b != -1)
            .count();
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
            let again = t.set(HashCached::with_hash(v, hash));
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
            t.set(HashCached::new(v));
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
        let filled_actual = t
            .hash_table
            .buckets
            .iter()
            .filter(|&&b| b != -1)
            .count();
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
        let ia = t.set(HashCached::with_hash(a, h_a));
        let ib = t.set(HashCached::with_hash(b, h_b));
        let ic = t.set(HashCached::with_hash(c, h_c));

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
            t.set(HashCached::with_hash((bucket, bucket), hash));
            assert_eq!(t.hash_table.buckets.len(), 4);
        }
        // Fourth insert must push buckets_filled to 3, which hits the threshold
        // at the top of `set` and triggers grow BEFORE the insert lands.
        t.set(HashCached::with_hash((3, 3), 3));
        assert_eq!(t.hash_table.buckets.len(), 8);
        assert_eq!(t.hash_table.len, 4);
    }
}
