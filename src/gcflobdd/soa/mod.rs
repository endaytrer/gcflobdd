use static_assertions::const_assert;
use std::alloc::{Layout, alloc, realloc};
use std::collections::VecDeque;
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
    /// - DontCare and Fork: does not apply
    /// - Internal: fat pointer to connections
    /// - Bdd: a pointer to Bdd
    union_pointer_0: *mut FatPointer,
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
        Self {
            inner: SOA::with_capacity(capacity),
            next: unsafe { alloc(Layout::array::<isize>(capacity).unwrap()) as *mut isize },
            hash: unsafe { alloc(Layout::array::<u64>(capacity).unwrap()) as *mut u64 },
        }
    }
    unsafe fn malloc(&mut self, capacity: usize) {
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
}

const_assert!(std::mem::size_of::<FatPointer>() == std::mem::size_of::<Vec<Vec<Connection>>>());
const_assert!(std::mem::size_of::<FatPointer>() == std::mem::size_of::<Bdd>() + 16);

#[derive(Debug, Clone, Copy)]
struct RefRchGcflobddNode<'grammar> {
    num_exits: usize,
    grammar: &'grammar Rc<GrammarNode>,
    node_type: SoaNodeType,
    union_pointer_0: FatPointer,
}
impl<'grammar> PartialEq for RefRchGcflobddNode<'grammar> {
    fn eq(&self, other: &Self) -> bool {
        self.num_exits == other.num_exits
            && self.grammar == other.grammar
            && match (self.node_type, other.node_type) {
                (SoaNodeType::DontCare, SoaNodeType::DontCare) => true,
                (SoaNodeType::Fork, SoaNodeType::Fork) => true,
                (SoaNodeType::Internal, SoaNodeType::Internal) => unsafe {
                    (&self.union_pointer_0 as *const FatPointer as *const InternalNode)
                        .as_ref()
                        .unwrap_unchecked()
                        == (&other.union_pointer_0 as *const FatPointer as *const InternalNode)
                            .as_ref()
                            .unwrap_unchecked()
                },
                (SoaNodeType::Bdd, SoaNodeType::Bdd) => unsafe {
                    (&self.union_pointer_0 as *const FatPointer as *const Bdd)
                        .as_ref()
                        .unwrap_unchecked()
                        == (&other.union_pointer_0 as *const FatPointer as *const Bdd)
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
                    (&self.union_pointer_0 as *const FatPointer as *const InternalNode)
                        .as_ref()
                        .unwrap_unchecked()
                        == internal_node
                },
                (SoaNodeType::Bdd, GcflobddNodeType::Bdd(bdd)) => unsafe {
                    (&self.union_pointer_0 as *const FatPointer as *const Bdd)
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
                alloc(Layout::array::<FatPointer>(capacity).unwrap()) as *mut FatPointer
            },
        }
    }

    unsafe fn malloc(&mut self, capacity: usize) {
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
            alloc(Layout::array::<FatPointer>(capacity).unwrap_unchecked()) as *mut FatPointer
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
                Layout::array::<FatPointer>(old_capacity).unwrap_unchecked(),
                new_capacity * std::mem::size_of::<FatPointer>(),
            ) as *mut FatPointer
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
            self.soa.realloc(self.len, self.capacity);
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
struct SoaAllocation<SOA: Soa> {
    soa: SoaVec<SOA>,
    free_list: VecDeque<usize>,
}

impl<SOA: Soa> SoaAllocation<SOA> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn malloc(&mut self, value: SOA::UnderlyingType) -> usize {
        if let Some(index) = self.free_list.pop_front() {
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
        self.free_list.push_back(index);
    }
}

#[derive(Debug, Default)]
pub struct SoaAllocationHashTable {
    buckets: Vec<isize>,
    mask: u64,
    len: usize,
    /// resize when len >= threshold
    threshold: usize,
    buckets_filled: usize,
}
impl SoaAllocationHashTable {
    pub fn new() -> Self {
        Self {
            buckets: vec![-1, -1, -1, -1],
            mask: 3,
            len: 0,
            threshold: 0,
            buckets_filled: 0,
        }
    }
}

pub struct SoaNodeTable<T: Soa> where T::UnderlyingType: std::hash::Hash {
    hash_table: SoaAllocationHashTable,
    allocation: SoaAllocation<SoaHashTableValue<T>>,
}

impl<T: Soa> SoaNodeTable<T> where T::UnderlyingType: std::hash::Hash {
    pub fn new() -> Self {
        Self {
            hash_table: SoaAllocationHashTable::new(),
            allocation: SoaAllocation::new(),
        }
    }

    pub unsafe fn resize(&mut self, new_capacity: usize) {
        debug_assert!(new_capacity.is_power_of_two());
        let old_capacity = self.hash_table.buckets.len();
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
            entry = unsafe { self.allocation.soa.get(entry as usize).next };
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
            unsafe { alloc(Layout::array::<usize>(capacity).unwrap()) as *mut usize }
        }

        unsafe fn malloc(&mut self, capacity: usize) {
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
    }
    impl Soa for (*mut usize, *mut usize) {
        type UnderlyingType = (usize, usize);
        type ReferenceType = (usize, usize);

        fn with_capacity(capacity: usize) -> Self {
            unsafe {
                (
                    alloc(Layout::array::<usize>(capacity).unwrap()) as *mut usize,
                    alloc(Layout::array::<usize>(capacity).unwrap()) as *mut usize,
                )
            }
        }

        unsafe fn malloc(&mut self, capacity: usize) {
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
}
