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
    type ReferenceType: Copy + PartialEq<Self::UnderlyingType>;
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

#[derive(Debug)]
struct HashTableLinkedList<SOA: Soa> {
    hash: u64,
    inner: SOA::UnderlyingType,
}

#[derive(Debug, Default)]
struct SoaHashTableValue<SOA: Soa> {
    hash: *mut u64,
    inner: SOA,
    next: *mut isize,
}

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

impl<SOA: Soa> PartialEq<HashTableLinkedList<SOA>> for HashedReferenceType<SOA> {
    fn eq(&self, other: &HashTableLinkedList<SOA>) -> bool {
        self.hash == other.hash && self.inner == other.inner
    }
}

impl<SOA: Soa> Soa for SoaHashTableValue<SOA> {
    type UnderlyingType = HashTableLinkedList<SOA>;
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
        debug_assert_eq!(self.next, null_mut());
        unsafe { self.inner.realloc(old_capacity, new_capacity) };
        self.next = unsafe {
            realloc(
                self.next as *mut u8,
                Layout::array::<isize>(old_capacity).unwrap_unchecked(),
                new_capacity,
            ) as *mut isize
        };
        self.hash = unsafe {
            realloc(
                self.hash as *mut u8,
                Layout::array::<u64>(old_capacity).unwrap_unchecked(),
                new_capacity,
            ) as *mut u64
        };
    }
    unsafe fn set(&mut self, index: usize, node: Self::UnderlyingType) {
        unsafe { self.inner.set(index, node.inner) };
        // don't write next
        unsafe { self.hash.add(index).write(node.hash) };
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
                new_capacity,
            ) as *mut usize
        };
        self.num_exits = unsafe {
            realloc(
                self.num_exits as *mut u8,
                Layout::array::<usize>(old_capacity).unwrap_unchecked(),
                new_capacity,
            ) as *mut usize
        };
        self.grammar = unsafe {
            realloc(
                self.grammar as *mut u8,
                Layout::array::<&'grammar Rc<GrammarNode>>(old_capacity).unwrap_unchecked(),
                new_capacity,
            ) as *mut &'grammar Rc<GrammarNode>
        };
        self.node_type = unsafe {
            realloc(
                self.node_type as *mut u8,
                Layout::array::<SoaNodeType>(old_capacity).unwrap_unchecked(),
                new_capacity,
            ) as *mut SoaNodeType
        };
        self.union_pointer_0 = unsafe {
            realloc(
                self.union_pointer_0 as *mut u8,
                Layout::array::<FatPointer>(old_capacity).unwrap_unchecked(),
                new_capacity,
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
    buckets: Box<[isize]>,
    mask: u64,
    len: usize,
    // /// resize when len >= threshold
    // threshold: usize,
}
impl SoaAllocationHashTable {
    pub fn new() -> Self {
        Self {
            buckets: Box::new([-1]),
            mask: 0,
            len: 0,
        }
    }
}

pub struct SoaNodeTable<T: Soa> {
    hash_table: SoaAllocationHashTable,
    allocation: SoaAllocation<SoaHashTableValue<T>>,
}

impl<T: Soa> SoaNodeTable<T> {
    pub fn new() -> Self {
        Self {
            hash_table: SoaAllocationHashTable::new(),
            allocation: SoaAllocation::new(),
        }
    }
}
impl<T: Soa> SoaNodeTable<T>
where
    T::ReferenceType: Eq,
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
{
    pub fn set(&mut self, hash: u64, value: T::UnderlyingType) -> usize {
        let mut entry = self.hash_table.buckets[(hash & self.hash_table.mask) as usize];
        let mut last_next = None;
        if entry == -1 {
            return entry as usize;
        }
        while entry != -1 {
            last_next = Some(unsafe {
                (self.allocation.soa.soa.next.add(entry as usize) as *mut isize)
                    .as_mut()
                    .unwrap_unchecked()
            });
            let candidate = unsafe { self.allocation.soa.get(entry as usize) };
            if candidate.hash == hash && candidate.inner == value {
                return entry as usize;
            }
            entry = unsafe { self.allocation.soa.get(entry as usize).next };
        }
        let slot = self
            .allocation
            .malloc(HashTableLinkedList { hash, inner: value });
        *last_next.unwrap() = slot as isize;
        slot as usize
    }
}
