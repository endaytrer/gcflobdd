use std::{
    cell::RefCell,
    hash::{Hash, Hasher},
};

#[derive(Clone)]
pub(super) struct ReturnMapT<T> {
    pub(super) map_array: Vec<T>,
    hash_cache: RefCell<Option<u64>>,
}
impl<T: std::fmt::Debug> std::fmt::Debug for ReturnMapT<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ReturnMapT").field(&self.map_array).finish()
    }
}

impl<T: Copy> ReturnMapT<T> {
    pub fn lookup(&self, index: usize) -> T {
        self.map_array[index]
    }
}
impl<T: Eq> ReturnMapT<T> {
    pub fn inverse_lookup(&self, value: &T) -> Option<usize> {
        self.map_array.iter().position(|x| *x == *value)
    }
}

impl<T: Hash> Hash for ReturnMapT<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let hash_check = self.hash_cache.borrow();
        if let Some(hash) = *hash_check {
            hash.hash(state);
            return;
        }
        std::mem::drop(hash_check);
        let mut hash_check = self.hash_cache.borrow_mut();
        let mut hasher = std::hash::DefaultHasher::new();
        self.map_array.hash(&mut hasher);
        let value = hasher.finish();
        *hash_check = Some(value);
        value.hash(state);
    }
}

impl<T> ReturnMapT<T> {
    pub fn new(map_array: Vec<T>) -> Self {
        Self {
            map_array,
            hash_cache: RefCell::new(None),
        }
    }
}
pub(crate) type ReturnMap = ReturnMapT<i32>;

impl ReturnMap {
    pub fn new_sequential(n: i32) -> Self {
        Self {
            map_array: (0..n).collect(),
            hash_cache: RefCell::new(None),
        }
    }
    pub fn set(&mut self, index: usize, value: i32) {
        self.map_array[index] = value;
        self.hash_cache.borrow_mut().take();
    }
    pub fn add_to_end(&mut self, value: i32) {
        self.map_array.push(value);
        self.hash_cache.borrow_mut().take();
    }
    pub fn complement(&self) -> Self {
        Self {
            map_array: self
                .map_array
                .iter()
                .map(|x| if *x == 0 { 1 } else { 0 })
                .collect(),
            hash_cache: RefCell::new(None),
        }
    }
}
