use std::borrow::Borrow;
use std::cell::RefCell;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::Deref;
use std::rc::Rc;

#[cfg(feature = "fx-hash")]
use rustc_hash::FxHasher as DefaultHasher;
#[cfg(not(feature = "fx-hash"))]
use std::hash::DefaultHasher;

#[derive(Clone)]
pub struct HashCachedWithHasher<T: Hash, H: Hasher + Default> {
    value: T,
    cache: RefCell<Option<u64>>,
    hasher: PhantomData<H>,
}
pub type HashCached<T> = HashCachedWithHasher<T, DefaultHasher>;
pub type Rch<T> = Rc<HashCached<T>>;

impl<T: Hash + Debug, H: Hasher + Default> Debug for HashCachedWithHasher<T, H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("HashCached").field(&self.value).finish()
    }
}

impl<T: Hash, H: Hasher + Default> AsRef<T> for HashCachedWithHasher<T, H> {
    fn as_ref(&self) -> &T {
        &self.value
    }
}
impl<T: Hash, H: Hasher + Default> Borrow<T> for HashCachedWithHasher<T, H> {
    fn borrow(&self) -> &T {
        &self.value
    }
}
impl<T: Hash, H: Hasher + Default> Deref for HashCachedWithHasher<T, H> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
impl<T: Hash, H: Hasher + Default> From<T> for HashCachedWithHasher<T, H> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}
impl<T: Hash, H: Hasher + Default> HashCachedWithHasher<T, H> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            cache: RefCell::new(None),
            hasher: PhantomData,
        }
    }
    pub fn with_hash(value: T, hash: u64) -> Self {
        Self {
            value,
            cache: RefCell::new(Some(hash)),
            hasher: PhantomData,
        }
    }
    pub fn hash_code(&self) -> u64 {
        let hash_check = self.cache.borrow();
        if let Some(hash) = *hash_check {
            return hash;
        }
        std::mem::drop(hash_check);
        let mut hash_check = self.cache.borrow_mut();
        let mut hasher = H::default();
        self.value.hash(&mut hasher);
        let value = hasher.finish();
        *hash_check = Some(value);
        value
    }
}
impl<T: Hash> Hash for HashCached<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash_code().hash(state);
    }
}
