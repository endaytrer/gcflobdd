use std::borrow::Borrow;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::Deref;
use std::rc::{Rc, Weak};

#[cfg(feature = "fx-hash")]
use rustc_hash::FxHasher as DefaultHasher;
#[cfg(not(feature = "fx-hash"))]
use std::hash::DefaultHasher;

#[derive(Clone)]
pub struct HashCachedWithHasher<T: Hash, H: Hasher + Default> {
    value: T,
    hash: u64,
    hasher: PhantomData<H>,
}
pub type HashCached<T> = HashCachedWithHasher<T, DefaultHasher>;
pub type Rch<T> = Rc<HashCached<T>>;
pub type Weakh<T> = Weak<HashCached<T>>;

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
        let mut hasher = H::default();
        value.hash(&mut hasher);
        let hash = hasher.finish();
        Self {
            value,
            hash,
            hasher: PhantomData,
        }
    }
    pub fn with_hash(value: T, hash: u64) -> Self {
        Self {
            value,
            hash,
            hasher: PhantomData,
        }
    }
    #[inline]
    pub fn hash_code(&self) -> u64 {
        self.hash
    }
}
impl<T: Hash> Hash for HashCached<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash_code().hash(state);
    }
}

impl<T: Hash + PartialEq, H: Hasher + Default> PartialEq for HashCachedWithHasher<T, H> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}
impl<T: Hash + Eq, H: Hasher + Default> Eq for HashCachedWithHasher<T, H> {}

#[derive(Debug)]
/// A struct with guarantee that a == b if and only if &a == &b
pub struct WeakKey<T: Hash>(Weakh<T>);

impl<T: Hash> Clone for WeakKey<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Hash> PartialEq for WeakKey<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        Weak::as_ptr(&self.0) == Weak::as_ptr(&other.0)
    }
}
impl<T: Hash> Eq for WeakKey<T> {}

impl<T: Hash> Hash for WeakKey<T> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        Weak::as_ptr(&self.0).hash(state);
    }
}

impl<T: Hash> From<&Rch<T>> for WeakKey<T> {
    #[inline]
    fn from(value: &Rch<T>) -> Self {
        Self(Rc::downgrade(value))
    }
}
impl<T: Hash> WeakKey<T> {
    pub fn is_valid(&self) -> bool {
        Weak::upgrade(&self.0).is_some()
    }
}
