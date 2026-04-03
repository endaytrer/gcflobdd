use parking_lot::RwLock;
use parking_lot::lock_api::RwLockUpgradableReadGuard;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

#[cfg(feature = "fx-hash")]
use rustc_hash::FxHasher as DefaultHasher;
#[cfg(not(feature = "fx-hash"))]
use std::hash::DefaultHasher;

pub struct HashCachedWithHasher<T: Hash, H: Hasher + Default> {
    value: T,
    cache: RwLock<Option<u64>>,
    hasher: PhantomData<H>,
}
pub type HashCached<T> = HashCachedWithHasher<T, DefaultHasher>;
pub type Arch<T> = Arc<HashCached<T>>;

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
impl<T: Hash, H: Hasher + Default> From<T> for HashCachedWithHasher<T, H> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Hash, H: Hasher + Default> HashCachedWithHasher<T, H> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            cache: RwLock::new(None),
            hasher: PhantomData,
        }
    }
    pub fn hash_code(&self) -> u64 {
        let hash_check = self.cache.upgradable_read();
        if let Some(hash) = *hash_check {
            return hash;
        }
        let mut hash_check = RwLockUpgradableReadGuard::upgrade(hash_check);
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
