#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic, future_incompatible)]

//! Library code for key-value store implementation

use dashmap::DashMap;

/// Key-value store wrapper
#[derive(Default)]
pub struct KvStore {
    store: DashMap<String, String>,
}

/// Methods on key-value store
impl KvStore {
    /// Returns empty `DashMap` with default hasher
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert key-value pair into store
    pub fn set(&mut self, key: String, value: String) {
        let _ = self.store.insert(key, value);
    }

    /// Return value for given key from store if present
    #[must_use]
    pub fn get(&self, key: &str) -> Option<String> {
        self.store.get(key).map(|v| v.value().to_owned())
    }

    /// Remove key-value pair from store for given key
    pub fn remove(&mut self, key: &str) {
        let _ = self.store.remove(key);
    }
}
