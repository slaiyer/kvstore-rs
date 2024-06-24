use dashmap::DashMap;

#[derive(Default)]
pub struct KvStore {
    store: DashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn set(&mut self, key: String, value: String) {
        let _ = self.store.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.store.get(&key).map(|v| v.value().clone())
    }

    pub fn remove(&mut self, key: String) {
        let _ = self.store.remove(&key);
    }
}
