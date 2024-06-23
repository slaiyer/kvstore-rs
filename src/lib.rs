#[derive(Default)]
pub struct KvStore {}

impl KvStore {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn set(&mut self, key: String, value: String) {
        todo!()
    }

    pub fn get(&self, key: String) -> Option<String> {
        todo!()
    }

    pub fn remove(&mut self, key: String) {
        todo!()
    }
}
