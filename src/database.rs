use std::collections::HashMap;
use tokio::sync::Mutex;

lazy_static! {
    static ref DATABASE: Mutex<HashMap<Vec<u8>, Vec<u8>>> = Mutex::new(HashMap::new());
}

pub async fn get_value(key: &[u8]) -> Option<Vec<u8>> {
    let database = DATABASE.lock().await;
    match database.get(key) {
        Some(value) => {
            return Some(value.to_owned())
        },
        None => return None,
    }
}

pub async fn set_value(key: &[u8], value: &[u8]) {
    let mut database = DATABASE.lock().await;
    database.insert(key.to_owned(), value.to_owned());
}