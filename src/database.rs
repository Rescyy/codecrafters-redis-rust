use std::{collections::HashMap, time::Duration};
use tokio::{sync::Mutex, time::sleep};

lazy_static! {
    static ref DATABASE: Mutex<HashMap<Vec<u8>, Vec<u8>>> = Mutex::new(HashMap::new());
    pub static ref CONFIG: Mutex<HashMap<Vec<u8>, Vec<u8>>> = Mutex::new(HashMap::new());
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
    let key = key.to_owned();
    database.insert(key.clone(), value.to_owned());
}

pub async fn get_config(key: &[u8]) -> Option<Vec<u8>> {
    let config = CONFIG.lock().await;
    match config.get(key) {
        Some(value) => {
            return Some(value.to_owned())
        },
        None => return None,
    }
}

#[allow(dead_code)]
pub async fn set_config(key: &[u8], value: &[u8]) {
    let mut config = CONFIG.lock().await;
    let key = key.to_owned();
    config.insert(key.clone(), value.to_owned());
}

pub async fn set_value_expiry(key: &[u8], value: &[u8], expiry: u64) {
    let mut database = DATABASE.lock().await;
    let key = key.to_owned();
    database.insert(key.clone(), value.to_owned());
    tokio::spawn(async move{
        sleep(Duration::from_millis(expiry)).await;
        delete_value(&key).await;
    });
}

pub async fn delete_value(key: &[u8]) {
    let mut database = DATABASE.lock().await;
    database.remove(key);
}