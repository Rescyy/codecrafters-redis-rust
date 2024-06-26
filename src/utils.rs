use std::{ascii::escape_default, error::Error, str::FromStr};

use anyhow::anyhow;
use rand::seq::SliceRandom;

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

pub fn generate_master_replid() -> Vec<u8> {
    let mut master_replid: Vec<u8> = Vec::with_capacity(40);
    let rng = &mut rand::thread_rng();
    for _ in 0..40 {
        master_replid.push(*HEX_DIGITS.choose(rng).unwrap());
    }
    return master_replid;
}

pub fn is_valid_master_replid(master_replid: &[u8]) -> bool {
    for byte in master_replid.iter() {
        if *byte < b'0' || *byte > b'9' && *byte < b'a' || *byte > b'f' {
            return false
        }
    }
    true
}

pub fn show(bs: &[u8]) -> String {
    let mut visible = String::new();
    for &b in bs {
        let part: Vec<u8> = escape_default(b).collect();
        visible.push_str(std::str::from_utf8(&part).unwrap());
    }
    visible
}

pub fn parse_vec_u8<F>(vec_u8: Vec<u8>) -> Result<F, Box<dyn Error>>
where F: FromStr, <F as FromStr>::Err: std::error::Error {
    match String::from_utf8(vec_u8)?.parse::<F>() {
        Ok(res) => Ok(res),
        Err(_) => Err(Box::from(anyhow!("Couldn't parse"))),
    }
}