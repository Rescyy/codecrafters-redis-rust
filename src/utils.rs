use rand::seq::SliceRandom;

const HEX_DIGITS2: &[u8; 16] = b"0123456789abcdef";

pub fn generate_master_replid() -> Vec<u8> {
    let mut master_replid: Vec<u8> = Vec::with_capacity(40);
    let rng = &mut rand::thread_rng();
    for _ in 0..40 {
        master_replid.push(*HEX_DIGITS2.choose(rng).unwrap());
    }
    return master_replid;
}