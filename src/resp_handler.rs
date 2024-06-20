use bytes::BufMut;
use format_bytes::format_bytes;

#[derive(Debug, Clone)]
pub enum RespDatatype {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(Vec<u8>),
    NullBulkString,
    Array(Vec<RespDatatype>),
    NullArray,
    // Null,
    // Boolean,
    // Double,
    // BigNumber,
    // BulkError,
    // VerbatimString,
    // Map,
    // Set,
    // Push
}

// In the future may change return type to Result, it's easier to implement Option
pub fn deserialize(buf: Vec<u8>) -> Option<RespDatatype> {
    
    let mut splice_array: Vec<&[u8]> = Vec::new();
    
    {
        let mut last_split = 0;
        
        for i in 0..buf.len()-1 {
            if &buf[i..i+2] == b"\r\n" {
                splice_array.push(&buf[last_split..i]);
                last_split = i+2;
            }
        }
    }
    
    return match deserialize_recursive(0, &splice_array) {
        Some((resp_object, i)) => if i == splice_array.len() {Some(resp_object)} else {None},
        None => None
    }
}

fn deserialize_recursive(mut i: usize, splice_array: &Vec<&[u8]>) -> Option<(RespDatatype, usize)> {
    
    if i >= splice_array.len() {
        return None;
    }
    let splice = splice_array[i];
    
    match splice[0] {
        b'+' => {
            match String::from_utf8(splice[1..].to_vec()) {
                Ok(simple_string) => {
                    return Some((RespDatatype::SimpleString(simple_string), i+1));
                },
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
        },
        b'-' => {
            match String::from_utf8(splice[1..].to_vec()) {
                Ok(simple_error) => {
                    return Some((RespDatatype::SimpleError(simple_error), i+1));
                },
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
        },
        b':' => {
            match String::from_utf8(splice[1..].to_vec()) {
                Ok(integer_string) => {
                    match integer_string.parse::<i64>() {
                        Ok(integer) => {
                            return Some((RespDatatype::Integer(integer), i+1));
                        },
                        Err(e) => {
                            println!("Error: {}", e);
                        }
                    }
                },
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
        },
        b'$' => {
            let bulk_length: isize;
            match String::from_utf8(splice[1..].to_vec()) {
                Ok(length_string) => {
                    match length_string.parse() {
                        Ok(length) => {
                            bulk_length = length;
                        },
                        Err(e) => {
                            println!("Error: {}", e);
                            return None;
                        }
                    }
                },
                Err(e) => {
                    println!("Error: {}", e);
                    return None;
                }
            };
            if bulk_length < 0 {
                return Some((RespDatatype::NullBulkString, i+1));
            }
            i += 1;
            let bulk_length: usize = bulk_length.try_into().unwrap();
            let mut bulk_string: Vec<u8> = Vec::with_capacity(bulk_length);
            loop {
                if i >= splice_array.len() {
                    return None;
                }
                bulk_string.put_slice(splice_array[i]);
                if bulk_string.len() < bulk_length {
                    bulk_string.put_slice(b"\r\n");
                    i += 1;
                } else {
                    break;
                }
            }
            if bulk_string.len() == bulk_length {
                return Some((RespDatatype::BulkString(bulk_string), i+1));
            }
        },
        b'*' => {
            let array_length: isize;
            match String::from_utf8(splice[1..].to_vec()) {
                Ok(length_string) => {
                    match length_string.parse() {
                        Ok(length) => {
                            array_length = length;
                        },
                        Err(e) => {
                            println!("Error: {}", e);
                            return None;
                        }
                    }
                },
                Err(e) => {
                    println!("Error: {}", e);
                    return None;
                }
            };
            if array_length < 0 {
                return Some((RespDatatype::NullArray, i+1));
            }
            i += 1;
            let array_length: usize = array_length.try_into().unwrap();
            let mut array: Vec<RespDatatype> = Vec::with_capacity(array_length);
            for _ in 0..array_length {
                match deserialize_recursive(i, splice_array) {
                    Some((resp_object, next_i)) => {
                        array.push(resp_object);
                        i = next_i;
                    },
                    None => return None,
                }
            }
            return Some((RespDatatype::Array(array), i));
        },
        _ => ()
    }
    
    return None;
}

pub fn serialize(resp_object: &RespDatatype) -> Vec<u8> {
    let mut bytes: Vec<u8> = Vec::new();
    serialize_recursive(&mut bytes, resp_object);
    return bytes;
}

fn serialize_recursive(bytes: &mut Vec<u8>, resp_object: &RespDatatype) {
    let mut serialized = match resp_object {
        RespDatatype::SimpleString(string) => {
            format_bytes!(b"+{}\r\n", string.as_bytes())
        },
        RespDatatype::SimpleError(error_string) => {
            format_bytes!(b"-{}\r\n", error_string.as_bytes())
        },
        RespDatatype::Integer(integer) => {
            format_bytes!(b":{}\r\n", integer.to_string().as_bytes())
        },
        RespDatatype::BulkString(bulk_string) => {
            format_bytes!(b"${}\r\n{}\r\n", 
                bulk_string.len().to_string().as_bytes(), 
                &bulk_string[..]
            )
        },
        RespDatatype::NullBulkString => {
            b"$-1\r\n".to_vec()
        },
        RespDatatype::Array(array) => {
            let array_len = array.len();
            let mut array_bytes = format_bytes!(b"*{}\r\n",
                array_len.to_string().as_bytes()
            );
            for resp_object in array.into_iter() {
                serialize_recursive(&mut array_bytes, resp_object);
            }
            array_bytes
        },
        RespDatatype::NullArray => {
            b"*-1\r\n".to_vec()
        }
    };
    bytes.append(&mut serialized);
}