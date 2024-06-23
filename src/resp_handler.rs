use std::{error::Error, io};
use anyhow::anyhow;
use async_recursion::async_recursion;
use format_bytes::format_bytes;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};

#[derive(Debug, Clone, PartialEq)]
pub enum RespDatatype {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(Vec<u8>),
    NullBulkString,
    Array(Vec<RespDatatype>),
    NullArray,
    RDBFile(Vec<u8>),
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

// RespStreamReader, takes in TcpStream
// Reads from the stream, stores the data in a buffer
// Extracts RespDatatypes, with the corresponding bytes, no longer owning them
// In case there is an error, the buffer is cleared until a valid RespDatatype is found
// This should be able to handle concurrent streams of commands
// This should be able to extract RespDatatypes and RDB files
#[derive(Debug)]
pub struct RespStreamHandler {
    pub stream: TcpStream,
    buf: Vec<u8>,
    index: usize,
}

impl RespStreamHandler {
    pub fn new(stream: TcpStream) -> Self {
        Self {stream, buf: Vec::new(), index: 0}
    }

    pub fn consume(self) -> TcpStream {
        self.stream
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), Box<dyn Error>> {
        Ok(self.stream.write_all(buf).await?)
    }

    pub async fn is_shutdown(&mut self) -> bool {
        if self.buf.len() > 0 {
            return false;
        }
        return match self.refill(0).await {
            Ok(bytes_read) => bytes_read == 0,
            Err(_) => true,
        }
    }

    pub async fn refill(&mut self, min_size: usize) -> Result<usize, Box<dyn Error>> {
        let mut bytes_filled = match self.stream.try_read_buf(&mut self.buf) {
            Ok(n) => n,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => 0,
            Err(e) => return Err(e.into()),
        };
        while min_size > self.buf.len() {
            dbg!(&self);
            bytes_filled += self.stream.read_buf(&mut self.buf).await?;
            dbg!(&self);
        }
        Ok(bytes_filled)
    }

    #[allow(unused)]
    async fn get_index(&mut self, index: usize) -> Result<u8, Box<dyn Error>> {
        self.refill(index+1).await?;
        return Ok(self.buf[index]);
    }

    async fn get_slice(&mut self, start: usize, end: usize) -> Result<&[u8], Box<dyn Error>> {
        self.refill(end).await?;
        return Ok(&self.buf[start..end])
    }

    async fn get_n(&mut self, amount: usize) -> Result<&[u8], Box<dyn Error>> {
        let index = self.index;
        self.index += amount;
        return Ok(self.get_slice(index, self.index).await?)
    }

    async fn get_n_until_crnl(&mut self, amount: usize) -> Result<&[u8], Box<dyn Error>> {
        if self.get_slice(self.index+amount, self.index+amount+2).await? == b"\r\n" {
            let index = self.index;
            self.index += amount+2;
            return Ok(self.get_slice(index, index+amount).await?)
        }
        return Err(Box::from(anyhow!("Bulk String did not end with \"\\r\\n\"")))
    }

    async fn get_until_crnl(&mut self) -> Result<&[u8], Box<dyn Error>> {
        let mut last_index = self.index;
        loop {
            self.refill(3).await?;
            for i in last_index..self.buf.len()-1 {
                if &self.buf[i..i+2] == b"\r\n" {
                    self.index = i+2;
                    return Ok(&self.buf[..i])
                }
            } 
            last_index = self.buf.len()-1;
        }
    }

    #[inline]
    fn get_drained(&mut self) -> Vec<u8> {
        let index = self.index;
        self.index = 0;
        self.buf.drain(..index).collect()
    }

    pub async fn get_rdb(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        let slice = self.get_until_crnl().await?;
        let size = match slice[0] {
            b'$' => String::from_utf8(slice[1..].to_vec())?.parse::<usize>()?,
            _ => return Err(Box::from(anyhow!("Invalid RDB file format"))),
        };
        let rdb = self.get_n(size).await?.to_vec();
        self.get_drained();
        Ok(rdb)
    }

    pub async fn deserialize_stream(&mut self) -> Result<(RespDatatype, Vec<u8>), Box<dyn Error>> {
        let resp_object = self.deserialize_stream_recursive().await?;
        let drained: Vec<u8> = self.get_drained();
        return Ok((resp_object, drained))
    }

    #[async_recursion]
    async fn deserialize_stream_recursive(&mut self) -> Result<RespDatatype, Box<dyn Error>> {

        let splice = self.get_until_crnl().await?;
        dbg!(splice);
        return match splice[0] {
            b'+' => Ok(RespDatatype::SimpleString(String::from_utf8(splice[1..].to_vec())?)),
            b'-' => Ok(RespDatatype::SimpleError(String::from_utf8(splice[1..].to_vec())?)),
            b':' => Ok(RespDatatype::Integer(String::from_utf8(splice[1..].to_vec())?.parse::<i64>()?)),
            b'$' => {
                let bulk_length = String::from_utf8(splice[1..].to_vec())?.parse::<isize>()?;
                if bulk_length < 0 {
                    return Ok(RespDatatype::NullBulkString);
                }
                let bulk_length: usize = bulk_length.try_into()?;
                let bulk_string: Vec<u8> = Vec::from(self.get_n_until_crnl(bulk_length).await?);
                Ok(RespDatatype::BulkString(bulk_string))
            },
            b'*' => {
                let array_length = String::from_utf8(splice[1..].to_vec())?.parse::<isize>()?;
                if array_length < 0 {
                    return Ok(RespDatatype::NullArray);
                }
                let array_length: usize = array_length.try_into()?;
                let mut array: Vec<RespDatatype> = Vec::with_capacity(array_length);
                for _ in 0..array_length {
                    array.push(self.deserialize_stream_recursive().await?)
                }
                Ok(RespDatatype::Array(array))
            },
            _ => Err(Box::from(anyhow!("Invalid First Byte"))),
        }
    }
}


// Takes in muttable buffer
// The buffer is drained according to the amount of bytes parsed and that take part in the RespDatatype
// Returns RespDatatype, Vector of its corresponding bytes and the rest of the bytes
// pub fn deserialize(buf: &mut Vec<u8>) -> Result<(RespDatatype, Vec<u8>), String> {
    
//     let mut splice_array: Vec<&[u8]> = Vec::new();
//     let mut splice_indeces: Vec<usize> = Vec::new();
//     let mut last_split = 0;
    
//     for i in 0..buf.len()-1 {
//         if &buf[i..i+2] == b"\r\n" {
//             splice_array.push(&buf[last_split..i]);
//             splice_indeces.push(i+2);
//             last_split = i+2;
//         }
//     }
    
//     return match deserialize_recursive(0, &splice_array) {
//         Some((resp_object, i)) =>  {
//             let temp_buf = buf.clone();
//             let collected = match splice_indeces.get(i-1) {
//                 Some(i) => if buf.len() >= *i {
//                     buf.clear();
//                     buf.put(&temp_buf[*i..]);
//                     temp_buf[..*i].to_vec()
//                 } else {
//                     buf.clear();
//                     temp_buf
//                 },
//                 None => {
//                     buf.clear();
//                     temp_buf
//                 },
//             };
            
//             Ok((resp_object, collected))
//         },
//         None => Err(format!("Couldn't deserialize the RESP bytes: {}", show(&buf[..])))
//     }
// }

// fn deserialize_recursive(mut i: usize, splice_array: &Vec<&[u8]>) -> Option<(RespDatatype, usize)> {
    
//     if i >= splice_array.len() {
//         return None;
//     }
//     let splice = splice_array[i];

//     match splice[0] {
//         b'+' => {
//             match String::from_utf8(splice[1..].to_vec()) {
//                 Ok(simple_string) => {
//                     return Some((RespDatatype::SimpleString(simple_string), i+1));
//                 },
//                 Err(e) => {
//                     println!("Error: {}", e);
//                 }
//             }
//         },
//         b'-' => {
//             match String::from_utf8(splice[1..].to_vec()) {
//                 Ok(simple_error) => {
//                     return Some((RespDatatype::SimpleError(simple_error), i+1));
//                 },
//                 Err(e) => {
//                     println!("Error: {}", e);
//                 }
//             }
//         },
//         b':' => {
//             match String::from_utf8(splice[1..].to_vec()) {
//                 Ok(integer_string) => {
//                     match integer_string.parse::<i64>() {
//                         Ok(integer) => {
//                             return Some((RespDatatype::Integer(integer), i+1));
//                         },
//                         Err(e) => {
//                             println!("Error: {}", e);
//                         }
//                     }
//                 },
//                 Err(e) => {
//                     println!("Error: {}", e);
//                 }
//             }
//         },
//         b'$' => {
//             let bulk_length: isize;
//             match String::from_utf8(splice[1..].to_vec()) {
//                 Ok(length_string) => {
//                     match length_string.parse() {
//                         Ok(length) => {
//                             bulk_length = length;
//                         },
//                         Err(e) => {
//                             println!("Error: {}", e);
//                             return None;
//                         }
//                     }
//                 },
//                 Err(e) => {
//                     println!("Error: {}", e);
//                     return None;
//                 }
//             };
//             if bulk_length < 0 {
//                 return Some((RespDatatype::NullBulkString, i+1));
//             }
//             i += 1;
//             let bulk_length: usize = bulk_length.try_into().unwrap();
//             let mut bulk_string: Vec<u8> = Vec::with_capacity(bulk_length);
//             loop {
//                 if i >= splice_array.len() {
//                     return None;
//                 }
//                 bulk_string.put_slice(splice_array[i]);
//                 if bulk_string.len() < bulk_length {
//                     bulk_string.put_slice(b"\r\n");
//                     i += 1;
//                 } else {
//                     break;
//                 }
//             }
//             if bulk_string.len() == bulk_length {
//                 return Some((RespDatatype::BulkString(bulk_string), i+1));
//             }
//         },
//         b'*' => {
//             let array_length: isize;
//             match String::from_utf8(splice[1..].to_vec()) {
//                 Ok(length_string) => {
//                     match length_string.parse() {
//                         Ok(length) => {
//                             array_length = length;
//                         },
//                         Err(e) => {
//                             println!("Error: {}", e);
//                             return None;
//                         }
//                     }
//                 },
//                 Err(e) => {
//                     println!("Error: {}", e);
//                     return None;
//                 }
//             };
//             if array_length < 0 {
//                 return Some((RespDatatype::NullArray, i+1));
//             }
//             i += 1;
//             let array_length: usize = array_length.try_into().unwrap();
//             let mut array: Vec<RespDatatype> = Vec::with_capacity(array_length);
//             for _ in 0..array_length {
//                 match deserialize_recursive(i, splice_array) {
//                     Some((resp_object, next_i)) => {
//                         array.push(resp_object);
//                         i = next_i;
//                     },
//                     None => return None,
//                 }
//             }
//             return Some((RespDatatype::Array(array), i));
//         },
//         _ => ()
//     }
    
//     return None;
// }

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
        },
        RespDatatype::RDBFile(contents) => {
            format_bytes!(b"${}\r\n{}",
                contents.len().to_string().as_bytes(),
                &contents[..]
            )
        },
    };
    bytes.append(&mut serialized);
}