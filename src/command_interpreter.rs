use std::vec::IntoIter;
use crate::resp_handler::RespDatatype;
use crate::database::*;

#[derive(Debug)]
pub enum RedisCommand {
    Ping,
    Ok,
    Error(String),
    BulkString(Option<Vec<u8>>),
}

const NULL_BULK_STRING_COMMAND: RedisCommand = RedisCommand::BulkString(None);

pub async fn interpret(resp_object: RespDatatype) -> Option<RedisCommand> {
    match resp_object {
        RespDatatype::Array(Some(array)) => {
            let mut array_iterator = array.into_iter();
            let command = match array_iterator.next() {
                Some(RespDatatype::BulkString(Some(bulk_string))) => bulk_string.to_ascii_uppercase(),
                _ => return None,
            };
            match &command[..] {
                b"PING" => {
                    return Some(RedisCommand::Ping);
                },
                b"ECHO" => {
                    match array_iterator.next() {
                        Some(RespDatatype::BulkString(Some(message))) => 
                        return Some(RedisCommand::BulkString(Some(message.to_owned()))),
                        _ => return None,
                    };
                },
                b"SET" => interpret_set(array_iterator).await,
                b"GET" => {
                    let key = match array_iterator.next() {
                        Some(RespDatatype::BulkString(Some(key))) => key,
                        _ => return None,
                    };
                    match get_value(&key).await {
                        Some(value) => Some(RedisCommand::BulkString(Some(value))),
                        None => Some(NULL_BULK_STRING_COMMAND),
                    }
                },
                _ => return None,
            }
        },
        _ => return None,
    }
}

async fn interpret_set(mut array_iterator: IntoIter<RespDatatype>) -> Option<RedisCommand> {
    let key = match array_iterator.next() {
        Some(RespDatatype::BulkString(Some(key))) => key,
        _ => return None,
    };
    let value = match array_iterator.next() {
        Some(RespDatatype::BulkString(Some(value))) => value,
        _ => return None,
    };
    let mut expiry: Option<u64> = None;
    while let Some(argument) = array_iterator.next() {
        match argument {
            RespDatatype::BulkString(Some(argument)) => {
                match &argument.to_ascii_uppercase()[..] {
                    b"PX" => {
                        expiry = match array_iterator.next() {
                            Some(RespDatatype::Integer(integer)) => {
                                if integer >= 0 {
                                    Some(integer.try_into().unwrap())
                                } else {
                                    return Some(RedisCommand::Error(String::from("Integer argument for PX cannot be negative")))
                                }
                            },
                            Some(RespDatatype::BulkString(Some(bulk_string))) => {
                                let bulk_string = match String::from_utf8(bulk_string) {
                                    Ok(bulk_string) => bulk_string,
                                    Err(_) => return Some(RedisCommand::Error(String::from("Invalid argument given for PX")))
                                };
                                match bulk_string.parse() {
                                    Ok(res) => Some(res),
                                    Err(_) => return Some(RedisCommand::Error(String::from("Invalid argument given for PX")))
                                }
                            }
                            None => return Some(RedisCommand::Error(String::from("No integer argument given for PX"))),
                            _ => return Some(RedisCommand::Error(String::from("Invalid argument given for PX"))),
                        }
                    },
                    _ => (),
                }
            },
            _ => (),
        }
    }
    set_value(&key, &value, expiry).await;
    return Some(RedisCommand::Ok);
}