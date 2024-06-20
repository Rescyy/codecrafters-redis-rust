use std::vec::IntoIter;
use format_bytes::format_bytes;

use crate::resp_handler::RespDatatype;
use crate::database::*;

#[derive(Debug)]
pub enum RedisCommand {
    Ping,
    Ok,
    Error(String),
    BulkString(Vec<u8>),
    NullBulkString,
}

pub async fn interpret(resp_object: RespDatatype) -> Option<RedisCommand> {
    match resp_object {
        RespDatatype::Array(array) => {
            let mut array_iterator = array.into_iter();
            let command = match array_iterator.next() {
                Some(RespDatatype::BulkString(bulk_string)) => bulk_string.to_ascii_uppercase(),
                _ => return None,
            };
            match &command[..] {
                b"PING" => {
                    return Some(RedisCommand::Ping);
                },
                b"ECHO" => {
                    match array_iterator.next() {
                        Some(RespDatatype::BulkString(message)) => 
                        Some(RedisCommand::BulkString(message.to_owned())),
                        _ => Some(RedisCommand::NullBulkString),
                    }
                },
                b"SET" => interpret_set(array_iterator).await,
                b"GET" => {
                    let key = match array_iterator.next() {
                        Some(RespDatatype::BulkString(key)) => key,
                        _ => return make_error_command("Expected key after GET"),
                    };
                    match get_value(&key).await {
                        Some(value) => Some(RedisCommand::BulkString(value)),
                        None => Some(RedisCommand::NullBulkString),
                    }
                },
                b"INFO" => {
                    let arg = match array_iterator.next() {
                        Some(RespDatatype::BulkString(arg)) => arg,
                        _ => return None,
                    };
                    match &arg[..] {
                        b"replication" => {
                            let role = match get_value(b"role").await {
                                Some(role) => role,
                                None => return make_error_command("Error happened in the Redis. For some reason this server does not have a role.")
                            };
                            let master_replid = match get_value(b"master_replid").await {
                                Some(master_replid) => master_replid,
                                None => return make_error_command("Error happened in the Redis. For some reason this server does not have a role.")
                            };
                            let master_repl_offset = match get_value(b"master_repl_offset").await {
                                Some(master_repl_offset) => master_repl_offset,
                                None => return make_error_command("Error happened in the Redis. For some reason this server does not have a role.")
                            };
                            Some(RedisCommand::BulkString(
                                format_bytes!(b"role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                                role, master_replid, master_repl_offset
                            )))
                        },
                        arg => make_error_command(format!("Unknown argument for INFO {arg:?}")),
                    }
                },
                _ => return make_error_command(format!("Unknown command received {:?}", command)),
            }
        },
        _ => return None,
    }
}

async fn interpret_set(mut array_iterator: IntoIter<RespDatatype>) -> Option<RedisCommand> {
    let key = match array_iterator.next() {
        Some(RespDatatype::BulkString(key)) => key,
        _ => return None,
    };
    let value = match array_iterator.next() {
        Some(RespDatatype::BulkString(value)) => value,
        _ => return None,
    };
    let mut expiry: Option<u64> = None;
    while let Some(argument) = array_iterator.next() {
        match argument {
            RespDatatype::BulkString(argument) => {
                match &argument.to_ascii_uppercase()[..] {
                    b"PX" => {
                        expiry = match array_iterator.next() {
                            Some(RespDatatype::Integer(integer)) => {
                                if integer >= 0 {
                                    Some(integer.try_into().unwrap())
                                } else {
                                    return make_error_command("Integer argument for PX cannott be negative");
                                }
                            },
                            Some(RespDatatype::BulkString(bulk_string)) => {
                                let bulk_string = match String::from_utf8(bulk_string) {
                                    Ok(bulk_string) => bulk_string,
                                    Err(_) => return make_error_command("Invalid argument given for PX")
                                };
                                match bulk_string.parse() {
                                    Ok(res) => Some(res),
                                    Err(_) => return make_error_command("Invalid argument given for PX")
                                }
                            }
                            None => return make_error_command("No integer argument given for PX"),
                            _ => return make_error_command("Invalid argument given for PX"),
                        }
                    },
                    _ => (),
                }
            },
            _ => (),
        }
    }
    if let Some(expiry) = expiry {set_value_expiry(&key, &value, expiry).await} else {set_value(&key, &value).await}; 
    return Some(RedisCommand::Ok);
}

#[inline]
fn make_error_command<T: ToString>(string: T) -> Option<RedisCommand> {
    return Some(RedisCommand::Error(string.to_string()));
}