use std::str::Split;
use std::vec::IntoIter;
use format_bytes::format_bytes;

use crate::resp_handler::RespDatatype;
use crate::{database::*, is_valid_master_replid, push_to_replicas, ReplicaTask};

#[allow(dead_code)]
#[derive(Debug)]
pub enum RedisCommand {
    Pong,
    Ok,
    Error(String),
    SimpleString(Vec<u8>),
    BulkString(Vec<u8>),
    FullResync(Vec<u8>, Vec<u8>),
    ReplconfOk1,
    ReplconfOk2,
    ReplconfAck(Vec<u8>),
    RespDatatype(RespDatatype),
    NullBulkString,
}

pub async fn interpret(resp_object: RespDatatype, buf: &Vec<u8>) -> Option<RedisCommand> {
    match resp_object {
        RespDatatype::Array(array) => {
            let mut array_iterator = array.into_iter();
            let command = match array_iterator.next() {
                Some(RespDatatype::BulkString(bulk_string)) => bulk_string.to_ascii_uppercase(),
                _ => return None,
            };
            match &command[..] {
                b"PING" => Some(RedisCommand::Pong),
                b"ECHO" => 
                    match array_iterator.next() {
                        Some(RespDatatype::BulkString(message)) => 
                        Some(RedisCommand::BulkString(message)),
                        _ => Some(RedisCommand::NullBulkString),
                    },
                b"SET" => {
                    let command = interpret_set(array_iterator).await;
                    if let Some(_) = command {
                        push_to_replicas(ReplicaTask::new(buf.clone())).await;
                    }
                    command
                },
                b"GET" => interpret_get(array_iterator).await,
                b"INFO" => interpret_info(array_iterator).await,
                b"REPLCONF" => interpret_replconf(array_iterator).await,
                b"PSYNC" => interpret_psync(array_iterator).await,
                // b"FULLRESYNC" => interpret_fullresync(array_iterator).await,
                _ => return make_error_command(format!("Unknown command received {:?}", command)),
            }
        },
        RespDatatype::SimpleString(string) => {
            let mut split: Split<&str> = string.trim().split(" ");
            match split.next() {
                Some("FULLRESYNC") => interpret_fullresync(split).await,
                Some(command) => make_error_command(format!("Unknown command received: {:?}", command)),
                _ => make_error_command(format!("Unknown command received."),)
            }
        },
        _ => return None,
    }
}

async fn interpret_get(mut array_iterator: IntoIter<RespDatatype>) -> Option<RedisCommand> {
    let key = match array_iterator.next() {
        Some(RespDatatype::BulkString(key)) => key,
        _ => return make_error_command("Expected key after GET"),
    };
    match get_value(&key).await {
        Some(value) => Some(RedisCommand::BulkString(value)),
        None => Some(RedisCommand::NullBulkString),
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

async fn interpret_info(mut array_iterator: IntoIter<RespDatatype>) -> Option<RedisCommand> {
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
}

#[allow(unused)]
async fn interpret_replconf(mut array_iterator: IntoIter<RespDatatype>) -> Option<RedisCommand> {
    while let Some(argument) = array_iterator.next() {
        match argument {
            RespDatatype::BulkString(argument) => {
                match &argument.to_ascii_lowercase()[..] {
                    b"listening-port" => {
                        let port = match array_iterator.next() {
                            Some(RespDatatype::BulkString(port)) => {
                                let port = match String::from_utf8(port) {
                                    Ok(port) => port,
                                    Err(_) => return make_error_command("Invalid argument for port"),
                                };
                                match port.parse::<u16>() {
                                    Ok(port) => if port == 0 {return make_error_command("Port cannot be 0")},
                                    Err(_) => return make_error_command(format!("Port cannot be {port}. Choose 1-65535")),
                                }
                                port
                            },
                            _ => return make_error_command("Invalid argument for port"),
                        };
                        return Some(RedisCommand::ReplconfOk1);
                    },
                    b"capa" => {
                        let capa = match array_iterator.next() {
                            _ => (),
                        };
                        return Some(RedisCommand::ReplconfOk2);
                    },
                    b"getack" => {
                        let getack_arg = match array_iterator.next() {
                            Some(RespDatatype::BulkString(getack_arg)) => getack_arg,
                            _ => return make_error_command("Invalid argument for GETACK"),
                        };
                        if &getack_arg[..] == b"*" {
                            return Some(RedisCommand::ReplconfAck(vec![b'0']))
                        }
                        return make_error_command("Invalid argument for GETACK")
                    }
                    _ => (),
                }
            }
            _ => (),
        }
    }
    todo!()
}

async fn interpret_psync(mut array_iterator: IntoIter<RespDatatype>) -> Option<RedisCommand> {
    let repl_id = match array_iterator.next() {
        Some(RespDatatype::BulkString(repl_id)) => repl_id,
        _ => return make_error_command("No repl_id argument for PSYNC command given."),
    };
    let repl_offset = match array_iterator.next() {
        Some(RespDatatype::BulkString(repl_offset)) => repl_offset,
        _ => return make_error_command("No repl_offset argument for PSYNC command given.")
    };
    if &repl_id[..] == b"?" && &repl_offset[..] == b"-1" {
        let repl_id = get_value(b"master_replid").await.unwrap();
        let repl_offset = get_value(b"master_repl_offset").await.unwrap();
        let empty_file_payload = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
        .unwrap();
        return Some(RedisCommand::FullResync(format_bytes!(b"FULLRESYNC {} {}", repl_id, repl_offset), empty_file_payload));
    }
    todo!();
}

async fn interpret_fullresync<'a>(mut array_iterator: Split<'_, &str>) -> Option<RedisCommand> {
    let master_replid = match array_iterator.next() {
        Some(master_replid) => {
            if is_valid_master_replid(master_replid.as_bytes()) {
                master_replid.as_bytes().to_vec()
            } else {
                return make_error_command("Invalid master replid")
            }
        },
        _ => return make_error_command("Invalid master replid"),
    };
    let master_repl_offset = match array_iterator.next() {
        Some(master_repl_offset) => {
            match master_repl_offset.parse::<u64>() {
                Ok(master_repl_offset) => master_repl_offset,
                Err(_) => return make_error_command("Invalid master repl offset"),
            };
            master_repl_offset.as_bytes().to_vec()
        },
        _ => return make_error_command("Invalid master repl offset"),
    };
    return Some(RedisCommand::FullResync(master_replid, master_repl_offset))
}
#[inline]
fn make_error_command<T: ToString>(string: T) -> Option<RedisCommand> {
    return Some(RedisCommand::Error(string.to_string()));
}