use anyhow::anyhow;
use tokio::net::TcpStream;
use std::str::Split;
use std::vec::IntoIter;
use format_bytes::format_bytes;

use crate::{get_value, is_valid_master_replid, serialize, set_value, set_value_expiry, show, RedisCommand, RespDatatype, RespStreamHandler, OK_STRING, PONG_STRING};

lazy_static! {  
    static ref PING_COMMAND: Vec<u8> = serialize(&RespDatatype::Array(vec![RespDatatype::BulkString(b"PING".to_vec())]));
}

pub async fn send_handshake(master_host: &String, master_port: &String, slave_port: &String) -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect(format!("{master_host}:{master_port}")).await?;
    let mut resp_stream_handler = RespStreamHandler::new(stream);

    resp_stream_handler.write_all(&PING_COMMAND[..]).await?;
    let (_, buf) = resp_stream_handler.deserialize_stream().await?;
    if &buf[..] != PONG_STRING {
        return Err(Box::from(anyhow!("Didn't receive PING response")));
    }

    let replconf_command1 = serialize(
        &RespDatatype::Array(
            vec![
                RespDatatype::BulkString(b"REPLCONF".to_vec()),
                RespDatatype::BulkString(b"listening-port".to_vec()),
                RespDatatype::BulkString(slave_port.as_bytes().to_vec())
            ]
        )
    );
    resp_stream_handler.write_all(&replconf_command1).await?;
    let (_, buf) = resp_stream_handler.deserialize_stream().await?;
    if &buf[..] != OK_STRING {
        return Err(Box::from(anyhow!("Didn't receive OK response")));
    }

    let replconf_command2 = serialize(
        &RespDatatype::Array(
            vec![
                RespDatatype::BulkString(b"REPLCONF".to_vec()),
                RespDatatype::BulkString(b"capa".to_vec()),
                RespDatatype::BulkString(b"psync2".to_vec())
            ]
        )
    );
    resp_stream_handler.write_all(&replconf_command2).await?;
    let (_, buf) = resp_stream_handler.deserialize_stream().await?;
    if &buf[..] != OK_STRING {
        return Err(Box::from(anyhow!("Didn't receive OK response")));
    }

    let psync_command = serialize(
        &RespDatatype::Array(
            vec![
                RespDatatype::BulkString(b"PSYNC".to_vec()),
                RespDatatype::BulkString(b"?".to_vec()),
                RespDatatype::BulkString(b"-1".to_vec())
            ]
        )
    );
    resp_stream_handler.write_all(&psync_command).await?;
    let (resp_object, buf) = resp_stream_handler.deserialize_stream().await?;

    let replica_data = ReplicaData::new();
    match replica_interpret(resp_object, &buf, &replica_data).await {
        Some(RedisCommand::FullResync(master_replid, master_repl_offset)) => {
            set_value(b"master_replid", &master_replid).await;
            set_value(b"master_repl_offset", &master_repl_offset).await;
        },
        _ => return Err(Box::from(anyhow!("Couldn't deserialize response to PSYNC: {}", show(&buf[..])))),
    };

    resp_stream_handler.get_rdb().await?;

    tokio::spawn(async move {handle_master(resp_stream_handler, replica_data).await});

    return Ok(());
}

#[derive(Default)]
struct ReplicaData {
    bytes_processed: usize,
}

impl ReplicaData {
    fn new() -> Self {
        Default::default()
    }
}

async fn handle_master(mut resp_stream_reader: RespStreamHandler, mut replica_data: ReplicaData) {
    println!("Listening to master commands");
    loop {
        if resp_stream_reader.is_shutdown().await {
            println!("Stream closed");
            break;
        }
    
        println!("Deserializing");
        let (resp_object, collected) = resp_stream_reader.deserialize_stream().await
        .expect("Failed to deserialize RESP object");

        println!("Interpreting");
        let redis_command = replica_interpret(resp_object, &collected, &replica_data)
        .await
        .expect("Failed to interpret Redis command");
        
        replica_respond(&mut resp_stream_reader, &redis_command).await;

        replica_data.bytes_processed += collected.len();
    }
}

#[allow(unused)]
async fn replica_interpret(resp_object: RespDatatype, buf: &Vec<u8>, replica_data: &ReplicaData) -> Option<RedisCommand> {
    match resp_object {
        RespDatatype::Array(array) => {
            let mut array_iterator = array.into_iter();
            let command = match array_iterator.next() {
                Some(RespDatatype::BulkString(bulk_string)) => bulk_string.to_ascii_uppercase(),
                _ => return None,
            };
            match &command[..] {
                b"PING" => Some(RedisCommand::Pong),
                b"SET" => interpret_set(array_iterator).await,
                b"INFO" => interpret_info(array_iterator).await,
                b"REPLCONF" => interpret_replconf(array_iterator, &replica_data).await,
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
async fn interpret_replconf(mut array_iterator: IntoIter<RespDatatype>, replica_data: &ReplicaData) -> Option<RedisCommand> {
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
                            return Some(RedisCommand::ReplconfAck(replica_data.bytes_processed.to_string().into_bytes()))
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

async fn replica_respond(stream: &mut RespStreamHandler, redis_command: &RedisCommand) {
    match replica_formulate_response(&redis_command) {
        Some(responses) => {
            for response in responses {
                stream.write_all(&response)
                    .await
                    .expect("Failed to respond");
                println!("Response: {:?}", String::from_utf8(response).unwrap_or(String::new()));
            }
        },
        None => (),
    }
}

fn replica_formulate_response(redis_command: &RedisCommand) -> Option<Vec<Vec<u8>>> {
    match redis_command {
        RedisCommand::ReplconfAck(ack_arg) => {
            Some(vec![serialize(&RespDatatype::Array(vec![
                RespDatatype::BulkString(b"REPLCONF".to_vec()),
                RespDatatype::BulkString(b"ACK".to_vec()),
                RespDatatype::BulkString(ack_arg.to_owned())
            ]))])
        },
        _ => None,
    }
}

#[inline]
fn make_error_command<T: ToString>(string: T) -> Option<RedisCommand> {
    return Some(RedisCommand::Error(string.to_string()));
}