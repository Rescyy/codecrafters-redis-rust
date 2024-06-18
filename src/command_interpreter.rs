use crate::resp_handler::RespDatatype;
use crate::database::*;

#[derive(Debug)]
pub enum RedisCommand {
    Ping,
    Ok,
    BulkString(Option<Vec<u8>>),
}

const NULL_BULK_STRING_COMMAND: RedisCommand = RedisCommand::BulkString(None);

pub async fn interpret(resp_object: RespDatatype) -> Option<RedisCommand> {
    match resp_object {
        RespDatatype::Array(Some(array)) => {
            let command = match array.get(0) {
                Some(RespDatatype::BulkString(Some(bulk_string))) => bulk_string.to_ascii_uppercase(),
                _ => return None,
            };
            match &command[..] {
                b"PING" => {
                    return Some(RedisCommand::Ping);
                },
                b"ECHO" => {
                    match array.get(1) {
                        Some(RespDatatype::BulkString(Some(message))) => 
                        return Some(RedisCommand::BulkString(Some(message.to_owned()))),
                        _ => return None,
                    };
                },
                b"SET" => {
                    let key = match array.get(1) {
                        Some(RespDatatype::BulkString(Some(key))) => key,
                        _ => return None,
                    };
                    let value = match array.get(2) {
                        Some(RespDatatype::BulkString(Some(value))) => value,
                        _ => return None,
                    };
                    set_value(key, value).await;
                    return Some(RedisCommand::Ok);
                },
                b"GET" => {
                    let key = match array.get(1) {
                        Some(RespDatatype::BulkString(Some(key))) => key,
                        _ => return None,
                    };
                    match get_value(key).await {
                        Some(value) => return Some(RedisCommand::BulkString(Some(value))),
                        None => return Some(NULL_BULK_STRING_COMMAND),
                    }
                },
                _ => return None,
            }
        },
        _ => return None,
    }
}