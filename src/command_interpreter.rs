use crate::resp_handler::RespDatatype;

pub enum RedisCommand {
    Ping,
    Echo(Option<Vec<u8>>),
}

pub fn interpret(resp_object: RespDatatype) -> Option<RedisCommand> {
    match resp_object {
        RespDatatype::Array(Some(array)) => {
            let command = match array.get(0) {
                Some(RespDatatype::BulkString(Some(bulk_string))) => &bulk_string[..],
                _ => return None,
            };
            match command {
                b"PING" => {
                    return Some(RedisCommand::Ping);
                },
                b"ECHO" => {
                    match array.get(1) {
                        Some(RespDatatype::BulkString(Some(message))) => 
                        return Some(RedisCommand::Echo(Some(message.to_owned()))),
                        _ => return None,
                    };
                },
                _ => return None,
            }
        },
        _ => return None,
    }
}