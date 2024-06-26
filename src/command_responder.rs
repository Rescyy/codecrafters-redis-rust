use crate::{command_interpreter::RedisCommand, RespStreamHandler};
use crate::resp_handler::{serialize, RespDatatype};

pub const PONG_STRING: &[u8] = b"+PONG\r\n";
pub const OK_STRING: &[u8] = b"+OK\r\n";
const NULL_BULK_STRING: &[u8] = b"$-1\r\n";

pub async fn respond(stream: &mut RespStreamHandler, redis_command: &RedisCommand) {
    match formulate_response(&redis_command) {
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

fn formulate_response(redis_command: &RedisCommand) -> Option<Vec<Vec<u8>>> {
    match redis_command {
        RedisCommand::Pong => {
            Some(vec![PONG_STRING.to_vec()])
        },
        RedisCommand::Ok | RedisCommand::ReplconfOk1 | RedisCommand::ReplconfOk2 => {
            Some(vec![OK_STRING.to_vec()])
        },
        RedisCommand::BulkString(message) => {
            Some(vec![serialize(&RespDatatype::BulkString(message.to_owned()))])
        },
        RedisCommand::Error(message) => {
            Some(vec![serialize(&RespDatatype::SimpleError(message.to_owned()))])
        },
        RedisCommand::NullBulkString => {
            Some(vec![NULL_BULK_STRING.to_vec()])
        },
        RedisCommand::SimpleString(message) => {
            Some(vec![serialize(&RespDatatype::SimpleString(String::from_utf8(message.to_owned()).unwrap()))])
        },
        RedisCommand::RespDatatype(resp_object) => {
            Some(vec![serialize(&resp_object)])
        },
        RedisCommand::FullResync(psync_response, rdb_file) => {
            Some(vec![
                serialize(&RespDatatype::SimpleString(String::from_utf8(psync_response.to_owned()).unwrap())),
                serialize(&RespDatatype::RDBFile(rdb_file.to_owned()))
            ])
        },
        RedisCommand::ReplconfAck(_) => None,
    }
}