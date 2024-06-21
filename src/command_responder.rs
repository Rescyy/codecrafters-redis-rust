use crate::command_interpreter::RedisCommand;
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use crate::resp_handler::{serialize, RespDatatype};

pub const PONG_STRING: &[u8] = b"+PONG\r\n";
pub const OK_STRING: &[u8] = b"+OK\r\n";
const NULL_BULK_STRING: &[u8] = b"$-1\r\n";

pub async fn respond(stream: &mut TcpStream, redis_command: &RedisCommand) {
    let responses = formulate_response(&redis_command);
    for response in responses {
        stream.write(&response)
            .await
            .expect("Failed to respond");
        stream.flush().await.unwrap();
        println!("Response: {:?}", String::from_utf8(response));
    }
}

#[allow(unreachable_patterns)]
fn formulate_response(redis_command: &RedisCommand) -> Vec<Vec<u8>> {
    match redis_command {
        RedisCommand::Pong => {
            vec![PONG_STRING.to_vec()]
        },
        RedisCommand::Ok | RedisCommand::ReplconfOk1 | RedisCommand::ReplconfOk2 => {
            vec![OK_STRING.to_vec()]
        },
        RedisCommand::BulkString(message) => {
            vec![serialize(&RespDatatype::BulkString(message.to_owned()))]
        },
        RedisCommand::Error(message) => {
            vec![serialize(&RespDatatype::SimpleError(message.to_owned()))]
        },
        RedisCommand::NullBulkString => {
            vec![NULL_BULK_STRING.to_vec()]
        },
        RedisCommand::SimpleString(message) => {
            vec![serialize(&RespDatatype::SimpleString(String::from_utf8(message.to_owned()).unwrap()))]
        },
        RedisCommand::RespDatatype(resp_object) => {
            vec![serialize(&resp_object)]
        },
        RedisCommand::FullResync(psync_response, rdb_file) => {
            vec![
                serialize(&RespDatatype::SimpleString(String::from_utf8(psync_response.to_owned()).unwrap())),
                serialize(&RespDatatype::RDBFile(rdb_file.to_owned()))
            ]
        },
    }
}