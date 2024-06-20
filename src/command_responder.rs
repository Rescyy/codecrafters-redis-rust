use crate::command_interpreter::RedisCommand;
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use crate::resp_handler::{serialize, RespDatatype};

pub const PONG_STRING: &[u8] = b"+PONG\r\n";
pub const OK_STRING: &[u8] = b"+OK\r\n";
const NULL_BULK_STRING: &[u8] = b"$-1\r\n";

pub async fn respond(stream: &mut TcpStream, redis_command: RedisCommand) {
    let response = formulate_response(redis_command);
    stream.write(&response)
        .await
        .expect("Failed to respond");
    stream.flush().await.unwrap();
    println!("Response: {:?}", String::from_utf8(response));
}

#[allow(dead_code)]
pub fn respond_test(redis_command: RedisCommand) {
    let response = formulate_response(redis_command);
    println!("Response: {:?}", String::from_utf8(response));
}

#[allow(unreachable_patterns)]
fn formulate_response(redis_command: RedisCommand) -> Vec<u8> {
    match redis_command {
        RedisCommand::Ping => {
            PONG_STRING.to_vec()
        },
        RedisCommand::Ok => {
            OK_STRING.to_vec()
        },
        RedisCommand::BulkString(message) => {
            serialize(&RespDatatype::BulkString(message))
        },
        RedisCommand::Error(message) => {
            serialize(&RespDatatype::SimpleError(message))
        },
        RedisCommand::NullBulkString => {
            NULL_BULK_STRING.to_vec()
        }
    }
}