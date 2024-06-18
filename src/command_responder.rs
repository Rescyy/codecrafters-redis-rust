use crate::command_interpreter::RedisCommand;
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use crate::resp_handler::{serialize, RespDatatype};

const PONG_STRING: &[u8] = b"+PONG\r\n";
const OK_STRING: &[u8] = b"+OK\r\n";

pub async fn respond(stream: &mut TcpStream, redis_command: RedisCommand) {
    let response = formulate_response(redis_command);
    println!("Response: {:?}", String::from_utf8(response.clone()).unwrap());
    stream.write(&response)
        .await
        .expect("Failed to respond");
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
            PONG_STRING.to_owned()
        },
        RedisCommand::Ok => {
            OK_STRING.to_owned()
        },
        RedisCommand::BulkString(message) => {
            serialize(RespDatatype::BulkString(message))
        },
        _ => vec![],
    }
}