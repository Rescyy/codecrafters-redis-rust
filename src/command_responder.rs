use crate::command_interpreter::RedisCommand;
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use crate::resp_handler::{serialize, RespDatatype};

pub async fn respond(mut stream: TcpStream, redis_command: RedisCommand) {
    let response = formulate_response(redis_command);
    stream.write(&response)
        .await
        .expect("Failed to respond");
}

pub fn respond_test(redis_command: RedisCommand) {
    let response = formulate_response(redis_command);
    println!("Response: {:?}", String::from_utf8(response));
}

fn formulate_response(redis_command: RedisCommand) -> Vec<u8> {
    match redis_command {
        RedisCommand::Ping => {
            b"+PONG\r\n".to_vec()
        },
        RedisCommand::Echo(message) => {
            serialize(RespDatatype::BulkString(message))
        },
    }
}