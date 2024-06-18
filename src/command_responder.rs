use crate::command_interpreter::RedisCommand;
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use crate::resp_handler::{serialize, RespDatatype};

pub async fn respond(mut stream: TcpStream, redis_command: RedisCommand) {
    match redis_command {
        RedisCommand::Ping => {
            stream.write(b"+PONG\r\n")
            .await
            .expect("Failed to respond");
        },
        RedisCommand::Echo(message) => {
            stream.write(
                &serialize(
                    RespDatatype::BulkString(message)))
                    .await
                    .expect("Failed to respond");
        },
    }
}