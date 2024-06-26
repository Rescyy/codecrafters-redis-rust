use anyhow::anyhow;
use tokio::net::TcpStream;

use crate::{interpret, serialize, set_value, show, RedisCommand, RespDatatype, RespStreamHandler, OK_STRING, PONG_STRING};

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

    match interpret(resp_object, &buf).await {
        Some(RedisCommand::FullResync(master_replid, master_repl_offset)) => {
            set_value(b"master_replid", &master_replid).await;
            set_value(b"master_repl_offset", &master_repl_offset).await;
        },
        _ => return Err(Box::from(anyhow!("Couldn't deserialize response to PSYNC: {}", show(&buf[..])))),
    };

    resp_stream_handler.get_rdb().await?;

    tokio::spawn(async move {handle_master(resp_stream_handler).await});

    return Ok(());
}

async fn handle_master(mut resp_stream_reader: RespStreamHandler) {
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
        let redis_command = interpret(resp_object, &collected)
        .await
        .expect("Failed to interpret Redis command");
        
        drop(redis_command);
    }
}