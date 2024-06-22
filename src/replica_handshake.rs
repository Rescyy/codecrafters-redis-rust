use anyhow::anyhow;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};

use crate::{deserialize, interpret, serialize, set_value, RedisCommand, RespDatatype, OK_STRING, PONG_STRING};

lazy_static! {  
    static ref PING_COMMAND: Vec<u8> = serialize(&RespDatatype::Array(vec![RespDatatype::BulkString(b"PING".to_vec())]));
}

pub async fn send_handshake(master_host: &String, master_port: &String, slave_port: &String) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(format!("{master_host}:{master_port}")).await?;
    let mut buf = Vec::<u8>::new();
    stream.write_all(&PING_COMMAND[..]).await?;
    stream.read_buf(&mut buf).await?;

    if &buf[..] != PONG_STRING {
        return Err(Box::from(anyhow!("Didn't receive PING response")));
    }

    buf.clear();

    let replconf_command1 = serialize(
        &RespDatatype::Array(
            vec![
                RespDatatype::BulkString(b"REPLCONF".to_vec()),
                RespDatatype::BulkString(b"listening-port".to_vec()),
                RespDatatype::BulkString(slave_port.as_bytes().to_vec())
            ]
        )
    );
    stream.write_all(&replconf_command1).await?;
    stream.read_buf(&mut buf).await?;

    if &buf[..] != OK_STRING {
        return Err(Box::from(anyhow!("Didn't receive OK response")));
    }

    buf.clear();
    
    let replconf_command2 = serialize(
        &RespDatatype::Array(
            vec![
                RespDatatype::BulkString(b"REPLCONF".to_vec()),
                RespDatatype::BulkString(b"capa".to_vec()),
                RespDatatype::BulkString(b"psync2".to_vec())
            ]
        )
    );
    stream.write_all(&replconf_command2).await?;
    stream.read_buf(&mut buf).await?;

    if &buf[..] != OK_STRING {
        return Err(Box::from(anyhow!("Didn't receive OK response")));
    }

    buf.clear();

    let psync_command = serialize(
        &RespDatatype::Array(
            vec![
                RespDatatype::BulkString(b"PSYNC".to_vec()),
                RespDatatype::BulkString(b"?".to_vec()),
                RespDatatype::BulkString(b"-1".to_vec())
            ]
        )
    );
    stream.write_all(&psync_command).await?;
    stream.read_buf(&mut buf).await?;

    let resp_object = match deserialize(&buf) {
        Some(resp_object) => resp_object,
        None => return Err(Box::from(anyhow!("Invalid response to PSYNC")))
    };
    
    match interpret(resp_object, &buf).await {
        Some(RedisCommand::FullResync(master_replid, master_repl_offset)) => {
            set_value(b"master_replid", &master_replid).await;
            set_value(b"master_repl_offset", &master_repl_offset).await;
        },
        _ => return Err(Box::from(anyhow!("Invalid response to PSYNC"))),
    };

    buf.clear();

    stream.read_buf(&mut buf).await?;

    // handle RDB file somehow

    buf.clear();

    tokio::spawn(async move {handle_master(stream).await});

    return Ok(());
}

async fn handle_master(mut stream: TcpStream) {
    println!("Listening to master commands");
    loop {
        let mut buf = Vec::<u8>::new();
        println!("Reading bytes");
        let read_bytes = stream.read_buf(&mut buf).await.expect("Couldn't read bytes");
        if read_bytes == 0 {
            println!("No bytes received");
            return;
        }
    
        println!("Deserializing");
        let resp_object = deserialize(&buf)
        .expect("Failed to deserialize RESP object");
    
        println!("Interpreting");
        let redis_command = interpret(resp_object, &buf)
        .await
        .expect("Failed to interpret Redis command");
        
        drop(redis_command);
    }
}