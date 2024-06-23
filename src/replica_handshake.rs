use anyhow::anyhow;
use bytes::BufMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};

use crate::{interpret, serialize, set_value, show, RedisCommand, RespDatatype, RespStreamHandler, OK_STRING, PONG_STRING};

lazy_static! {  
    static ref PING_COMMAND: Vec<u8> = serialize(&RespDatatype::Array(vec![RespDatatype::BulkString(b"PING".to_vec())]));
}

pub async fn send_handshake(master_host: &String, master_port: &String, slave_port: &String) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(format!("{master_host}:{master_port}")).await?;
    let mut resp_stream_handler = RespStreamHandler::new(stream);
    let mut buf: Vec<u8> = Vec::new();
    resp_stream_handler.write_all(&PING_COMMAND[..]).await?;
    resp_stream_handler.stream.read_buf(&mut buf).await?;
    println!("Passed");
    // resp_stream_handler.write_all(&PING_COMMAND[..]).await?;
    
    // dbg!(&resp_stream_handler);
    // let (_, buf) = resp_stream_handler.deserialize_stream().await?;
    // dbg!(&resp_stream_handler);
    // if &buf[..] != PONG_STRING {
    //     return Err(Box::from(anyhow!("Didn't receive PING response")));
    // }
    // dbg!(&resp_stream_handler);

    // let replconf_command1 = serialize(
    //     &RespDatatype::Array(
    //         vec![
    //             RespDatatype::BulkString(b"REPLCONF".to_vec()),
    //             RespDatatype::BulkString(b"listening-port".to_vec()),
    //             RespDatatype::BulkString(slave_port.as_bytes().to_vec())
    //         ]
    //     )
    // );
    // resp_stream_handler.write_all(&replconf_command1).await?;
    // let (_, buf) = resp_stream_handler.deserialize_stream().await?;
    // if &buf[..] != OK_STRING {
    //     return Err(Box::from(anyhow!("Didn't receive OK response")));
    // }
    
    // let replconf_command2 = serialize(
    //     &RespDatatype::Array(
    //         vec![
    //             RespDatatype::BulkString(b"REPLCONF".to_vec()),
    //             RespDatatype::BulkString(b"capa".to_vec()),
    //             RespDatatype::BulkString(b"psync2".to_vec())
    //         ]
    //     )
    // );
    // resp_stream_handler.write_all(&replconf_command2).await?;
    // let (_, buf) = resp_stream_handler.deserialize_stream().await?;
    // if &buf[..] != OK_STRING {
    //     return Err(Box::from(anyhow!("Didn't receive OK response")));
    // }

    // let psync_command = serialize(
    //     &RespDatatype::Array(
    //         vec![
    //             RespDatatype::BulkString(b"PSYNC".to_vec()),
    //             RespDatatype::BulkString(b"?".to_vec()),
    //             RespDatatype::BulkString(b"-1".to_vec())
    //         ]
    //     )
    // );
    // resp_stream_handler.write_all(&psync_command).await?;
    // let (resp_object, buf) = resp_stream_handler.deserialize_stream().await?;

    // match interpret(resp_object, &buf).await {
    //     Some(RedisCommand::FullResync(master_replid, master_repl_offset)) => {
    //         set_value(b"master_replid", &master_replid).await;
    //         set_value(b"master_repl_offset", &master_repl_offset).await;
    //     },
    //     _ => return Err(Box::from(anyhow!("Couldn't deserialize response to PSYNC: {}", show(&buf[..])))),
    // };

    // resp_stream_handler.get_rdb().await?;

    // tokio::spawn(async move {handle_master(resp_stream_handler).await});

    return Ok(());
}

async fn handle_master(mut resp_stream_reader: RespStreamHandler) {
    println!("Listening to master commands");
    loop {
        if resp_stream_reader.is_shutdown().await {
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

#[allow(unused)]
async fn read_rdb(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
    let mut index = 0;
    // let mut state = 0; // 0-looking for $, 1-looking for number, 2-looking for \r\n, 3-looking for text
    // let box_error = Err(Box::from(anyhow!("Invalid RDB File")));
    loop {
        match buf.get(index) {
            Some(&b'$') => {index += 1; break},
            Some(_) => return Err(Box::from(anyhow!("Invalid RDB File1"))),
            None => {stream.read_buf(buf).await?;}
        }
    };
    let number;
    loop {
        match buf.get(index) {
            Some(&b'\r') => {
                number = String::from_utf8(buf[1..index].to_vec())?.parse::<usize>()?;
                index += 1;
                break;
            },
            Some(_) => index += 1,
            None => {stream.read_buf(buf).await?;}
        }
    };
    stream.read_buf(buf).await?;
    match buf.get(index) {
        Some(&b'\n') => index += 1,
        Some(_) => return Err(Box::from(anyhow!("Invalid RDB File2"))),
        None => {stream.read_buf(buf).await?;},
    }
    if buf.len() == number+index {
        buf.clear();
    } else if buf.len() > number+index {
        let temp_buf = buf[index+number..].to_vec();
        buf.clear();
        buf.put(&temp_buf[..])
    } else {
        while buf.len() < number+index {
            stream.read_buf(buf).await?;
        }
        let temp_buf = buf[index+number..].to_vec();
        buf.clear();
        buf.put(&temp_buf[..])
    }
    return Ok(())
}