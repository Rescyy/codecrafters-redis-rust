use anyhow::anyhow;
use bytes::BufMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};

use crate::{deserialize, interpret, serialize, set_value, show, RedisCommand, RespDatatype, OK_STRING, PONG_STRING};

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

    let (resp_object, collected) = match deserialize(&mut buf) {
        Ok(result) => result,
        Err(err) => return Err(Box::from(anyhow!(err)))
    };

    match interpret(resp_object, &collected).await {
        Some(RedisCommand::FullResync(master_replid, master_repl_offset)) => {
            set_value(b"master_replid", &master_replid).await;
            set_value(b"master_repl_offset", &master_repl_offset).await;
        },
        _ => return Err(Box::from(anyhow!("Couldn't deserialize response to PSYNC: {}", show(&collected[..])))),
    };

    read_rdb(&mut stream, &mut buf).await?;

    tokio::spawn(async move {handle_master(stream).await});

    return Ok(());
}

async fn handle_master(mut stream: TcpStream) {
    
    println!("Listening to master commands");
    let mut buf = Vec::<u8>::new();
    loop {
        println!("Reading bytes");
        let read_bytes = stream.read_buf(&mut buf).await.expect("Couldn't read bytes");
        // println!("Current buffer: {}", show(&buf[..]));
        if read_bytes == 0 {
            println!("No bytes received");
            return;
        } else {
            println!("{} bytes received", read_bytes);
        }
    
        println!("Current buffer: {}", show(&buf[..]));
        println!("Deserializing");
        let (resp_object, collected) = deserialize(&mut buf)
        .expect("Failed to deserialize RESP object");
        println!("Current buffer: {}", show(&buf[..]));

        println!("Interpreting");
        let redis_command = interpret(resp_object, &collected)
        .await
        .expect("Failed to interpret Redis command");
        
        drop(redis_command);
    }
}

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