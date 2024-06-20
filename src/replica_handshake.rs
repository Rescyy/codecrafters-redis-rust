use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{serialize, RespDatatype};

lazy_static! {  
    static ref PING_COMMAND: Vec<u8> = serialize(&RespDatatype::Array(vec![RespDatatype::BulkString(b"PING".to_vec())]));
}

pub async fn send_handshake(master_host: &String, master_port: &String, slave_port: &String) -> Result<(), std::io::Error> {
    let mut stream = TcpStream::connect(format!("{master_host}:{master_port}")).await?;
    stream.write(&PING_COMMAND[..]).await?;
    stream.flush().await?;
    let replconf_command1 = serialize(
        &RespDatatype::Array(
            vec![
                RespDatatype::BulkString(b"REPLCONF".to_vec()),
                RespDatatype::BulkString(b"listening-port".to_vec()),
                RespDatatype::BulkString(slave_port.as_bytes().to_vec())
            ]
        )
    );
    stream.write(&replconf_command1).await?;
    stream.flush().await?;
    let replconf_command2 = serialize(
        &RespDatatype::Array(
            vec![
                RespDatatype::BulkString(b"REPLCONF".to_vec()),
                RespDatatype::BulkString(b"capa".to_vec()),
                RespDatatype::BulkString(b"psync2".to_vec())
            ]
        )
    );
    stream.write(&replconf_command2).await?;
    stream.flush().await?;
    return Ok(());
}