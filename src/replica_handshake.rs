use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{serialize, PING_COMMAND};

pub async fn send_handshake(master_host: &String, master_port: &String) -> Result<(), std::io::Error> {
    let mut stream = TcpStream::connect(format!("{master_host}:{master_port}")).await?;
    stream.write(&serialize(PING_COMMAND.clone())[..]).await?;
    return Ok(());
}