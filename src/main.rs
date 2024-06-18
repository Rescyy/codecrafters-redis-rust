mod resp_handler;
use resp_handler::*;

mod command_interpreter;
use command_interpreter::*;

mod command_responder;
use command_responder::*;

use tokio::net::{TcpListener, TcpStream};
use tokio::io::AsyncReadExt;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;
        
        match stream {
            Ok((stream, _)) => {
                println!("Accepted new connection!");
                tokio::spawn(handle_client(stream));
            }
            Err(_) => ()
        }
    }
}

async fn handle_client(mut stream: TcpStream) {
    let mut buf = Vec::<u8>::new();
    let read_bytes = stream.read_to_end(&mut buf).await.unwrap();
    if read_bytes <= 0 {
        return;
    }

    let resp_object = deserialize(buf)
    .expect("Failed to deserialize RESP object");

    let redis_command = interpret(resp_object)
    .expect("Failed to interpret Redis command");

    respond(stream, redis_command).await;
}

// fn main() {
//     let resp_object = deserialize(b"*2\r\n$4\r\nECHO\r\n$9\r\npineapple\r\n".to_vec())
//         .expect("Failed to deserialize RESP object");
    
//     println!("{:?}", resp_object);

//     let redis_command = interpret(resp_object)
//     .expect("Failed to interpret Redis command");

//     println!("{:?}", redis_command);

//     respond_test(redis_command);
// }