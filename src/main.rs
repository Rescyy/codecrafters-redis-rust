// Uncomment this block to pass the first stage
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;
        
        match stream {
            Ok((mut stream, _)) => {
                println!("accepted new connection!");

                tokio::spawn(async move {
                    let mut buf = [0u8; 512];
                    loop {
                        let read_count = stream.read(&mut buf).await.unwrap();
                        if read_count == 0 {
                            break;
                        }

                        stream.write(b"+PONG\r\n").await.unwrap();
                    }
                });
            }
            Err(e) => {

            }
        }
    }

    println!("Program ended");
}

// fn handle_client(mut stream: TcpStream) {
//     let mut buffer = [0u8; 512]; 
//     let response = "+PONG\r\n".as_bytes();
//     loop {
//         let read_bytes = stream.read(&mut buffer).expect("Failed to read from client");
//         if read_bytes == 0 {
//             return;
//         }
//         stream.write_all(response).expect("Failed to send PONG response");
//     }
// }
