// Uncomment this block to pass the first stage
use std::net::TcpListener;
use std::net::TcpStream;
use std::io::Read;
use std::io::Write;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_client(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) { 
    stream.flush().expect("Error flushing the stream");
    let response = "+PONG\r\n".as_bytes();
    stream.write_all(response).expect("Failed to send PONG response");
}
