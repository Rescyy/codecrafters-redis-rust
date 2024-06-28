mod resp_handler;
use resp_handler::*;

mod command_interpreter;
use command_interpreter::*;

mod command_responder;
use command_responder::*;

mod database;
use database::*;

mod utils;
use utils::*;

mod replica_handshake;
use replica_handshake::*;

mod replicas;
use replicas::*;

use tokio::net::{TcpListener, TcpStream};
use std::env;

#[macro_use]
extern crate lazy_static;

const INCORRECT_FORMAT_PORT: &str = "Incorrect format for --port flag. Required format \"--port <PORT>\"";
const INCORRECT_FORMAT_REPLICAOF: &str = "Incorrect format for --replicaof flag. Required format \"--replicaof <MASTER_HOST MASTER_PORT>\"";

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let mut port = String::from("6379");
    let mut role: &[u8] = b"master";
    let mut master_host = String::from("localhost");
    let mut master_port = String::from("6379");
    let mut args = env::args();

    args.next();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--port" => {
                port = args.next().expect(INCORRECT_FORMAT_PORT);
                port.parse::<u16>().expect("Invalid port given");
            },
            "--replicaof" => {
                role = b"slave";
                let replicaof_arg = args.next().expect(INCORRECT_FORMAT_REPLICAOF);
                let mut master_args = replicaof_arg.split(" ");
                master_host = master_args.next().expect(INCORRECT_FORMAT_REPLICAOF).to_string();
                master_port = master_args.next().expect(INCORRECT_FORMAT_REPLICAOF).to_string();
                if master_args.next() != None {
                    panic!("{}", INCORRECT_FORMAT_REPLICAOF);
                }
                send_handshake(&master_host, &master_port, &port).await.expect("Handshake failed");
            },
            flag => panic!("Unknown flag: \"{flag}\""),
        }
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await.expect("Couldn't start the server");

    set_value(b"port", port.as_bytes()).await;
    set_value(b"role", role).await;
    set_value(b"master_port", master_port.as_bytes()).await;
    set_value(b"master_host", master_host.as_bytes()).await;
    if role == b"master" {
        set_value(b"master_replid", &generate_master_replid()).await;
        set_value(b"master_repl_offset", b"0").await;
        start_replicas();
    }

    loop {
        let stream = listener.accept().await;
        
        match stream {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    handle_client(stream).await
                });
            }
            Err(_) => ()
        }
    }
}

async fn handle_client(stream: TcpStream) {
    println!("Accepted new connection! Handling client");
    let mut resp_stream_handler = RespStreamHandler::new(stream);
    let mut replica_identifier: ReplicaIdentifier = ReplicaIdentifier::init();
    loop {
        if resp_stream_handler.is_shutdown().await {
            break;
        }
    
        println!("Deserializing");
        let (resp_object, collected) = resp_stream_handler.deserialize().await
        .expect("Failed to deserialize RESP object: ");

        println!("Interpreting");
        let redis_command = interpret(resp_object, &collected)
        .await
        .expect("Failed to interpret Redis command");
    
        println!("Responding");
        respond(&mut resp_stream_handler, &redis_command).await;

        if replica_identifier.is_replica(&redis_command) {
            break;
        }
    }
    
    if replica_identifier.is_synced() {
        handle_replica(resp_stream_handler).await;
    }
}

// fn main() {
//     let resp_object = deserialize(b"*5\r\n$3\r\nSET\r\n$4\r\npear\r\n$6\r\nbanana\r\n$2\r\npx\r\n$3\r\n100\r\n".to_vec())
//         .expect("Failed to deserialize RESP object");
//     println!("{:?}",resp_object);
// }