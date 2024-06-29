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
use std::{env, path::Path};

#[macro_use]
extern crate lazy_static;

const INCORRECT_FORMAT_PORT: &str = "Incorrect format for --port flag. Required format \"--port <PORT>\"";
const INCORRECT_FORMAT_REPLICAOF: &str = "Incorrect format for --replicaof flag. Required format \"--replicaof <MASTER_HOST MASTER_PORT>\"";
const INCORRECT_FORMAT_DIR: &str = "Incorrect format for --dir flag. Required format \"--replicaof <path>\"";
const INCORRECT_FORMAT_DBFILENAME: &str = "Incorrect format --dbfilename flag. Required \"--dbfilename <name>.rdb\"";

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let mut config = CONFIG.lock().await;
    let mut port = String::from("6379");
    let mut role: &[u8] = b"master";
    let mut master_host = String::from("localhost");
    let mut master_port = String::from("6379");
    let mut args = env::args();
    let mut dir = String::from("./");
    let mut dbfilename = String::from("rdbfilename");

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
            "--dir" => {
                let test_dir = args.next().expect(INCORRECT_FORMAT_DIR);
                if Path::new(test_dir.as_str()).exists() {
                    dir = test_dir;
                } else {
                    panic!("The directory given was not found")
                }
            },
            "--dbfilename" => {
                dbfilename = args.next().expect(INCORRECT_FORMAT_DBFILENAME);
                if !dbfilename.ends_with(".rdb") {
                    dbfilename.push_str(".rdb");
                }
            }
            flag => panic!("Unknown flag: \"{flag}\""),
        }
    }
    
    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await.expect("Couldn't start the server");
    
    config.insert(b"port".to_vec(), port.into_bytes());
    config.insert(b"role".to_vec(), role.to_vec());
    config.insert(b"master_port".to_vec(), master_port.into_bytes());
    config.insert(b"master_host".to_vec(), master_host.into_bytes());
    config.insert(b"dir".to_vec(), dir.into_bytes());
    config.insert(b"dbfilename".to_vec(), dbfilename.into_bytes());
    if role == b"master" {
        config.insert(b"master_replid".to_vec(), generate_master_replid());
        config.insert(b"master_repl_offset".to_vec(), vec![b'0']);
        start_replicas();
    }

    drop(config);
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