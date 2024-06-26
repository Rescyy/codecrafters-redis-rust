use std::{collections::LinkedList, time::Duration};

use tokio::time::sleep;
use tokio::{io::AsyncWriteExt, sync::Mutex, time::Instant};

use crate::{RedisCommand, RespStreamHandler};

lazy_static! {
    static ref REPLICAS: Mutex<LinkedList<Replica>> = Mutex::new(LinkedList::new());
    static ref REPLICA_TASKS: Mutex<LinkedList<ReplicaTask>> = Mutex::new(LinkedList::new());
}

#[derive(PartialEq)]
enum ReplicaState {
    Null,
    Ponged,
    Replconf1,
    Replconf2,
    FullSynced,
}

pub struct ReplicaIdentifier {
    slave_state: ReplicaState,
}

impl ReplicaIdentifier {
    pub fn init() -> Self {
        ReplicaIdentifier {slave_state: ReplicaState::Null}
    }

    pub fn is_replica(&mut self, redis_command: &RedisCommand) -> bool {
        match (redis_command, &self.slave_state) {
            (RedisCommand::Pong, ReplicaState::Null) => {
                println!("Slave Ponged");
                self.slave_state = ReplicaState::Ponged
            },
            (RedisCommand::ReplconfOk1, ReplicaState::Ponged) => {
                println!("Slave Replconf1");
                self.slave_state = ReplicaState::Replconf1
            },
            (RedisCommand::ReplconfOk2, ReplicaState::Replconf1) => {
                println!("Slave Replconf2");
                self.slave_state = ReplicaState::Replconf2
            },
            (RedisCommand::FullResync(_,_), ReplicaState::Replconf2) => {
                println!("Slave Full Synced");
                self.slave_state = ReplicaState::FullSynced;
                return true
            },
            _ => self.slave_state = ReplicaState::Null,
        }
        return false;
    }

    pub fn is_synced(&self) -> bool {
        self.slave_state == ReplicaState::FullSynced
    }
}

pub struct Replica {
    stream: RespStreamHandler,
}

impl Replica {
    async fn new(stream: RespStreamHandler) {
        let mut replicas = REPLICAS.lock().await;
        replicas.push_back(Replica {stream});
    }

    async fn give_task(&mut self, replica_task: &ReplicaTask) {
        self.stream.write_all(&replica_task.task_command[..]).await.unwrap();
        self.stream.stream.flush().await.unwrap();
    }
}

pub async fn handle_replica(stream: RespStreamHandler) {
    println!("Accepted replica connection.");
    Replica::new(stream).await;
    loop {
        break;
    }
}

pub struct ReplicaTask {
    task_command: Vec<u8>,
}

impl ReplicaTask {
    pub fn new(task_command: Vec<u8>) -> Self {
        ReplicaTask {task_command}
    }
}

pub async fn push_to_replicas(replica_task: ReplicaTask) {
    // tokio::spawn(async move {
        let mut slave_tasks = REPLICA_TASKS.lock().await;
        slave_tasks.push_back(replica_task);
    // });
}

pub async fn wait_to_replicas(numreplicas: usize, timeout: usize) -> usize {
    
    let start = Instant::now();
    let mut replicas = REPLICAS.lock().await;
    let mut num_replies = 0;
    let replconf_getack: &[u8] = b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nGETACK\r\n$1\r\n*\r\n";
    for replica in replicas.iter_mut() {
        replica.stream.stream.write_all(&replconf_getack).await.unwrap();
    }

    let mut buf: Vec<u8> = Vec::new();
    while start.elapsed() < Duration::from_millis(timeout.try_into().unwrap()) && num_replies < numreplicas {
        for replica in replicas.iter_mut() {
            match replica.stream.stream.try_read_buf(&mut buf) {
                Ok(0) => continue,
                Ok(_) => {
                    if buf.starts_with(b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n") {
                        num_replies += 1;
                    }
                    buf.clear();
                }
                Err(_) => continue,
            }
        }
        sleep(Duration::from_millis(1)).await;
    }

    return num_replies; 
}

pub fn start_replicas() {
    tokio::spawn(async {
        handle_replicas().await;
    });
}

async fn handle_replicas() {
    loop {
        let mut replicas = REPLICAS.lock().await;
        let replica_task = {REPLICA_TASKS.lock().await.pop_front()};
        match replica_task {
            Some(replica_task) => {
                for slave in replicas.iter_mut() {
                    slave.give_task(&replica_task).await;
                }
            },
            None => (),
        }
    }
}