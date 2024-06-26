use std::collections::LinkedList;

use tokio::{io::AsyncWriteExt, net::TcpStream, sync::Mutex};

use crate::RedisCommand;

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
    stream: TcpStream,
}

impl Replica {
    async fn new(stream: TcpStream) {
        let mut replicas = REPLICAS.lock().await;
        replicas.push_back(Replica {stream});
    }

    async fn give_task(&mut self, replica_task: &ReplicaTask) {
        self.stream.write_all(&replica_task.task_command[..]).await.unwrap();
        self.stream.flush().await.unwrap();
    }
}

pub async fn handle_replica(stream: TcpStream) {
    println!("Accepted replica connection.");
    Replica::new(stream).await;
    loop {
        // do replica stuff maybe
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