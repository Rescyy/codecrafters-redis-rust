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

#[derive(Debug)]
pub struct Replica {
    stream: RespStreamHandler,
    offset: usize,
}

impl Replica {
    async fn new(stream: RespStreamHandler) {
        let mut replicas = REPLICAS.lock().await;
        replicas.push_back(Replica {
            stream, 
            offset: 0
        });
    }

    async fn give_task(&mut self, replica_task: &ReplicaTask) {
        let task_command = &replica_task.task_command[..];
        self.offset += task_command.len();
        self.stream.write_all(task_command).await.unwrap();
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

pub async fn wait_to_replicas(start: Instant, numreplicas: usize, timeout: usize) -> usize {
    
    let timeout: u64 = timeout.try_into().unwrap();
    let mut replicas = REPLICAS.lock().await;
    let mut num_replies = 0;
    let replconf_getack: &[u8] = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
    let mut busy_replicas: Vec<&mut Replica> = Vec::new();
    for replica in replicas.iter_mut() {
        dbg!(&replica, num_replies);
        if replica.offset == 0 {
            num_replies += 1;
            continue
        }
        replica.stream.stream.write_all(&replconf_getack).await.unwrap_or(());
        busy_replicas.push(replica);
    }
    let mut buf: Vec<u8> = Vec::new();

    while start.elapsed() < Duration::from_millis(timeout) || timeout == 0 {
        for replica in busy_replicas.iter_mut() {
            match replica.stream.stream.try_read_buf(&mut buf) {
                Ok(0) => continue,
                Ok(_) => {
                    if buf.starts_with(b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n") {
                        num_replies += 1;
                    }
                }
                Err(_) => continue,
            }
            buf.clear();
        }
        if num_replies >= numreplicas {
            num_replies = numreplicas;
            break;
        }
        sleep(Duration::from_millis(1)).await;
    }

    return num_replies; 
}

// pub async fn wait_to_replicas(numreplicas: usize, timeout: usize) -> usize {

//     // let mutex_timeout: Mutex<bool> = Mutex::from(true);
//     let start = Instant::now();
//     let mutex_num_replies: Mutex<usize> = Mutex::from(0);
//     // let timeout: *const Mutex<bool> = &mutex_timeout as *const Mutex<bool>;
//     // let num_replies: *const Mutex<usize> = &mutex_num_replies as * const Mutex<usize>;

//     let mut replicas = REPLICAS.lock().await;   
//     let mut wait_futures: Vec<JoinHandle<()>> = Vec::with_capacity(replicas.len());
//     let replconf_getack: &[u8] = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
//     for replica in replicas.iter_mut() {
//         replica.stream.stream.write_all(replconf_getack).await.unwrap();
//     }
//     for replica in replicas.iter_mut() {
//         let wait_future = tokio::spawn(async {
//             match replica.stream.deserialize().await {
//                 Ok((RespDatatype::Array(array), _)) => {
//                     if array.len() != 3 {
//                         return;
//                     }
//                     match &array[0] {
//                         RespDatatype::BulkString(replconf) => {
//                             if &replconf[..] != b"REPLCONF" {return}
//                         }
//                         _ => return,
//                     }
//                     match &array[1] {
//                         RespDatatype::BulkString(ack) => {
//                             if &ack[..] != b"ACL" {return}
//                         }
//                         _ => return,
//                     }
//                     match &array[2] {
//                         RespDatatype::BulkString(offset) => {
//                             let offset  = match String::from_utf8(offset.clone()) {
//                                 Ok(offset) => offset,
//                                 _ => return,
//                             };
//                             let offset = match offset.parse::<usize>() {
//                                 Ok(offset) => offset,
//                                 _ => return,
//                             };

//                         }
//                         _ => return,
//                     }
//                 },
//                 _ => (),
//             }
//         });
//         wait_futures.push(wait_future);
//     }

//     while start.elapsed() < Duration::from_millis(timeout.try_into().unwrap()) || num_replies <= numreplicas {

//     }

//     todo!();
// }

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