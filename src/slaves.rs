use std::collections::LinkedList;

use tokio::{io::AsyncWriteExt, net::TcpStream, sync::Mutex};

use crate::RedisCommand;

lazy_static! {
    static ref SLAVES: Mutex<LinkedList<Slave>> = Mutex::new(LinkedList::new());
    static ref SLAVE_TASKS: Mutex<LinkedList<SlaveTask>> = Mutex::new(LinkedList::new());
}

#[derive(PartialEq)]
enum SlaveState {
    Null,
    Ponged,
    Replconf1,
    Replconf2,
    FullSynced,
}

pub struct SlaveIdentifier {
    slave_state: SlaveState,
}

impl SlaveIdentifier {
    pub fn init() -> Self {
        SlaveIdentifier {slave_state: SlaveState::Null}
    }

    pub fn is_slave(&mut self, redis_command: &RedisCommand) -> bool {
        match (redis_command, &self.slave_state) {
            (RedisCommand::Pong, SlaveState::Null) => {
                println!("Slave Ponged");
                self.slave_state = SlaveState::Ponged
            },
            (RedisCommand::ReplconfOk1, SlaveState::Ponged) => {
                println!("Slave Replconf1");
                self.slave_state = SlaveState::Replconf1
            },
            (RedisCommand::ReplconfOk2, SlaveState::Replconf1) => {
                println!("Slave Replconf2");
                self.slave_state = SlaveState::Replconf2
            },
            (RedisCommand::FullResync(_,_), SlaveState::Replconf2) => {
                println!("Slave Full Synced");
                self.slave_state = SlaveState::FullSynced;
                return true
            },
            _ => self.slave_state = SlaveState::Null,
        }
        return false;
    }

    pub fn is_synced(&self) -> bool {
        self.slave_state == SlaveState::FullSynced
    }
}

pub struct Slave {
    stream: TcpStream,
}

impl Slave {
    async fn new(stream: TcpStream) {
        let mut slaves = SLAVES.lock().await;
        slaves.push_back(Slave {stream});
    }

    async fn give_task(&mut self, slave_task: &SlaveTask) {
        self.stream.write_all(&slave_task.task_command[..]).await.unwrap();
        self.stream.flush().await.unwrap();
    }
}

pub async fn handle_slave(stream: TcpStream) {
    println!("Accepted slave connection.");
    Slave::new(stream).await;
    loop {
        // do slave stuff maybe
        break;
    }
}

pub struct SlaveTask {
    task_command: Vec<u8>,
}

impl SlaveTask {
    pub fn new(task_command: Vec<u8>) -> Self {
        SlaveTask {task_command}
    }
}

pub async fn push_to_slaves(slave_task: SlaveTask) {
    // tokio::spawn(async move {
        let mut slave_tasks = SLAVE_TASKS.lock().await;
        slave_tasks.push_back(slave_task);
    // });
}

pub fn start_slaves() {
    tokio::spawn(async {
        handle_slaves().await;
    });
}

async fn handle_slaves() {
    loop {
        let mut slaves = SLAVES.lock().await;
        let slave_task = {SLAVE_TASKS.lock().await.pop_front()};
        match slave_task {
            Some(slave_task) => {
                for slave in slaves.iter_mut() {
                    slave.give_task(&slave_task).await;
                }
            },
            None => (),
        }
    }
}