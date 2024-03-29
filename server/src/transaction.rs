use crate::command::Command;

#[derive(Debug)]
pub struct Transaction {
    error: bool,
    queue: Vec<Command>,
}

impl Transaction {
    pub fn new() -> Transaction {
        Transaction {
            error: false,
            queue: vec![],
        }
    }

    pub fn push(&mut self, cmd: Command) {
        self.queue.push(cmd);
    }

    pub fn drain_queue(&mut self) -> std::vec::Drain<Command> {
        self.queue.drain(..)
    }
}
