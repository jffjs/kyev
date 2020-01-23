use crate::command::Command;

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

    pub fn enqueue(&mut self, cmd: Command) {
        self.queue.push(cmd);
    }
}
