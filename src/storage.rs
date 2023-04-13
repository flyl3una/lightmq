use crate::message::Message;

// 存储数据
#[derive(Debug)]
pub struct Store {
    pub queue: Vec<Message>,
}

impl Store {
    pub fn new() -> Store {
        Store { queue: Vec::new() }
    }

    pub fn push(&mut self, message: Message) {
        self.queue.push(message);
    }

    pub fn pop(&mut self) -> Option<Message> {
        self.queue.pop()
    }
}
