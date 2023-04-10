use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc;
use crate::err::{MQResult, MQError};
use uuid::Uuid;

const CHANNEL_BUFFER_LENGTH: usize = 1024;

struct SessionManager {
    // 一个接收消息rx， 接收客户端发来的请求
    pub rx: Receiver<SessionManagerRequest>,
    // 客户端clone该对象，用来向rx发送请求。
    pub tx: Sender<SessionManagerRequest>,
    // 基于topic的消息队列 topic: MessageQueue
    pub message_queue: HashMap<String, Session::<Message>>,
}

pub enum Value {
    Str(String),
    Int(i32),
    Float(f64),
    // Bytes()
}

// 与connector交互数据的消息
pub struct Message {
    // 消息主题
    pub topic: String,
    // 消息内容
    pub value: Value,
}

// 添加topic
pub struct AddTopic {
    // 主题名
    pub topic: String,
    // 连接名
    pub name: String,
    // 用此发送端，回发数据
    pub tx: Sender<SessionManagerResponse>,
}

// 删除topic
pub struct RemoveTopic {
    pub topic: String,
    // 用此发送端，回发数据
    pub tx: Sender<SessionManagerResponse>,
}

// 接收到的消息请求
pub enum SessionManagerRequest {
    // 消息内容
    // Message,
    // 添加topic
    AddTopic,
    // 移除topic
    RemoveTopic
}

// 相应的消息请求
pub enum SessionManagerResponse {
    // 中断链接，移除该消费者、生产者连接
    BreakSession,
}

// 消费者、生产者 请求端，在connector 处进行使用
pub struct RequestEndpoint<T> {
    // pub name: String,
    pub uuid: String,
    // 接收session返回的消息相应
    pub response_rx: Receiver<T>,
    // 向session发送消息请求， response_rx在session里， 该值由session的tx克隆而来
    pub request_tx: Sender<T>,

}

// 消费者、生产者 相应端，在session处进行接收处理
pub struct ResponseEndpoint<T> {
    // pub name: String,
    pub uuid: String,
    // session向消费者发送响应数据
    pub response_tx: Sender<T>,
    // 接收请求端发送的消息请求, 使用session中的rx
    // pub request_rx: Receiver<TransferMessageRequest>,
}
// 每个会话
struct Session<M> {
    // 接收消息
    pub rx: Receiver<M>,
    // 发送消息端，clone给使用者
    pub tx: Sender<M>,
    // 消费者
    pub consumers: Vec<ResponseEndpoint::<M>>,
    // 生产者
    pub producers: Vec<ResponseEndpoint::<M>>,

}

impl <M> Session<M> {
    pub fn new() -> Session<T> {
        let (tx1, rx1) = mpsc::channel::<Message>(CHANNEL_BUFFER_LENGTH);
        Session::<Message>{rx: rx1, tx: tx1, consumers: vec![], producers: vec![] }
    }

    fn create_endpoint(&self, topic: String) -> MQResult<(RequestEndpoint::<M>, ResponseEndpoint::<M>)> {
        let (tx1, rx1) = mpsc::channel::<M>(CHANNEL_BUFFER_LENGTH);
        let uuid = Uuid::new_v4();
        let request_endpoint = RequestEndpoint{
            uuid: uuid.to_string(),
            response_rx: rx1,
            request_tx: self.tx.clone(),
        };
        let response_endpoint = ResponseEndpoint {
            uuid: uuid.to_string(),
            response_tx: tx1,
        };
        Ok((request_endpoint, response_endpoint))
    }

    // 创建一个消费者
    pub fn create_consumer(&mut self, topic: String) -> MQResult<RequestEndpoint::<M>> {
        let (request_endpoint, response_endpoint) = self.create_endpoint(topic)?;
        self.consumers.push(response_endpoint);
        Ok(request_endpoint)
    }

    // 创建一个生产者
    pub fn create_producer(&mut self, topic: String) -> MQResult<RequestEndpoint::<M>> {
        let (request_endpoint, response_endpoint) = self.create_endpoint(topic)?;
        self.producers.push(response_endpoint);
        Ok(request_endpoint)
    }

    // 移除一个消费者
    pub fn remove_consumer(&mut self, name: String) -> MQResult<()> {
        self.consumers.retain(|x| x.name != name);
        Ok(())
    }

    // 移除一个生产者，
    // TODO: 移除时向消费端发送消息
    pub fn remove_producer(&mut self, name: String) -> MQResult<()> {
        self.producers.retain(|x| x.name != name);
        Ok(())
    }
}

impl SessionManager {
    pub fn new() -> SessionManager {
        let (tx1, rx1) = mpsc::channel::<SessionManagerRequest>(CHANNEL_BUFFER_LENGTH);
        SessionManager{ rx: rx1, tx: tx1, message_queue: HashMap::new()}
    }

    // 添加一个主题
    pub fn add_topic(&mut self, topic: String) -> MQResult<()>{
        if self.message_queue.contains_key(&topic) {
            return Err(MQError::E(format!("the topic already exists")))
        }
        let mut session = Session::new();
        self.message_queue.insert(topic, session);
        Ok(())
    }
    
    // 移除消费者
    pub fn create_consumer(&mut self, topic: String) -> MQResult<RequestEndpoint> {
        match self.message_queue.get_mut(&topic) {
            Some(session) => {
                session.create_consumer(topic)
            },
            None => Err(MQError::E(format!("the topic does not exist")))
        }
    }

    // 移除消费者
    pub fn create_producer(&mut self, topic: String) -> MQResult<RequestEndpoint> {
        match self.message_queue.get_mut(&topic) {
            Some(session) => {
                session.create_producer(topic)
            },
            None => Err(MQError::E(format!("the topic does not exist")))
        }
    }

    // 移除一个主题
    pub fn remove_topic(&mut self, topic: String) -> MQResult<()> {
        match self.message_queue.get_mut(&topic) {
            Some(session) => {
                // 向所有连接发送主题下线通知，并删除所有主题
                for consumer in session.consumers.iter() {
                    consumer.response_tx.send(TransferMessageResponse::BreakSession).await;
                }
                for producer in session.producers.iter() {
                    producer.response_tx.send(TransferMessageResponse::BreakSession).await;
                }
                Ok(())
            },
            None => Err(MQError::E(format!("the topic does not exist")))
        }
    }

    pub async fn poll_topic_channel(&mut self) {
        loop {

        }
    }
}
