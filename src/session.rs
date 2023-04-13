use crate::err::{MQError, MQResult};
use crate::message::Message;
use crate::storage::Store;
use chrono::{DateTime, TimeZone, Utc};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use uuid::Uuid;

pub const CHANNEL_BUFFER_LENGTH: usize = 1024 * 100;

#[derive(Debug)]
pub struct SessionManager {
    // 一个接收消息rx， 接收客户端发来的请求
    pub rx: Receiver<SessionManagerRequest>,
    // 客户端clone该对象，用来向rx发送请求。
    pub tx: Sender<SessionManagerRequest>,
    // 基于topic的消息队列 topic: MessageQueue
    // pub message_queue: HashMap<String, Session>,
    pub session_queue_tx: HashMap<String, Sender<SessionRequest>>,
}

// 添加topic
#[derive(Debug)]
pub struct AddTopic {
    // 主题名
    pub topic: String,
    // 连接名
    pub name: String,
    // 用此发送端，回发数据
    pub tx: Sender<SessionManagerResponse>,
    // 注册角色
}

// 删除topic
#[derive(Debug)]
pub struct RemoveTopic {
    pub topic: String,
    // 用此发送端，回发数据
    pub tx: Sender<SessionManagerResponse>,
}

// 接收到的消息请求
#[derive(Debug)]
pub enum SessionManagerRequest {
    // 消息内容
    // Message,
    // 添加topic
    AddTopic(AddTopic),
    // 移除topic
    RemoveTopic(RemoveTopic),
    // 注册消费者
    // Register
    // 注册生产者
}

#[derive(Debug)]
pub struct AddTopicResponse {
    pub code: i32,
    pub msg: String,
    pub tx: Sender<SessionRequest>,
}

#[derive(Debug)]
pub struct ResponseMsg {
    pub code: i32,
    pub msg: String,
}
#[derive(Debug)]
pub struct RemoveTopicResponse {
    pub code: i32,
    pub msg: String,
    // pub tx: Sender<SessionRequest>,
}

// 相应的消息请求
#[derive(Debug)]
pub enum SessionManagerResponse {
    // 中断链接，移除该消费者、生产者连接
    // BreakSession,
    // topic注册成功，返回数据
    AddTopicResponse(AddTopicResponse),
    // 删除topic
    RemoveTopicResponse(RemoveTopicResponse),
}

#[derive(Debug)]
pub struct RegisterSubscribeRequestParam {
    pub tx: Sender<SessionResponse>,
    pub name: String,
}

#[derive(Debug)]
pub struct RegisterPublisherRequestParam {
    pub tx: Sender<SessionResponse>,
}

#[derive(Debug)]
pub struct RemovePublisherRequestParam {
    pub uuid: String,
    // pub tx: Sender<SessionResponse>
}

#[derive(Debug)]
pub struct RemoveSubscribeRequestParam {
    pub uuid: String,
    // pub tx: Sender<SessionResponse>
}

#[derive(Debug)]
pub enum SessionRequest {
    // 注册消费者
    RegisterSubscribe(RegisterSubscribeRequestParam),
    // 注册生产者
    RegisterPublisher(RegisterPublisherRequestParam),
    // 移除消费者
    RemoveSubscribe(RemoveSubscribeRequestParam),
    // 移除生产者
    RemovePublisher(RemovePublisherRequestParam),
    // 订阅者消费数据
    // ConsumeMessage(Message),
    // 生产数据
    PublishMessage(Message),
    // 中断
    Break,
}

#[derive(Debug)]
pub enum SessionResponse {
    BreakSession,
    // 注册为订阅者，响应的消息端
    Subscriber(Endpoint),
    // 注册为发布者后，响应的消息端
    Publisher(Endpoint),
    Result(ResponseMsg),

    // 将数据发送到消费者
    ConsumeMessage(Message),
}

// 消费者、生产者 相应端，在session处进行接收处理
#[derive(Debug)]
pub struct SessionEndpoint {
    // pub name: String,
    pub uuid: String,
    // session向消费者发送响应数据
    pub tx: Sender<SessionResponse>,
    // 接收请求端发送的消息请求, 使用session中的rx
    // pub request_rx: Receiver<TransferMessageRequest>,
}

// 消费者，生产者， 使用端
#[derive(Debug)]
pub struct Endpoint {
    pub uuid: String,
    // clone给session，用以回发数据。
    pub tx: Sender<SessionResponse>,
    // 接收session回发数据。
    pub rx: Receiver<SessionResponse>,
    // session的tx, 用来向session发送数据
    pub session_tx: Sender<SessionRequest>,
}

// pub struct Publisher {
//     pub uuid: String,
//     // clone给session，用以回发数据。
//     pub tx: Sender<SessionResponse>,
//     // 接收session回发数据。
//     pub rx: Receiver<SessionResponse>,
// }
//
// 每个会话
#[derive(Debug)]
struct Session {
    // 接收消息
    pub rx: Receiver<SessionRequest>,
    // 发送消息端，clone给使用者
    pub tx: Sender<SessionRequest>,
    // 消费者
    pub subscribers: Vec<SessionEndpoint>,
    // 生产者
    pub Publisheres: Vec<SessionEndpoint>,

    // 数据存储
    pub store: Store,

    // 是否正在向订阅者发送数据
    pub b_publish: bool,
}

impl Session {
    pub fn new() -> Session {
        let (tx1, rx1) = mpsc::channel::<SessionRequest>(CHANNEL_BUFFER_LENGTH);
        Session {
            rx: rx1,
            tx: tx1,
            subscribers: vec![],
            Publisheres: vec![],
            store: Store::new(),
            b_publish: false,
        }
    }

    fn create_endpoint(&self) -> MQResult<(Endpoint, SessionEndpoint)> {
        let (tx1, rx1) = mpsc::channel::<SessionResponse>(CHANNEL_BUFFER_LENGTH);
        let uuid = Uuid::new_v4();
        let endpoint = Endpoint {
            uuid: uuid.to_string(),
            tx: tx1.clone(),
            rx: rx1,
            session_tx: self.tx.clone(),
        };
        let session_endpoint = SessionEndpoint {
            uuid: uuid.to_string(),
            tx: tx1,
        };
        Ok((endpoint, session_endpoint))
    }

    // 创建一个消费者
    pub fn create_subscriber(&mut self) -> MQResult<Endpoint> {
        let (endpoint, session_endpoint) = self.create_endpoint()?;
        self.subscribers.push(session_endpoint);
        Ok(endpoint)
    }

    // 创建一个生产者
    pub fn create_publisher(&mut self) -> MQResult<Endpoint> {
        let (endpoint, session_endpoint) = self.create_endpoint()?;
        self.Publisheres.push(session_endpoint);
        Ok(endpoint)
    }

    // 移除一个消费者
    pub fn remove_subscriber(&mut self, uuid: String) {
        self.subscribers.retain(|x| x.uuid != uuid);
    }

    // 移除一个生产者，
    // TODO: 移除时向消费端发送消息
    pub fn remove_publisher(&mut self, uuid: String) {
        self.Publisheres.retain(|x| x.uuid != uuid);
    }

    pub async fn deal_request(&mut self, mut request: SessionRequest) -> MQResult<()> {
        use SessionRequest::*;
        match request {
            RegisterSubscribe(x) => {
                self.recv_register_subscribe(x).await?;
            }
            RegisterPublisher(x) => {
                self.recv_register_publish(x).await?;
            }
            RemoveSubscribe(x) => {
                self.remove_subscriber(x.uuid.clone());
                // x.tx.send(SessionResponse::Result(ResponseMsg{code: 0, msg: e.to_string()})).await?;
            }
            RemovePublisher(x) => {
                self.remove_publisher(x.uuid.clone());
                // x.tx.send(SessionResponse::Result(ResponseMsg{code: 0, msg: e.to_string()})).await?;
            }
            // ConsumeMessage(msg) => {
            //     // 消费数据
            // }
            PublishMessage(mut msg) => {
                // 生产数据, 向所有在线消费者发送消息
                self.recv_publish_msg(msg).await;
            }
            _ => {
                error!("no support message.");
            }
        }
        Ok(())
    }

    pub async fn listen(&mut self) -> MQResult<()> {
        while let Some(mut request) = self.rx.recv().await {
            debug!("session get request:\n{:?}", &request);
            match self.deal_request(request).await {
                Ok(_) => {}
                Err(e) => {
                    error!("do manager request failed. {}", e.to_string())
                }
            }
        }
        Ok(())
    }

    // 注册订阅者
    pub async fn recv_register_subscribe(
        &mut self,
        x: RegisterSubscribeRequestParam,
    ) -> MQResult<()> {
        info!("register subscribe: {}", &x.name);
        let res = match self.create_subscriber() {
            Ok(endpoint) => SessionResponse::Subscriber(endpoint),
            Err(e) => SessionResponse::Result(ResponseMsg {
                code: 1,
                msg: e.to_string(),
            }),
        };
        x.tx.send(res).await.map_err(|e| {
            MQError::E(format!(
                "send register subscriber response failed. e: {}",
                e
            ))
        })?;
        // 触发一次订阅数据的获取
        self.send_msg_to_subscribers().await;
        Ok(())
    }

    // 注册发布者
    pub async fn recv_register_publish(
        &mut self,
        x: RegisterPublisherRequestParam,
    ) -> MQResult<()> {
        let res = match self.create_publisher() {
            Ok(endpoint) => SessionResponse::Publisher(endpoint),
            Err(e) => SessionResponse::Result(ResponseMsg {
                code: 1,
                msg: e.to_string(),
            }),
        };
        x.tx.send(res).await.map_err(|e| {
            MQError::E(format!(
                "send register Publisher response failed.\n\terror: {}",
                e
            ))
        })?;
        Ok(())
    }

    // 接收到发布的消息
    pub async fn recv_publish_msg(&mut self, mut msg: Message) -> MQResult<()> {
        info!("recv publish message: {}", &msg);
        let current_time = Utc::now();
        msg.send_time = Utc::now();
        // 将数据发送到数据中心（内存、持久化引擎）， 然后再触发订阅者拉取数据。
        self.store.push(msg);
        if self.subscribers.len() > 0 {
            self.send_msg_to_subscribers().await;
        }
        Ok(())
    }

    pub async fn send_msg_to_subscriber(&mut self) -> MQResult<()> {
        while let Some(msg) = self.store.pop() {
            for subscriber in self.subscribers.iter() {
                subscriber
                    .tx
                    .send(SessionResponse::ConsumeMessage(msg.clone()))
                    .await
                    .map_err(|e| {
                        MQError::E(format!("send message to subscriber failed. e: {}", e))
                    })?;
            }
        }
        Ok(())
    }

    // 获取缓冲区数据并将数据发送给注册该主题的所有订阅者
    pub async fn send_msg_to_subscribers(&mut self) {
        if self.b_publish {
            // 当前有其他操作正在发送数据，不再重复触发
            warn!("send msg to subscribe, other thread occupied.");
            return;
        }
        self.b_publish = true;
        match self.send_msg_to_subscriber().await {
            Ok(_) => (),
            Err(e) => {
                error!("send_msg_to_subscribers failed: {}", e);
            }
        }
        self.b_publish = false;
    }
}

impl SessionManager {
    pub fn new() -> SessionManager {
        let (tx1, rx1) = mpsc::channel::<SessionManagerRequest>(CHANNEL_BUFFER_LENGTH);
        SessionManager {
            rx: rx1,
            tx: tx1,
            session_queue_tx: HashMap::new(),
        }
    }

    // 添加一个主题, 返回一个向session发送的tx请求
    fn add_topic(&mut self, topic: String) -> MQResult<Session> {
        if self.session_queue_tx.contains_key(&topic) {
            return Err(MQError::E(format!("the topic already exists")));
        }
        let mut session = Session::new();
        let tx = session.tx.clone();
        self.session_queue_tx.insert(topic, tx);
        Ok(session)
        // Ok(())
    }

    // 移除一个主题
    pub async fn remove_topic(&mut self, topic: String) -> MQResult<()> {
        match self.session_queue_tx.get_mut(&topic) {
            Some(session_tx) => {
                // 向所有连接发送主题下线通知，并删除所有主题
                let r = session_tx.send(SessionRequest::Break).await.map_err(|e| {
                    MQError::E(format!("send remove topic result failed. e: {}", e))
                })?;
                Ok(())
            }
            None => Err(MQError::E(format!("the topic does not exist"))),
        }
    }

    pub async fn run(&mut self) {
        // 判断是否退出
        // Receive the data
        while let Some(session_manager_request) = self.rx.recv().await {
            debug!("session manager request: {:?}", &session_manager_request);
            match self.deal_manager_request(session_manager_request).await {
                Ok(_) => {}
                Err(e) => {
                    error!("do manager request failed. {}", e.to_string())
                }
            }
        }
    }

    pub async fn deal_manager_request(&mut self, request: SessionManagerRequest) -> MQResult<()> {
        match request {
            SessionManagerRequest::AddTopic(x) => {
                // 将该数据返回给请求者
                let topic = x.topic.clone();
                debug!("session manager get add topic request: {:?}", &topic);
                // topic存在则不添加topic，直接返回session
                let res = match self.session_queue_tx.get(&topic) {
                    Some(tx) => {
                        // 该topic存在
                        AddTopicResponse {
                            code: 0,
                            msg: "".to_string(),
                            tx: tx.clone(),
                        }
                    }
                    None => {
                        // 无该topic的session
                        let mut session = self.add_topic(topic.clone())?;
                        let res = AddTopicResponse {
                            code: 0,
                            msg: "".to_string(),
                            tx: session.tx.clone(),
                        };
                        // 将该session放在线程中监听。
                        // 创建异步任务并将其放入后台队列。
                        tokio::spawn(async {
                            let mut s = session;
                            match s.listen().await {
                                Ok(()) => {}
                                Err(e) => {
                                    error!("session listen failed. {}", e.to_string())
                                }
                            }
                            // TODO：退出后，管理端需要移除该topic
                            // self.remove_topic(topic)?;
                        });
                        res
                    }
                };

                debug!("response add topic result:{:?}", res);
                x.tx.send(SessionManagerResponse::AddTopicResponse(res))
                    .await
                    .map_err(|e| {
                        MQError::E(format!("response add topic result failed. e: {}", e))
                    })?;
            }
            // 移除topic
            SessionManagerRequest::RemoveTopic(x) => {
                let res = match self.session_queue_tx.get(&x.topic) {
                    Some(tx) => {
                        // TODO： 向session发送消息，断开所有消费者、生产者
                        // 该topic存在
                        RemoveTopicResponse {
                            code: 0,
                            msg: "".to_string(),
                        }
                    }
                    None => {
                        RemoveTopicResponse {
                            code: 1,
                            msg: format!("the topic does not exist: {}", &x.topic),
                            // tx: tx.clone(),
                        }
                    }
                };
                match x
                    .tx
                    .send(SessionManagerResponse::RemoveTopicResponse(res))
                    .await
                {
                    Ok(t) => {}
                    Err(e) => {
                        error!("send remove topic result failed. e: {}", e);
                        // Err(MQError::E(format!("send remove topic result failed. e: {}", e)))
                    }
                }
                self.remove_topic(x.topic).await?;
            }
        }
        Ok(())
    }
}
