use chrono::Utc;
use tokio::net::TcpStream;

use crate::{
    connect_handle::ProtocolBodyRegisterQueue,
    err::{MQError, MQResult},
    message::{Message, ValueType},
    protocol::{Protocol, ProtocolArgs, ProtocolHeaderType, PullRequest},
    session::ResponseResult,
};

pub struct ClientInstant {
    pub server: String,
    pub stream: Option<TcpStream>,
}

impl ClientInstant {
    pub fn new(server_addr: String) -> Self {
        Self {
            server: server_addr,
            stream: None,
        }
    }

    pub async fn register_publisher(&mut self, topic: &str, name: &str) {
        info!("connect to server: {}", &self.server);
        let mut stream = TcpStream::connect(&self.server.as_str()).await.unwrap();
        let body = ProtocolBodyRegisterQueue {
            topic: String::from(topic),
            name: name.to_string(),
        };
        let subscribe_proto = Protocol::new(
            ProtocolHeaderType::RegisterPublisher,
            ProtocolArgs::Null,
            serde_json::to_vec::<ProtocolBodyRegisterQueue>(&body).unwrap(),
        );
        Protocol::send(&mut stream, subscribe_proto).await.unwrap();
        let res = Protocol::read(&mut stream).await.unwrap();
        match res.header.p_type {
            ProtocolHeaderType::RegisterPublisherRes => {
                let response = serde_json::from_slice::<ResponseResult<i32>>(&res.body).unwrap();
                if response.is_success() {
                    info!("register publish successfully");
                    self.stream = Some(stream);
                } else {
                    error!("register publish failed");
                }
            }
            _ => {
                error!("register publish recv head type not match.");
            }
        }
    }

    pub async fn register_subscriber(&mut self, topic: &str, name: &str) {
        info!("connect to server: {}", &self.server);
        let mut stream = TcpStream::connect(&self.server).await.unwrap();
        let body = ProtocolBodyRegisterQueue {
            topic: String::from(topic),
            name: name.to_string(),
        };
        // 发送订阅请求
        let subscribe_proto = Protocol::new(
            ProtocolHeaderType::RegisterSubscriber,
            ProtocolArgs::Null,
            serde_json::to_vec::<ProtocolBodyRegisterQueue>(&body).unwrap(),
        );
        Protocol::send(&mut stream, subscribe_proto).await.unwrap();
        let res = Protocol::read(&mut stream).await.unwrap();
        match res.header.p_type {
            ProtocolHeaderType::RegisterSubscriberRes => {
                let response = serde_json::from_slice::<ResponseResult<i32>>(&res.body).unwrap();
                if response.is_success() {
                    info!("register subscribe successfully");
                    self.stream = Some(stream);
                } else {
                    error!("register subscribe failed");
                }
            }
            _ => {
                error!("register subscribe recv head type not match.");
            }
        }
    }

    // 推送一个32位整数
    pub async fn push_int(&mut self, number: i32) -> MQResult<()> {
        self.push_value(ValueType::Int, number.to_ne_bytes().to_vec())
            .await
    }

    // 推送一个64位浮点数
    pub async fn push_float(&mut self, number: f64) -> MQResult<()> {
        self.push_value(ValueType::Float, number.to_ne_bytes().to_vec())
            .await
    }

    // 推送一个字节流
    pub async fn push_bytes(&mut self, buff: Vec<u8>) -> MQResult<()> {
        self.push_value(ValueType::Bytes, buff).await
    }

    // 推送字符串
    pub async fn push_str(&mut self, data: String) -> MQResult<()> {
        self.push_value(ValueType::Str, data.as_bytes().to_vec())
            .await
    }

    // 推送数据
    async fn push_value(&mut self, value_type: ValueType, buff: Vec<u8>) -> MQResult<()> {
        let current = Utc::now();
        let message = Message {
            // create_time: current.clone(),
            // send_time: current.clone(),
            // recv_time: current.clone(),
            value_type,
            value_length: buff.len() as u64,
            value: buff,
            create_timestamp: 0,
            fetch_timestamp: 0,
        };
        self.push_msg(message).await
    }

    // 推送一个消息
    async fn push_msg(&mut self, value: Message) -> MQResult<()> {
        match self.stream {
            Some(ref mut stream) => {
                let proto = Protocol::new(
                    ProtocolHeaderType::PushMessage,
                    ProtocolArgs::Null,
                    value.into(),
                );
                info!("publish protocol: {:?}", &proto);
                // 无响应
                Protocol::send(stream, proto).await
            }
            None => {
                panic!("need register publish.");
            }
        }
    }

    // 拉取数据,number表示等待拉取的数据数量
    pub async fn pull(&mut self, number: u32) -> MQResult<Vec<Message>> {
        if number == 0 {
            return Err(MQError::E(
                "the number param must more than zero".to_string(),
            ));
        }
        match self.stream {
            Some(ref mut stream) => {
                let pull_message_request_body = PullRequest { number };
                // 发送一个拉数据请求
                let proto = Protocol::new(
                    ProtocolHeaderType::PullMessage,
                    ProtocolArgs::Null,
                    serde_json::to_vec(&pull_message_request_body).unwrap(),
                );
                Protocol::send(stream, proto).await?;
                // 监听socket，等待返回。
                let result_proto = Protocol::read(stream).await?;
                let head_type = result_proto.header.p_type.clone();
                info!("pull response head type: {:?}", &head_type);
                match head_type {
                    ProtocolHeaderType::PullMessageRes => {
                        serde_json::from_slice::<Vec<Message>>(&result_proto.body).map_err(|e| {
                            MQError::ConvertError(
                                "convert pull messages failed from protocol body.".to_string(),
                            )
                        })
                    }
                    _ => {
                        return Err(MQError::E(
                            "the pull message response must be a pull message response".to_string(),
                        ))
                    }
                }
            }
            None => panic!("need register subscribe."),
        }
    }

    async fn subscribe(&mut self, topic: String) {
        match self.stream {
            Some(ref mut stream) => {
                while let protocol = Protocol::read(stream).await.unwrap() {
                    // debug!("recv publish message: {:?}", protocol);
                    let head_type = protocol.header.p_type.clone();
                    match head_type {
                        ProtocolHeaderType::PullMessage => {
                            let message = Message::try_from(protocol.body.clone()).unwrap();
                            let value = message.value;
                            match message.value_type {
                                ValueType::Str => {
                                    info!(
                                        "[topic-{}] [create: {}, fetch: {}] recv string: {}",
                                        &topic,
                                        message.create_timestamp,
                                        message.fetch_timestamp,
                                        String::from_utf8_lossy(&value)
                                    );
                                }
                                ValueType::Null => todo!(),
                                ValueType::Bool => todo!(),
                                ValueType::Int => {
                                    let buff: [u8; 4] = value[0..4].try_into().unwrap();
                                    info!(
                                        "[topic-{}] [create: {}, recv: {}] recv int: {}",
                                        &topic,
                                        message.create_timestamp,
                                        message.fetch_timestamp,
                                        i32::from_ne_bytes(buff)
                                    );
                                }
                                ValueType::Float => {
                                    let buff: [u8; 8] = value[0..8].try_into().unwrap();
                                    info!(
                                        "[topic-{}] [create: {}, recv: {}] recv float: {}",
                                        &topic,
                                        message.create_timestamp,
                                        message.fetch_timestamp,
                                        f64::from_ne_bytes(buff)
                                    );
                                }
                                ValueType::Bytes => {
                                    info!(
                                        "[topic-{}] [create: {}, recv: {}] recv byte: {:?}",
                                        &topic,
                                        message.create_timestamp,
                                        message.fetch_timestamp,
                                        &value
                                    );
                                }
                            }
                        }
                        _ => {
                            error!(
                                "recv data. head type not match PullMessage. head type: {:?}",
                                &head_type
                            );
                            break;
                        }
                    }
                }
            }
            None => {
                panic!("need register as subscriber.")
            }
        }

        // 接收发布的数据
    }
}
