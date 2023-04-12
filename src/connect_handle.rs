use tokio::sync::mpsc::{self, Sender};

use crate::{
    connector::{LocalContext, ServerContext},
    err::{ErrorCode, MQError, MQResult},
    protocol::{Protocol, ProtocolArgs, ProtocolHeader, ProtocolHeaderType},
    session::{
        AddTopic, Endpoint, Message, RegisterPublisherRequestParam, RegisterSubscribeRequestParam,
        SessionManagerRequest, SessionManagerResponse, SessionRequest, SessionResponse, Value,
        CHANNEL_BUFFER_LENGTH,
    },
    utils::{
        channel::{ChannelUtil, CHANNEL_BUFF_LEN},
        convert::BuffUtil,
    },
};

// 处理接收到的链接
pub async fn handle_connect(
    mut server_context: ServerContext,
    mut local_context: LocalContext,
) -> MQResult<()> {
    let proto = Protocol::read(&mut local_context.stream).await?;
    // let proto_args = ProtoArgs::from_
    let proto_head_type = proto.header.p_type.clone();
    let mut handle = ConnectorHandler::new(server_context, local_context, proto);
    debug!("recv head type: {:?}", &proto_head_type);
    match proto_head_type {
        crate::protocol::ProtocolHeaderType::Null => todo!(),
        crate::protocol::ProtocolHeaderType::Disconnect => todo!(),
        crate::protocol::ProtocolHeaderType::RegisterPublisher => {
            // 将该注册到session manager中，注册为生产者。
            handle.publish().await?;
            // 相应注册成功消息
            // 监听接收新数据，并将数据发送到session
        }
        crate::protocol::ProtocolHeaderType::RegisterSubscriber => {
            // 将该链接注册到session manager中，注册为消费者
            // 响应注册成功消息
            // 接收session发送的数据，并将数据回发到client_context.stream中。
            handle.subscribe().await?;
        }
        _ => {
            warn!("not support protocol head type. {:?}", proto_head_type)
        }
    }
    Ok(())
}

// 消息body类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolBodyRegisterPublisher {
    pub topic: String,
    pub name: String,
}

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct ProtocolBodyRegisterSubscribe {
//     pub topic: String,
//     pub name: String,
// }

// struct SessionClient {
//     // 向session发送请求
//     pub session_tx: SessionRequest,
//     // session向客户端回发响应数据
//     pub client_tx: SessionResponse,
//     // session客户端接收session响应数据
//     pub client_rx: SessionResponse,
// }

struct ConnectorHandler {
    server_context: ServerContext,
    local_context: LocalContext,
    protocol: Protocol,
}

impl ConnectorHandler {
    fn new(server_context: ServerContext, local_context: LocalContext, protocol: Protocol) -> Self {
        Self {
            server_context,
            local_context,
            protocol,
        }
    }

    // 注册为发布者
    async fn publish(&mut self) -> MQResult<()> {
        // 向session_manager 添加topic，确保主题存在
        // debug!("add topic: {}", self.topic);
        let session_tx = self.request_session_manager_add_topic().await?;
        // 向该主题的session注册为发布者
        info!("register add topic successful.");
        let endpoint = self.register_publisher(&session_tx).await?;
        // 接收客户端数据，将消息发布到session
        info!("register publisher successful.");
        let peer_addr = self
            .local_context
            .stream
            .peer_addr()
            .map_err(|e| MQError::E("get peer addr failed.".to_string()))?
            .to_string();
        loop {
            match self.recv_client_publish_message(&endpoint).await {
                Ok(_) => {}
                Err(e) => {
                    if let MQError::IoError(w) = &e {
                        warn!("[{}] io closed. warn: {}", peer_addr, w);
                        break;
                    };
                    error!("{}", e);
                }
            }
        }
        Ok(())
    }

    // 向session注册为发布者
    async fn register_publisher(&mut self, tx: &Sender<SessionRequest>) -> MQResult<Endpoint> {
        let (mut tx2, mut rx2) = mpsc::channel::<SessionResponse>(CHANNEL_BUFF_LEN);
        let param = RegisterPublisherRequestParam { tx: tx2 };
        let res = ChannelUtil::request::<SessionRequest, SessionResponse>(
            &tx,
            &mut rx2,
            SessionRequest::RegisterPublisher(param),
        )
        .await?;
        match res {
            SessionResponse::Publisher(x) => Ok(x),
            _ => Err(MQError::E(
                "register publish to session failed.".to_string(),
            )),
        }
    }

    // 接收客户端发布的消息
    async fn recv_client_publish_message(&mut self, endpoint: &Endpoint) -> MQResult<()> {
        let protocol = Protocol::read(&mut self.local_context.stream).await?;
        debug!("recv publish message protocol: {:?}", &protocol);
        let head_type = protocol.header.p_type.clone();
        use crate::protocol::ProtocolHeaderType::*;
        let publish_value = match head_type.clone() {
            Null => Value::Null,
            Disconnect => {
                // 断开连接
                return Err(MQError::E("todo: 断开连接".to_string()));
            }
            SendStr => {
                let body = String::from_utf8(protocol.body).map_err(|e| {
                    MQError::ConvertError(format!("buff convert str failed.\n\terror:{}", e))
                })?;
                Value::Str(body)
            }
            SendInt => {
                let num = BuffUtil::buff_to_i32(protocol.body)?;
                Value::Int(num)
            }
            SendFloat => {
                let num = BuffUtil::buff_to_f64(protocol.body)?;
                Value::Float(num)
            }
            SendBytes => Value::Bytes(protocol.body),
            SendBool => {
                let b = BuffUtil::buff_to_bool(protocol.body)?;
                Value::Bool(b)
            }
            _ => {
                return Err(MQError::E(format!(
                    "not supported head type: {:?}",
                    head_type
                )));
            }
        };

        let publish_data = SessionRequest::PublishMessage(Message {
            value: publish_value,
        });
        debug!("ready publish value: {:?}", publish_data);
        endpoint
            .session_tx
            .send(publish_data)
            .await
            .map_err(|e| MQError::E(format!("send failed. error: {}", e)))?;
        Ok(())
    }

    // 请求session_manager, 请求add_topic
    async fn request_session_manager_add_topic(&mut self) -> MQResult<Sender<SessionRequest>> {
        let peer = self
            .local_context
            .stream
            .peer_addr()
            .map_err(|e| {
                MQError::E(format!(
                    "get peer connect failed from local stream.\n\terror: {}",
                    e
                ))
            })?
            .to_string();
        let body_obj =
            serde_json::from_slice::<ProtocolBodyRegisterPublisher>(&self.protocol.body)?;
        let (mut tx1, mut rx1) = mpsc::channel::<SessionManagerResponse>(CHANNEL_BUFFER_LENGTH);

        let add_topic = AddTopic {
            topic: body_obj.topic,
            name: peer,
            tx: tx1.clone(),
        };
        let res = ChannelUtil::request::<SessionManagerRequest, SessionManagerResponse>(
            &self.server_context.tx,
            &mut rx1,
            SessionManagerRequest::AddTopic(add_topic),
        )
        .await?;

        // 接收响应
        // 监听rx，接收session manager回应数据
        debug!("register publisher response: {:?}", &res);
        match res {
            SessionManagerResponse::AddTopicResponse(response) => {
                // 使用endpoint
                if response.code != ErrorCode::Success as i32 {
                    error!("{}", response.msg);
                    return Err(MQError::E("recv add topic response failed.".to_string()));
                }
                Ok(response.tx)
            }
            _ => Err(MQError::E("recv response not match.".to_string())),
        }
    }

    // 注册为订阅者
    async fn subscribe(&mut self) -> MQResult<()> {
        // 向session_manager 添加topic，确保主题存在
        let session_tx = self.request_session_manager_add_topic().await?;
        // 向该主题的session注册为发布者
        let mut endpoint = self.register_subscriber(&session_tx).await?;
        // 接收客户端数据，将消息发布到session
        let peer_addr = self
            .local_context
            .stream
            .peer_addr()
            .map_err(|e| {
                MQError::E(format!(
                    "get peer connect failed from local stream.\n\terror: {}",
                    e
                ))
            })?
            .to_string();
        loop {
            match self.recv_message_to_client(&mut endpoint).await {
                Ok(_) => {}
                Err(e) => {
                    if let MQError::IoError(w) = &e {
                        warn!("[{}] io closed. warn: {}", peer_addr, w);
                        break;
                    };
                    error!("{}", e)
                }
            }
        }
        Ok(())
    }

    // 向session注册为发布者
    async fn register_subscriber(&mut self, tx: &Sender<SessionRequest>) -> MQResult<Endpoint> {
        let (mut tx2, mut rx2) = mpsc::channel::<SessionResponse>(CHANNEL_BUFF_LEN);
        let param = RegisterSubscribeRequestParam { tx: tx2 };

        let res = ChannelUtil::request::<SessionRequest, SessionResponse>(
            tx,
            &mut rx2,
            SessionRequest::RegisterSubscribe(param),
        )
        .await?;
        match res {
            SessionResponse::Subscriber(x) => Ok(x),
            _ => Err(MQError::E(
                "register publish to session failed.".to_string(),
            )),
        }
    }

    // 订阅，接收session发布的消息，并发送给客户端
    async fn recv_message_to_client(&mut self, endpoint: &mut Endpoint) -> MQResult<()> {
        match endpoint.rx.recv().await {
            Some(res) => {
                use SessionResponse::*;
                match res {
                    SessionResponse::BreakSession => todo!(),
                    SessionResponse::ConsumeMessage(msg) => {
                        // let v = msg.value;
                        let value_len = msg.value.len();
                        use ProtocolHeaderType::*;
                        let proto_head_type = match &msg.value {
                            Value::Null => {
                                return Err(MQError::E(format!("value is Null.")));
                            }
                            Value::Bool(_) => RecvBool,
                            Value::Str(_) => RecvStr,
                            Value::Int(_) => RecvInt,
                            Value::Float(_) => RecvFloat,
                            Value::Bytes(_) => RecvBytes,
                        };
                        // let proto_head = ProtocolHeader::new(proto_head_type, 0, value_len as u64);
                        let proto =
                            Protocol::new(proto_head_type, ProtocolArgs::Null, msg.value.to_buff());
                        Protocol::send(&mut self.local_context.stream, proto).await?;
                    }
                    _ => return Err(MQError::E(format!("not supported response."))),
                }
            }
            None => return Err(MQError::E("not recv session manager response.".to_string())),
        }
        Ok(())
    }
}
