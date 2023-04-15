use std::vec;

use chrono::Utc;
use tokio::{
    sync::mpsc::{self, Sender},
    time::{sleep, Duration, Instant},
};

use crate::{
    connector::{LocalContext, ServerContext},
    err::{ErrorCode, MQError, MQResult},
    protocol::{Protocol, ProtocolArgs, ProtocolHeader, ProtocolHeaderType},
    session::{
        AddTopic, Endpoint, PullMessageRequestParam, PushMessageRequestParam,
        RegisterPublisherRequestParam, RegisterSubscribeRequestParam, ResponseResult,
        SessionManagerRequest, SessionManagerResponse, SessionRequest, SessionResponse,
        CHANNEL_BUFFER_LENGTH,
    },
    utils::{
        channel::{ChannelUtil, CHANNEL_BUFF_LEN},
        convert::BuffUtil,
    },
};
use crate::{message::Message, protocol::PullRequest};

pub const PULL_SLEEP_SECONDS: u64 = 3;

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
pub struct ProtocolBodyRegisterQueue {
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
        let session_tx = self.request_session_manager_add_topic().await?;
        // 向该主题的session注册为发布者
        info!("register add topic successful.");
        let peer_addr = self
            .local_context
            .stream
            .peer_addr()
            .map_err(|e| MQError::E("get peer addr failed.".to_string()))?
            .to_string();

        let mut endpoint = self.register_publisher(&session_tx).await?;
        // 接收客户端数据，将消息发布到session
        loop {
            match self.recv_push_request(&mut endpoint).await {
                Ok(_) => {}
                Err(e) => {
                    if let MQError::IoError(x) = &e {
                        warn!("[{}] io closed. {}", peer_addr, x);
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
            SessionResponse::Publisher(x) => {
                info!("register publisher successful. uuid: {}", &x.uuid);
                let res_body = ResponseResult::<i32>::new(0, "".to_string());
                let response_proto = Protocol::new(
                    ProtocolHeaderType::RegisterPublisherRes,
                    ProtocolArgs::Null,
                    serde_json::to_vec(&res_body).map_err(|e| {
                        MQError::ConvertError(format!(
                            "response result convert json failed. error:{}",
                            e
                        ))
                    })?,
                );
                Protocol::send(&mut self.local_context.stream, response_proto).await?;
                Ok(x)
            }
            _ => {
                let res_body =
                    ResponseResult::<i32>::new(1, "recv session response error.".to_string());
                let response_proto = Protocol::new(
                    ProtocolHeaderType::RegisterPublisherRes,
                    ProtocolArgs::Null,
                    serde_json::to_vec(&res_body).map_err(|e| {
                        MQError::ConvertError(format!(
                            "response result convert json failed. error:{}",
                            e
                        ))
                    })?,
                );
                Protocol::send(&mut self.local_context.stream, response_proto).await?;
                Err(MQError::E(
                    "register publish to session failed.".to_string(),
                ))
            }
        }
    }

    // 接收客户端发布的消息
    async fn recv_push_request(&mut self, endpoint: &mut Endpoint) -> MQResult<()> {
        let protocol = Protocol::read(&mut self.local_context.stream).await?;
        let current_time = Utc::now();
        debug!("recv publish message protocol: {:?}", &protocol);
        use crate::protocol::ProtocolHeaderType::*;
        debug!("the push message head type num: {}", PushMessage as u16);
        match protocol.header.p_type.clone() {
            PushMessage => {
                let mut message = Message::try_from(protocol.body)?;
                // message.recv_time = current_time;
                let push_message_param = PushMessageRequestParam {
                    uuid: endpoint.uuid.to_string(),
                    message,
                };
                let push_data = SessionRequest::PushMessage(push_message_param);
                debug!("ready publish value: {:?}", push_data);
                let res = ChannelUtil::request::<SessionRequest, SessionResponse>(
                    &mut endpoint.session_tx,
                    &mut endpoint.rx,
                    push_data,
                )
                .await?;
                match res {
                    SessionResponse::Result(result) => {
                        if !result.is_success() {
                            MQError::E(result.error().to_string());
                        }
                        info!("push message successfully.");
                    }
                    _ => {
                        error!("push data not suuport response type.")
                    }
                }
                Ok(())
            }
            _ => Err(MQError::E(format!(
                "only support push message protocol head type. current head type: {}",
                protocol.header.p_type.clone() as u8
            ))),
        }
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
        let body_obj = serde_json::from_slice::<ProtocolBodyRegisterQueue>(&self.protocol.body)?;
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
        // 向该主题的session注册为发布者
        let mut endpoint = self.register_subscriber(&session_tx).await?;

        // Protocol::send(&mut self.local_context.stream, response_proto).await?;
        // 接收客户端数据，将消息发布到session

        loop {
            match self.recv_pull_request(&mut endpoint).await {
                Ok(_) => {}
                Err(e) => {
                    if let MQError::IoError(w) = &e {
                        warn!("[{}] io closed. {}", peer_addr, w);
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
        let (mut tx2, mut rx2) = mpsc::channel::<SessionResponse>(CHANNEL_BUFF_LEN);
        let param = RegisterSubscribeRequestParam {
            tx: tx2,
            name: peer,
        };

        let res = ChannelUtil::request::<SessionRequest, SessionResponse>(
            tx,
            &mut rx2,
            SessionRequest::RegisterSubscribe(param),
        )
        .await?;
        match res {
            SessionResponse::Subscriber(x) => {
                info!("register subscribe successful. uuid: {}", &x.uuid);
                let res_body = ResponseResult::<i32>::new(0, "".to_string());
                let response_proto = Protocol::new(
                    ProtocolHeaderType::RegisterSubscriberRes,
                    ProtocolArgs::Null,
                    serde_json::to_vec(&res_body).map_err(|e| {
                        MQError::ConvertError(format!(
                            "response result convert json failed. error:{}",
                            e
                        ))
                    })?,
                );
                Protocol::send(&mut self.local_context.stream, response_proto).await?;
                Ok(x)
            }
            _ => {
                let res_body =
                    ResponseResult::<i32>::new(1, "recv session response error.".to_string());
                let response_proto = Protocol::new(
                    ProtocolHeaderType::RegisterSubscriberRes,
                    ProtocolArgs::Null,
                    serde_json::to_vec(&res_body).map_err(|e| {
                        MQError::ConvertError(format!(
                            "response result convert json failed. error:{}",
                            e
                        ))
                    })?,
                );
                Protocol::send(&mut self.local_context.stream, response_proto).await?;
                Err(MQError::E(
                    "register subscriber to session failed.".to_string(),
                ))
            }
        }
    }

    // 订阅，接收session发布的消息，并发送给客户端
    // async fn recv_message_to_client(&mut self, endpoint: &mut Endpoint) -> MQResult<()> {
    //     match endpoint.rx.recv().await {
    //         Some(mut res) => {
    //             use SessionResponse::*;
    //             match res {
    //                 SessionResponse::BreakSession => todo!(),
    //                 SessionResponse::PullMessage(mut msg) => {
    //                     let current_time = Utc::now();
    //                     msg.recv_time = current_time;
    //                     let value_len = msg.value.len();
    //                     let proto = Protocol::new(
    //                         ProtocolHeaderType::PullMessage,
    //                         ProtocolArgs::Null,
    //                         msg.into(),
    //                     );
    //                     Protocol::send(&mut self.local_context.stream, proto).await?;
    //                 }
    //                 _ => return Err(MQError::E(format!("not supported response."))),
    //             }
    //         }
    //         None => return Err(MQError::E("not recv session manager response.".to_string())),
    //     }
    //     Ok(())
    // }

    // 接收客户端发布的消息
    async fn recv_pull_request(&mut self, endpoint: &mut Endpoint) -> MQResult<()> {
        let protocol = Protocol::read(&mut self.local_context.stream).await?;

        debug!("recv publish message protocol: {:?}", &protocol);
        use crate::protocol::ProtocolHeaderType::*;
        match protocol.header.p_type.clone() {
            PullMessage => {
                let pull_request = serde_json::from_slice::<PullRequest>(&protocol.body)?;
                self.recv_pull_response_to_client(endpoint, pull_request)
                    .await
            }
            _ => Err(MQError::E(format!(
                "only support pull message protocol head type. current head type: {}",
                protocol.header.p_type.clone() as u8
            ))),
        }
    }

    // 向session拉去消息并发送给客户端
    async fn recv_pull_response_to_client(
        &mut self,
        endpoint: &mut Endpoint,
        pull_request: PullRequest,
    ) -> MQResult<()> {
        let current_time = Utc::now();

        // 是否需要重新发送
        let mut b_pull = true;
        while b_pull {
            b_pull = false;
            let param = PullMessageRequestParam {
                // TODO: 拉取数量还需要确认实现罗技
                number: pull_request.number,
                uuid: endpoint.uuid.to_string(),
            };

            let pull_request_param = SessionRequest::PullMessage(param);
            debug!("ready pull value: {:?}", pull_request_param);
            let response = ChannelUtil::request::<SessionRequest, SessionResponse>(
                &mut endpoint.session_tx,
                &mut endpoint.rx,
                pull_request_param,
            )
            .await?;
            match response {
                SessionResponse::PullMessage(result) => {
                    if !result.is_success() {
                        return Err(MQError::E(result.error().to_string()));
                    }
                    match result.data {
                        Some(mut data) => {
                            // TODO: 此处可以优化，不传json数据，直接返回二进制，可以优化时间
                            for msg in data.iter_mut() {
                                msg.fetch_timestamp = current_time.timestamp_nanos();
                            }
                            let proto = Protocol::new(
                                ProtocolHeaderType::PullMessageRes,
                                ProtocolArgs::Null,
                                serde_json::to_vec(&data).map_err(|e| {
                                    MQError::ConvertError(
                                        "convert message buffs to json failed.".to_string(),
                                    )
                                })?,
                            );
                            Protocol::send(&mut self.local_context.stream, proto).await?
                        }
                        None => return Err(MQError::E(format!("the pull message is None."))),
                    }
                }
                SessionResponse::NotPullMessage => {
                    b_pull = true;
                    debug!("not pull message, ready reattempt send pull request.");
                    sleep(Duration::from_secs(PULL_SLEEP_SECONDS)).await
                }
                _ => {
                    error!("push data not suuport response type.")
                }
            }
        }
        Ok(())
    }
}
