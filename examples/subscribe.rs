#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_must_use)]
#![allow(warnings)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate tokio;
#[macro_use]
extern crate serde;

use lightmq::connect_handle::ProtocolBodyRegisterPublisher;
use lightmq::err::{MQError, MQResult};
use lightmq::logger::init_console_log;
use lightmq::protocol::{Protocol, ProtocolArgs, ProtocolHeaderType};
use lightmq::session::{Message, ValueType};
use lightmq::utils::convert::{BuffUtil, StringUtil};
use lightmq::utils::stream::{Buff, StreamUtil};
use std::net::ToSocketAddrs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use toml::ser;

const SERVER_ADDR: &str = "127.0.0.1:7221";
const TOPIC: &str = "topic-test";

async fn subscribe(topic: String) {
    let mut stream = TcpStream::connect(SERVER_ADDR).await.unwrap();
    let body = ProtocolBodyRegisterPublisher {
        topic: topic.clone(),
        name: "test-name".to_string(),
    };
    // 发送订阅请求
    let subscribe_proto = Protocol::new(
        ProtocolHeaderType::RegisterSubscriber,
        ProtocolArgs::Null,
        serde_json::to_vec::<ProtocolBodyRegisterPublisher>(&body).unwrap(),
    );
    Protocol::send(&mut stream, subscribe_proto).await.unwrap();
    info!("register subscribe successfully");
    while let protocol = Protocol::read(&mut stream).await.unwrap() {
        // debug!("recv publish message: {:?}", protocol);
        let head_type = protocol.header.p_type.clone();
        match head_type {
            ProtocolHeaderType::SubscribeMessage => {
                let message = Message::try_from(protocol.body.clone()).unwrap();
                let value = message.value;
                match message.value_type {
                    ValueType::Str => {
                        info!(
                            "[topic-{}] [create: {}, recv: {}] recv string: {}",
                            &topic,
                            message
                                .create_time
                                .format("%Y-%m-%d %H:%M:%S.%f")
                                .to_string(),
                            message.recv_time.format("%Y-%m-%d %H:%M:%S.%f").to_string(),
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
                            message
                                .create_time
                                .format("%Y-%m-%d %H:%M:%S.%f")
                                .to_string(),
                            message.recv_time.format("%Y-%m-%d %H:%M:%S.%f").to_string(),
                            i32::from_be_bytes(buff)
                        );
                    }
                    ValueType::Float => {
                        let buff: [u8; 8] = value[0..8].try_into().unwrap();
                        info!(
                            "[topic-{}] [create: {}, recv: {}] recv float: {}",
                            &topic,
                            message
                                .create_time
                                .format("%Y-%m-%d %H:%M:%S.%f")
                                .to_string(),
                            message.recv_time.format("%Y-%m-%d %H:%M:%S.%f").to_string(),
                            f64::from_be_bytes(buff)
                        );
                    }
                    ValueType::Bytes => {
                        info!(
                            "[topic-{}] [create: {}, recv: {}] recv byte: {:?}",
                            &topic,
                            message
                                .create_time
                                .format("%Y-%m-%d %H:%M:%S.%f")
                                .to_string(),
                            message.recv_time.format("%Y-%m-%d %H:%M:%S.%f").to_string(),
                            &value
                        );
                    }
                }
            }
            _ => {
                error!(
                    "recv data. head type not match SubscribeMessage. head type: {:?}",
                    &head_type
                );
                break;
            }
        }
    }
    // 接收发布的数据
}

#[tokio::main]
async fn main() {
    let log_level = "info".to_string();
    init_console_log(log_level);
    subscribe(TOPIC.to_string()).await;
}
