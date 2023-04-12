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
            ProtocolHeaderType::RecvStr => {
                info!(
                    "[topic-{}] recv string: {}",
                    &topic,
                    String::from_utf8_lossy(&protocol.body)
                );
            }
            ProtocolHeaderType::RecvInt => {
                info!(
                    "[topic-{}] recv int: {}",
                    &topic,
                    BuffUtil::buff_to_i32(protocol.body).unwrap()
                );
            }
            ProtocolHeaderType::RecvFloat => {
                info!(
                    "[topic-{}] recv int: {}",
                    &topic,
                    BuffUtil::buff_to_f64(protocol.body).unwrap()
                );
            }
            ProtocolHeaderType::RecvBytes => {
                info!("[topic-{}] recv bytes: {:?}", &topic, protocol.body);
            }
            ProtocolHeaderType::RecvBool => todo!(),
            _ => todo!(),
        }
    }
    // 接收发布的数据
}

#[tokio::main]
async fn main() {
    let log_level = "error".to_string();
    init_console_log(log_level);
    subscribe(TOPIC.to_string()).await;
}
