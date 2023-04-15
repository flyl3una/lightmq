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

use lightmq::connect_handle::ProtocolBodyRegisterQueue;
use lightmq::err::{MQError, MQResult};
use lightmq::instant::ClientInstant;
use lightmq::logger::init_console_log;
use lightmq::message::{Message, ValueType};
use lightmq::protocol::{Protocol, ProtocolArgs, ProtocolHeaderType};
use lightmq::utils::convert::{BuffUtil, StringUtil};
use lightmq::utils::stream::{Buff, StreamUtil};
use std::net::ToSocketAddrs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use toml::ser;

const SERVER_ADDR: &str = "127.0.0.1:7221";
const TOPIC: &str = "topic-test";

async fn subscribe(topic: &str, num: u32) {
    let mut instant = ClientInstant::new(String::from(SERVER_ADDR));
    instant.register_subscriber(topic, "subscribe-test1").await;

    // info!("register subscribe successfully");
    match instant.pull(num).await {
        Ok(message) => {
            info!("message: {:?}", message);
        }
        Err(err) => {
            error!("subscribe failed: {}", err);
        }
    }
}

async fn subscribe_all(topic: &str, num: u32) {
    let mut instant = ClientInstant::new(String::from(SERVER_ADDR));
    instant.register_subscriber(topic, "subscribe-test1").await;
    let index = 0u32;
    loop {
        match instant.pull(num).await {
            Ok(message) => {
                info!("message: {:?}", message);
            }
            Err(err) => {
                error!("subscribe failed: {}", err);
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
    subscribe_all(TOPIC, 3).await;
    // subscribe(TOPIC, 13).await;
}
