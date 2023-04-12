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
use rand::Rng;

use lightmq::err::{MQError, MQResult};
use lightmq::logger::init_console_log;
use lightmq::protocol::{Protocol, ProtocolArgs, ProtocolHeaderType};
use lightmq::utils::convert::StringUtil;
use lightmq::utils::stream::{Buff, StreamUtil};
use std::net::ToSocketAddrs;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use toml::ser;

const SERVER_ADDR: &str = "127.0.0.1:7221";
const TOPIC: &str = "topic-test";

fn generate_random_string(length: usize) -> String {
    let mut rng = rand::thread_rng();
    let charset: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let result: String = (0..length)
        .map(|_| {
            let index = rng.gen_range(0..charset.len());
            charset[index] as char
        })
        .collect();
    result
}

fn generate_random_i32() -> i32 {
    let mut rng = rand::thread_rng();
    let num = rng.gen_range(0..65535);
    num
}

fn generate_random_f64() -> f64 {
    let mut rng = rand::thread_rng();
    let num = rng.gen_range(0.0..100000000.0);
    num
}

async fn publish(topic: String) {
    // let server_addr = "127.0.0.1:7221";
    let mut stream = TcpStream::connect(SERVER_ADDR).await.unwrap();

    let body = ProtocolBodyRegisterPublisher {
        topic: topic.clone(),
        name: "test-name".to_string(),
    };
    // 发送订阅请求
    let publish_proto = Protocol::new(
        ProtocolHeaderType::RegisterPublisher,
        ProtocolArgs::Null,
        serde_json::to_vec::<ProtocolBodyRegisterPublisher>(&body).unwrap(),
    );
    Protocol::send(&mut stream, publish_proto).await.unwrap();
    debug!("send register publisher protocol successful.");

    // loop {
    for i in 0..100 {
        let sleep_ms = 100;
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        let publish_proto = if i % 4 == 0 {
            let v = generate_random_string(10);
            info!("[topic-{}] publish message: {}", &topic, &v);
            Protocol::new(
                ProtocolHeaderType::SendStr,
                ProtocolArgs::Null,
                v.as_bytes().to_vec(),
            )
        } else if i % 4 == 1 {
            let v = generate_random_i32();
            info!("[topic-{}] publish int number: {}", &topic, &v);
            Protocol::new(
                ProtocolHeaderType::SendInt,
                ProtocolArgs::Null,
                v.to_be_bytes().to_vec(),
            )
        } else if i % 4 == 2 {
            let v = generate_random_f64();
            info!("[topic-{}] publish int float: {}", &topic, &v);
            Protocol::new(
                ProtocolHeaderType::SendFloat,
                ProtocolArgs::Null,
                v.to_be_bytes().to_vec(),
            )
        } else {
            let v = generate_random_string(32);
            info!("[topic-{}] publish int bytes: {:?}", &topic, &v);
            Protocol::new(
                ProtocolHeaderType::SendBytes,
                ProtocolArgs::Null,
                v.as_bytes().to_vec(),
            )
        };

        // info!("publish protocol: {:?}", &publish_proto);
        Protocol::send(&mut stream, publish_proto).await.unwrap();
        debug!("send publish message protocol successful.");
    }
}

#[tokio::main]
async fn main() {
    let log_level = "error".to_string();
    init_console_log(log_level);
    let start = Instant::now();

    publish(TOPIC.to_string()).await;
    let end = Instant::now();
    let duration = end - start;
    println!("publish end, use time: {:?}", duration);
}
