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
use chrono::{DateTime, TimeZone, Utc};
use lightmq::connect_handle::ProtocolBodyRegisterPublisher;
use lightmq::session::{Message, ValueType};
use rand::Rng;

use lightmq::err::{MQError, MQResult};
use lightmq::logger::init_console_log;
use lightmq::protocol::{Protocol, ProtocolArgs, ProtocolHeaderType};
use lightmq::utils::convert::StringUtil;
use lightmq::utils::stream::{Buff, StreamUtil};
use serde_json::to_vec;
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

async fn publish(topic: String, publish_num: usize) {
    // let server_addr = "127.0.0.1:7221";
    let mut stream = TcpStream::connect(SERVER_ADDR).await.unwrap();

    let body = ProtocolBodyRegisterPublisher {
        topic: topic.clone(),
        name: "test-name".to_string(),
    };
    // let current_time = Utc::now();
    // let message = Message{ create_time: current_time.clone(), send_time: current_time.clone(), recv_time: current_time, value_type: ValueType::Str, value_length: , value: todo!() }
    // 发送订阅请求
    let register_publish = serde_json::to_vec::<ProtocolBodyRegisterPublisher>(&body).unwrap();
    let publish_proto = Protocol::new(
        ProtocolHeaderType::RegisterPublisher,
        ProtocolArgs::Null,
        register_publish,
    );
    Protocol::send(&mut stream, publish_proto).await.unwrap();
    debug!("send register publisher protocol successful.");

    // loop {
    for i in 0..publish_num {
        let sleep_ms = 100;
        // tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        let current = Utc::now();
        let mut buff: Vec<u8> = vec![];
        let mut value_type = ValueType::Null;
        if i % 4 == 0 {
            let v = generate_random_string(10);
            info!("[topic-{}] publish str: {}", &topic, &v);
            buff = v.as_bytes().to_vec();
            value_type = ValueType::Str;
        } else if i % 4 == 1 {
            let v = generate_random_i32();
            info!("[topic-{}] publish int: {}", &topic, &v);
            buff = v.to_be_bytes().to_vec();
            value_type = ValueType::Int;
        } else if i % 4 == 2 {
            let v = generate_random_f64();
            info!("[topic-{}] publish float: {}", &topic, &v);
            buff = v.to_be_bytes().to_vec();
            value_type = ValueType::Float;
        } else {
            let v = generate_random_string(32);
            info!("[topic-{}] publish bytes: {:?}", &topic, &v);
            buff = v.as_bytes().to_vec();
            value_type = ValueType::Bytes;
        }
        let message = Message {
            create_time: current.clone(),
            send_time: current.clone(),
            recv_time: current.clone(),
            value_type,
            value_length: buff.len() as u64,
            value: buff,
        };
        let proto = Protocol::new(
            ProtocolHeaderType::PublishMessage,
            ProtocolArgs::Null,
            message.into(),
        );
        // info!("publish protocol: {:?}", &publish_proto);
        Protocol::send(&mut stream, proto).await.unwrap();
        debug!("send publish message protocol successful.");
    }
}

#[tokio::main]
async fn main() {
    let log_level = "error".to_string();
    init_console_log(log_level);
    let start = Instant::now();

    publish(TOPIC.to_string(), 10).await;
    let end = Instant::now();
    let duration = end - start;
    println!("publish end, use time: {:?}", duration);
}
