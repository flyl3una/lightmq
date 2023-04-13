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

use chrono::Utc;
use lightmq::connect_handle::ProtocolBodyRegisterPublisher;
use lightmq::err::{MQError, MQResult};
use lightmq::logger::init_console_log;
use lightmq::message::{Message, ValueType};
use lightmq::protocol::{Protocol, ProtocolArgs, ProtocolHeaderType};
use lightmq::utils::convert::{BuffUtil, StringUtil};
use lightmq::utils::stream::{Buff, StreamUtil};
use rand::Rng;
use std::net::ToSocketAddrs;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use toml::ser;

const SERVER_ADDR: &str = "127.0.0.1:7221";

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
                            i32::from_ne_bytes(buff)
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
                            f64::from_ne_bytes(buff)
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
            buff = v.to_ne_bytes().to_vec();
            value_type = ValueType::Int;
        } else if i % 4 == 2 {
            let v = generate_random_f64();
            info!("[topic-{}] publish float: {}", &topic, &v);
            buff = v.to_ne_bytes().to_vec();
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

// 10s, 当publish大于subscribe时，管道阻塞会导致速度变慢
async fn test_one_publish() {
    let log_level = "error".to_string();
    init_console_log(log_level);

    let topic = generate_random_string(5);
    let start = Instant::now();
    let t = topic.clone();
    tokio::spawn(async {
        subscribe(t).await;
    });
    publish(topic, 100).await;
    let end = Instant::now();
    let duration = end - start;
    let sleep_ms = 1000;
    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    println!("publish end, use time: {:?}", duration);
}

async fn test_more_publish() {
    let log_level = "error".to_string();
    init_console_log(log_level);

    let topic = generate_random_string(5);
    let start = Instant::now();
    let t = topic.clone();
    tokio::spawn(async {
        subscribe(t).await;
    });
    for i in 0..100 {
        let t = topic.clone();
        tokio::spawn(async {
            publish(t, 10000).await;
        });
    }
    publish(topic, 10000).await;
    let end = Instant::now();
    let duration = end - start;
    let sleep_ms = 1000;
    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    println!("publish end, use time: {:?}", duration);
}

//time：100s
async fn test_more_subscribe() {
    let log_level = "error".to_string();
    init_console_log(log_level);

    let topic = generate_random_string(5);
    let start = Instant::now();
    let t = topic.clone();
    for i in 0..10 {
        let t = topic.clone();
        tokio::spawn(async {
            subscribe(t).await;
        });
    }
    publish(topic, 1000).await;
    let end = Instant::now();
    let duration = end - start;
    let sleep_ms = 1000;
    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    println!("publish end, use time: {:?}", duration);
}

#[tokio::main]
async fn main() {
    // test_one_publish().await;
    // test_more_subscribe().await;
    test_more_publish().await;
}
