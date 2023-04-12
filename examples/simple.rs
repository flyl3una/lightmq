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
                info!("[topic-{}] recv int: {:?}", &topic, protocol.body);
            }
            ProtocolHeaderType::RecvBool => todo!(),
            _ => todo!(),
        }
    }
    // 接收发布的数据
}

async fn publish(topic: String, num: usize) {
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
    for i in 0..num {
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
            let v = generate_random_string(4);
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
    for i in 0..10 {
        let t = topic.clone();
        tokio::spawn(async {
            publish(t, 100).await;
        });
    }
    publish(topic, 100).await;
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
    test_more_subscribe().await;
    // test_more_publish().await;
}
