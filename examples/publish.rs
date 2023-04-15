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
use lightmq::connect_handle::ProtocolBodyRegisterQueue;
use lightmq::instant::ClientInstant;
use lightmq::message::{Message, ValueType};
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

async fn publish(topic: &str, publish_num: usize) {
    let mut instant = ClientInstant::new(String::from(SERVER_ADDR));
    instant.register_publisher(topic, "publish-test1").await;

    // loop {
    for i in 0..publish_num {
        let current = Utc::now();
        let mut buff: Vec<u8> = vec![];
        // let mut value_type = ValueType::Null;
        if i % 4 == 0 {
            let v = generate_random_string(10);
            info!("[topic-{}] publish str: {}", &topic, &v);
            instant.push_str(v).await.unwrap();
        } else if i % 4 == 1 {
            let v = generate_random_i32();
            info!("[topic-{}] publish int: {}", &topic, &v);
            instant.push_int(v).await.unwrap();
        } else if i % 4 == 2 {
            let v = generate_random_f64();
            info!("[topic-{}] publish float: {}", &topic, &v);
            instant.push_float(v).await.unwrap();
        } else {
            let v = generate_random_string(32);
            info!("[topic-{}] publish bytes: {:?}", &topic, &v);
            instant.push_bytes(v.as_bytes().to_vec()).await.unwrap();
        }
        // info!("publish protocol: {:?}", &publish_proto);
        // Protocol::send(&mut stream, proto).await.unwrap();
        debug!("send publish message protocol successful.");
    }
}

#[tokio::main]
async fn main() {
    let log_level = "info".to_string();
    init_console_log(log_level);

    let start = Instant::now();

    // 30s ,1000000条数据
    publish(TOPIC, 10).await;
    let end = Instant::now();
    let duration = end - start;
    println!("publish end, use time: {:?}", duration);
}
