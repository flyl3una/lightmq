#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_must_use)]
#![allow(warnings)]

pub mod err;
pub mod logger;
pub mod protocol;
// pub mod tunnel;
pub mod connect_handle;
mod connector;
pub mod session;
pub mod utils;
// pub mod tls;
// pub mod dispatch;
// pub mod proxy;
// pub mod handle;

#[macro_use]
extern crate log;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate tokio;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate anyhow;

// use serde::{Serialize, Deserialize};
use crate::connector::Connector;
use crate::err::MQResult;
use session::SessionManager;
use std::fs;
use toml::from_str;
// use serde_json::from_str;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Log {
    pub level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Server {
    pub listen: String,
}

// config配置文件结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Configure {
    pub log: Log,
    pub server: Server,
}

pub fn load_configure(config_path: String) -> Configure {
    let contents = fs::read_to_string(&config_path)
        .expect(format!("Not read config file: {}", config_path).as_str());
    // println!("contents: {}", &contents);
    let config: Configure = from_str(&contents).unwrap();
    // println!("{:?}", config);
    config
}

pub struct LightMQCore {
    pub configure: Configure,
}

impl LightMQCore {
    pub fn new(config: Configure) -> Self {
        Self { configure: config }
    }

    pub async fn run(&self) -> MQResult<()> {
        // 创建一个会话管理 session

        let session_manager = SessionManager::new();
        let session_manager_tx = session_manager.tx.clone();
        tokio::spawn(async move {
            // 接收数据并处理协议
            let mut s = session_manager;
            info!("session manager run...");
            s.run().await;
        });

        // 监听连接
        let mut connector = Connector::new(self.configure.server.listen.clone());
        connector.build_context(session_manager_tx);
        connector.run().await
    }
}
