#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_must_use)]
#![allow(warnings)]

pub mod logger;
pub mod err;
// pub mod protocol;
// pub mod tunnel;
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
extern crate crypto;

// use serde::{Serialize, Deserialize};
use crate::err::MQResult;
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
