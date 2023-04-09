#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_must_use)]
#![allow(warnings)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate tokio;
// #[macro_use]
// extern crate lazy_static;
#[macro_use]
extern crate serde;

#[macro_use]
extern crate clap;

use std::process::exit;
use lightmq::{load_configure, LightMQCore};
use clap::{Arg, ArgMatches, Parser, Command, Subcommand};
// use clap::{AppSettings};
// #[macro_use]

#[derive(Debug, Clone)]
#[derive(Parser)] // requires `derive` feature
#[clap(name = "lightmqd")]
// #[clap(setting = AppSettings::ColoredHelp)]
#[clap(about = "轻量级的MQ程序", long_about = None)]
struct Cli {
    // #[clap(subcommand)]
    // command: Commands,
    #[arg(short='c', long="config", value_parser, required=true)]
    configure: String,
    // 日志级别， [trace, debug, info, warn, debug]
    #[arg(long="level", value_parser, required=false, default_value="warn")]
    log_level: String,

    // #[clap(long="level", value_parser, required=false, default_value="warn")]
    // version: Option<String>,
}

// async fn run_proxy(param: RunParamProxy) {
//     let nfc_proxy = ProxyServer::new(param);
//     match nfc_proxy.run().await {
//         Err(e) => {
//             error!("run tunnel error: {}", e)
//         },
//         _ => {},
//     }
// }

async fn run() {
    let args = Cli::parse();
    println!("config: {}", &args.configure);

    // println!("log level: {}", args.log_level);
    let config = load_configure(args.configure.clone());
    // match  {
    //     Ok(config) => {
    //         println!("load config");
    //         println!("config: {:?}", config);
    //     },
    //     Err(e) => {
    //         println!("{}", e);
    //         exit(1)
    //     },
    // }
    println!("config: {:?}", config);
    let core = LightMQCore::new(config);
    core.run().await;
}

#[tokio::main]
async fn main() {
    run().await;
}