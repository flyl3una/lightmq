use log::LevelFilter;
use log4rs::append::console::{ConsoleAppender, Target};
use log4rs::append::file::FileAppender;
use log4rs::encode::pattern::PatternEncoder;
use log4rs::config::{Appender, Config, Logger, Root};

pub static LOG_SETTING_PATH: &str = "config/log4rs.yaml";

// 日志需要改为不读取文件。

pub fn init_console_log(level: String){
    let stdout = ConsoleAppender::builder()
        .target(Target::Stderr)
        .encoder(Box::new(PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S)} - {l} - {t} - [{M}<{T}> - {f}:{L}] - {m}{n}")))
        .build();

    let level1 = level.to_lowercase();
    let mut log_level = LevelFilter::Warn;
    log_level = if level1.eq("debug") { LevelFilter::Debug }
    else if level1.eq("warn") {LevelFilter::Warn}
    else if level1.eq("info") {LevelFilter::Info}
    else if level1.eq("error") {LevelFilter::Error}
    else if level1.eq("trace") {LevelFilter::Trace}
    else {LevelFilter::Warn};
    
    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(log_level))
        .unwrap();

    log4rs::init_config(config);
    info!("init log successful. current level: {}", &level);
}


pub fn init_file_log(path: Option<String>) -> anyhow::Result<()> {
    let mut p;
    if let Some(c) = path {
        p = c;
    } else {
        p = String::from(LOG_SETTING_PATH);
    }

    log4rs::init_file(p.as_str(), Default::default())?;
    Ok(())
}

// 不使用日志配置文件，直接设置
pub fn init_stderr_log() -> anyhow::Result<()> {
    Ok(())
}