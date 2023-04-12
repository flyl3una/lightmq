use std::path::Display;
use thiserror;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

#[derive(Debug, thiserror::Error)]
pub enum MQError {
    #[error("std io error: `{0}`")]
    StdIoError(#[from] std::io::Error),

    // #[error("futures io error: `{0}`")]
    // FuturesIoError(futures::io::Error),
    #[error("the key `{0}` is no exists.")]
    KeyError(String),

    #[error("the variable `{0}` is None")]
    NoneError(String),

    #[error("invalid param: `{0}`")]
    InvalidParamError(String),

    // 连接断开
    #[error("io error: `{0}`")]
    IoError(String),

    #[error("error: `{0}`")]
    E(String),

    #[error("convert error: `{0}`")]
    ConvertError(String),

    #[error("serde json error: `{0}`")]
    SerdeJsonError(#[from] serde_json::Error),
    // #[error("error code: `{0}`")]
    // ResultCodeError(i32),

    // #[error("anyhow error")]
    // Other(#[from] anyhow::Error),
}

impl MQError {
    pub fn description(&self) -> String {
        use MQError::*;
        match self {
            // Other(e) => anyhow_error_to_chain(e),
            IoError(e) => e.to_string(),
            KeyError(e) => e.to_string(),
            NoneError(e) => e.to_string(),
            InvalidParamError(e) => e.to_string(),
            SerdeJsonError(e) => e.to_string(),
            E(e) => e.to_string(),
            ConvertError(e) => e.to_string(),
            // ResultCodeError(e) => e.to_string(),
            _ => "".to_string(),
        }
    }

}


#[repr(i32)]
pub enum ErrorCode {
    Success = 0,
    Fail = -1,
}

// impl Into<i32> for ErrorCode {
//     fn into(self) -> i32 {
//         self as i32
//     }
// }

pub type MQResult<T> = Result<T, MQError>;

// pub fn anyhow_error_to_chain(e: &anyhow::Error) -> String {
//     let mut err = "".to_string();
//     e.chain().next()
//         .iter()
//         .enumerate()
//         .for_each(|(index, e)| err = format!("{}\n{} - {}", err, index, e));
//     err
// }
