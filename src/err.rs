use std::path::Display;
use thiserror;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

#[derive(Debug, thiserror::Error)]
pub enum MQError {
    #[error("std io error: `{0}`")]
    StdIoError(#[from] std::io::Error),

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
}

impl MQError {
    pub fn description(&self) -> String {
        use MQError::*;
        match self {
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


pub type MQResult<T> = Result<T, MQError>;


