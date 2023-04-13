use std::fmt::Display;

use crate::err::{MQError, MQResult};
use chrono::{DateTime, TimeZone, Utc};

#[derive(Debug, Clone)]
#[repr(C)]
pub enum ValueType {
    Null = 1,
    Bool = 2,
    Str = 3,
    Int = 4,
    Float = 5,
    Bytes = 6,
}

// 与connector交互数据的消息
#[derive(Debug, Clone)]
pub struct Message {
    // 消息主题
    // pub topic: String,
    // 消息发送时间
    pub create_time: DateTime<Utc>,
    pub send_time: DateTime<Utc>,
    // 发送时间
    pub recv_time: DateTime<Utc>,
    // 数据类型
    pub value_type: ValueType,
    // 数据长度
    pub value_length: u64,
    // 数据内容
    pub value: Vec<u8>,
    // 消息内容
    // pub value: Value,
    // pub value: serde_json::Value,
}

impl TryFrom<Vec<u8>> for Message {
    type Error = MQError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let err = "protocol buff must more than 33 byte.".to_string();
        if value.len() < 33 {
            return Err(Self::Error::E(err));
        }
        // let type_buff: [u8; 4] = buff[1..5].try_into().expect(err.as_str());
        let create_times_buff: [u8; 8] = value[0..8].try_into().expect(err.as_str());
        let send_times_buff: [u8; 8] = value[8..16].try_into().expect(err.as_str());
        let recv_times_buff: [u8; 8] = value[16..24].try_into().expect(err.as_str());
        let value_len_buff: [u8; 8] = value[25..33].try_into().expect(err.as_str());
        // let proto_head_type_num = u16::from_ne_bytes(value[0..16]);
        let create_timestamp = i64::from_ne_bytes(create_times_buff);
        let send_timestamp = i64::from_ne_bytes(send_times_buff);
        let recv_timestamp = i64::from_ne_bytes(recv_times_buff);
        let value_len = u64::from_ne_bytes(value_len_buff);

        Ok(Self {
            create_time: Utc.timestamp_nanos(create_timestamp),
            send_time: Utc.timestamp_nanos(send_timestamp),
            recv_time: Utc.timestamp_nanos(recv_timestamp),
            value_length: value_len,
            value_type: ValueType::try_from(value[24])?,
            value: value[33..].to_vec(),
        })
    }
}

impl Into<Vec<u8>> for Message {
    fn into(self) -> Vec<u8> {
        let mut buff: Vec<u8> = vec![];
        let create_time_num = self.create_time.timestamp_nanos();
        let send_time_num = self.create_time.timestamp_nanos();
        let recv_time_num = self.create_time.timestamp_nanos();
        buff.extend_from_slice(&create_time_num.to_ne_bytes());
        buff.extend_from_slice(&send_time_num.to_ne_bytes());
        buff.extend_from_slice(&recv_time_num.to_ne_bytes());
        buff.push(self.value_type as u8);
        buff.extend_from_slice(&self.value_length.to_ne_bytes());
        buff.extend_from_slice(&self.value);
        buff
    }
}

impl TryFrom<u8> for ValueType {
    type Error = MQError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        use ValueType::*;
        if value == Null as u8 {
            Ok(Null)
        } else if value == Bool as u8 {
            Ok(Bool)
        } else if value == Str as u8 {
            Ok(Str)
        } else if value == Int as u8 {
            Ok(Int)
        } else if value == Float as u8 {
            Ok(Float)
        } else if value == Bytes as u8 {
            Ok(Bytes)
        } else {
            Err(MQError::E(format!("not match value type: {}", value)))
        }
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = self.value.clone();
        let v = match &self.value_type {
            ValueType::Str => {
                format!("recv string: {}", String::from_utf8_lossy(&value))
            }
            ValueType::Null => todo!(),
            ValueType::Bool => todo!(),
            ValueType::Int => {
                let buff: [u8; 4] = value[0..4].try_into().unwrap();
                format!("recv int: {}", i32::from_ne_bytes(buff))
            }
            ValueType::Float => {
                let buff: [u8; 8] = value[0..8].try_into().unwrap();
                format!("recv float: {}", f64::from_ne_bytes(buff))
            }
            ValueType::Bytes => {
                format!("recv byte: {:?}", &value)
            }
        };
        write!(
            f,
            "[{} - {}] type: {:?}, length: {}, value: {}",
            &self.create_time, &self.recv_time, &self.value_type, &self.value_length, v,
        )
    }
}