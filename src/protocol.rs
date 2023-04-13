use crate::err::{MQError, MQResult};
use crate::utils::convert::{BuffUtil, VecUtil};
use crate::utils::stream::StreamUtil;
use std::convert::{From, Into, TryFrom, TryInto};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
// use std::prelude::rust_2021::{TryFrom, TryInto};
use crate::err::ErrorCode;
use crate::err::ErrorCode::Success;
use crate::utils::stream::Buff;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tokio::net::TcpStream;

pub const PROTOCOL_VERSION: u8 = 1;
pub const PROTOCOL_REVERSE: u8 = 0;

#[derive(Debug, Clone, Serialize)]
pub struct Protocol {
    // 协议头
    pub header: ProtocolHeader,
    // 协议头扩展参数
    pub args: ProtocolArgs,
    // 协议消息体
    pub body: Buff,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[repr(C)]
pub struct ProtocolHeader {
    #[serde(rename = "type")]
    // 协议版本
    pub version: u8,
    // 保留字段
    pub reverse: u8,
    // 协议类型, u32
    pub p_type: ProtocolHeaderType, // response时，type加0x100
    // 协议扩展参数长度
    pub args_len: u32,
    // 协议body长度
    pub body_len: u64,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
// #[repr(C)]
pub enum ProtocolHeaderType {
    Null = 0, // u16
    // 注销连接
    Disconnect = 1,
    // 注册为生产者
    RegisterPublisher = 11,
    // 发送数据
    PublishMessage = 12,

    // SendStr = 12,
    // SendInt = 13,
    // SendFloat = 14,
    // SendBytes = 15,
    // SendBool = 16,
    // SendStr = 12,

    // 注册为消费者
    RegisterSubscriber = 21,
    // 接收数据
    SubscribeMessage = 22,
    // RecvStr = 22,
    // RecvInt = 23,
    // RecvFloat = 24,
    // RecvBytes = 25,
    // RecvBool = 26,
}

pub const PROTOCOL_HEAD_VERSION: u8 = 0x01;

// pub const MQ_RESPONSE_TYPE_INCREASE: u8 = 0x01;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterProcuer {
    // pub name: String,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum ProtocolArgs {
    // 协议参数,json 格式，根据协议定
    Null,
    // Bool(bool),
    // Str(String),
    // Int(i32),
    // Float(f64),
    // Bytes(Vec<u8>),
    // 参数
    // RegisterPublisher,
    // SendStr(String),
    // SendInt(i32),
    // SendFloat(f64),

    // ResigerSubscribe,
    // RecvStr(String),
    // RecvInt(i32),
    // RecvFloat(f64),
    // TunnelStart(ProtocolTunnelStartArgs),
    // Data(Data),
}

impl ProtocolArgs {
    pub fn to_buff(self) -> Vec<u8> {
        match self {
            ProtocolArgs::Null => Vec::new(),
            // ProtocolArgs::Bool(b) => {
            //     if b {
            //         [1;1].to_vec()
            //     } else {
            //         [0;1].to_vec()
            //     }
            // },
            // ProtocolArgs::Str(ref s) => s.as_bytes().to_vec(),
            // ProtocolArgs::Int(i) => i.to_be_bytes().to_vec(),
            // ProtocolArgs::Float(f) => f.to_be_bytes().to_vec(),
            // ProtocolArgs::Bytes(b) => b,
        }
    }
}

// #[derive(Debug, Clone, Serialize, Default)]
// pub struct ProtocolArgs {
//     // 协议参数,json 格式，根据协议定
//     pub args: String,
// }

#[derive(Debug, Clone, Serialize, Default)]
pub struct ProtocolBody {
    pub body: Vec<u8>,
}

pub const PROTOCOL_HEAD_LENGTH: usize = 16; // 根据Protocolheader计算得出

impl Into<u16> for ProtocolHeaderType {
    fn into(self) -> u16 {
        self as u16
    }
}

impl From<u16> for ProtocolHeaderType {
    fn from(value: u16) -> Self {
        use ProtocolHeaderType::*;
        if value == Disconnect as u16 {
            Disconnect
        } else if value == RegisterPublisher as u16 {
            RegisterPublisher
        // } else if value == SendStr as u16 {
        //     SendStr
        // } else if value == SendInt as u16 {
        //     SendInt
        // } else if value == SendFloat as u16 {
        //     SendFloat
        // } else if value == SendBytes as u16 {
        //     SendBytes
        } else if value == PublishMessage as u16 {
            PublishMessage
        } else if value == RegisterSubscriber as u16 {
            RegisterSubscriber
        } else if value == SubscribeMessage as u16 {
            SubscribeMessage
        // } else if value == RecvStr as u16 {
        //     RecvStr
        // } else if value == RecvInt as u16 {
        //     RecvInt
        // } else if value == RecvFloat as u16 {
        //     RecvFloat
        // } else if value == RecvBytes as u16 {
        //     RecvBytes
        } else {
            Null
        }
    }
}

impl From<[u8; PROTOCOL_HEAD_LENGTH]> for ProtocolHeader {
    fn from(buff: [u8; PROTOCOL_HEAD_LENGTH]) -> Self {
        let err = format!(
            "protocol buff index error. buff len: {}",
            PROTOCOL_HEAD_LENGTH
        );
        // let type_buff: [u8; 4] = buff[1..5].try_into().expect(err.as_str());
        let proto_head_type: [u8; 2] = buff[2..4].try_into().expect(err.as_str());
        let arg_len_buff: [u8; 4] = buff[4..8].try_into().expect(err.as_str());
        let body_len_buff: [u8; 8] = buff[8..16].try_into().expect(err.as_str());
        // let p_type_num = u32::from_be_bytes(type_buff);
        let proto_head_type_num = u16::from_be_bytes(proto_head_type);
        let args_len = u32::from_be_bytes(arg_len_buff);
        let body_len = u64::from_be_bytes(body_len_buff);
        Self {
            version: buff[0],
            reverse: buff[1],
            p_type: ProtocolHeaderType::from(proto_head_type_num),
            args_len,
            body_len,
        }
    }
}

impl Into<Vec<u8>> for ProtocolHeader {
    fn into(self) -> Vec<u8> {
        let mut buff: Vec<u8> = vec![];

        buff.push(self.version);
        buff.push(self.reverse);
        let type_num = self.p_type as u16;
        buff.extend_from_slice(&type_num.to_be_bytes());
        // buff.push(self.p_type as u8);
        buff.extend_from_slice(&self.args_len.to_be_bytes());
        buff.extend_from_slice(&self.body_len.to_be_bytes());
        buff
    }
}

impl ProtocolHeader {
    pub fn new(head_type: ProtocolHeaderType, args_len: u32, body_len: u64) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            reverse: PROTOCOL_REVERSE,
            p_type: head_type,
            args_len: args_len,
            body_len: body_len,
        }
    }
}

impl Into<Vec<u8>> for ProtocolArgs {
    fn into(self) -> Vec<u8> {
        self.to_buff()
    }
}

impl TryFrom<Vec<u8>> for ProtocolHeader {
    type Error = MQError;

    fn try_from(buff: Vec<u8>) -> Result<Self, Self::Error> {
        let length = buff.len();
        if length.clone() < PROTOCOL_HEAD_LENGTH {
            return Err(MQError::E(format!(
                "the protocol buff must is {}, current length: {}",
                PROTOCOL_HEAD_LENGTH, &length
            )));
        }

        let err = format!("protocol buff index error. buff len: {}", length);

        let proto_head_type: [u8; 2] = buff[2..4].try_into().unwrap();
        let proto_head_type_num = u16::from_be_bytes(proto_head_type);

        let arg_len_buff: [u8; 4] = buff[4..8].try_into().unwrap();
        let body_len_buff: [u8; 8] = buff[8..16].try_into().unwrap();

        let args_len = u32::from_be_bytes(arg_len_buff);
        let body_len = u64::from_be_bytes(body_len_buff);
        let header_type = ProtocolHeaderType::from(proto_head_type_num);
        let header = ProtocolHeader {
            version: buff[0],
            reverse: buff[1],
            p_type: header_type,
            args_len,
            body_len,
        };
        Ok(header)
    }
}

impl ProtocolArgs {
    pub fn len(&self) -> usize {
        match self {
            // ProtocolArgs::Str(s) => s.len(),
            // ProtocolArgs::Bytes(b) => b.len(),
            // ProtocolArgs::Int(i) => 4,
            ProtocolArgs::Null => 0,
            // ProtocolArgs::Bool(b) => 1,
            // ProtocolArgs::Float(f) => 8,
            // ProtocolArgs::TunnelStart(arg) => {
            //     let buff = serde_json::to_vec(&arg).unwrap();
            //     buff.len()
            // }
        }
    }
    pub fn make(head_type: ProtocolHeaderType, buff: Vec<u8>) -> MQResult<ProtocolArgs> {
        use ProtocolHeaderType::*;
        let arg = match head_type {
            _ => {
                warn!("the proto args not parse");
                ProtocolArgs::Null
            }
        };
        Ok(arg)
    }
}

impl ProtocolHeader {
    // 接收协议头
    pub async fn read<T>(reader: &mut T) -> MQResult<Self>
    where
        T: AsyncReadExt + Unpin,
    {
        let proto_header_buff = StreamUtil::read_exact(reader, PROTOCOL_HEAD_LENGTH).await?;
        let proto_header = ProtocolHeader::try_from(proto_header_buff)?;
        Ok(proto_header)
    }
}

impl Protocol {
    // 接收协议，并保存到缓冲区中。
    pub async fn read_buff<T>(reader: &mut T) -> MQResult<Buff>
    where
        T: AsyncReadExt + Unpin,
    {
        StreamUtil::read_exact(reader, PROTOCOL_HEAD_LENGTH).await
    }

    // 接收协议
    pub async fn read<T>(reader: &mut T) -> MQResult<Self>
    where
        T: AsyncReadExt + Unpin,
    {
        let mut proto_args = ProtocolArgs::Null;
        let mut proto_body = Vec::new();
        let proto_header_buff = StreamUtil::read_exact(reader, PROTOCOL_HEAD_LENGTH).await?;

        let proto_header = ProtocolHeader::try_from(proto_header_buff)?;
        let args_len = proto_header.args_len.clone() as usize;
        if args_len != 0 {
            let proto_args_buff = StreamUtil::read_exact(reader, args_len).await?;
            let proto_args_buff_len = proto_args_buff.len();
            if args_len != proto_args_buff_len {
                return Err(MQError::E(format!(
                    "the args len must be recv proto args buffer length"
                )));
            }
            let origin_bytes = format!("bytes: {:?}", &proto_args_buff);

            use ProtocolHeaderType::*;
            proto_args = ProtocolArgs::make(proto_header.p_type.clone(), proto_args_buff)?;
        }
        let body_len = proto_header.body_len.clone() as usize;
        if body_len != 0 {
            let proto_body_buff = StreamUtil::read_exact(reader, body_len).await?;
            proto_body = proto_body_buff;
        }
        let proto = Protocol {
            header: proto_header,
            args: proto_args,
            body: proto_body,
        };
        // debug!("recv protocol: {:?}", &proto);
        Ok(proto)
    }

    //发送数据
    pub async fn send<T>(writer: &mut T, proto: Protocol) -> MQResult<()>
    where
        T: AsyncWriteExt + Unpin,
    {
        // debug!("ready send protocol: {:?}", &proto);
        let mut args_buff = vec![];
        let args_buff_len = proto.header.args_len.clone();
        let body_buff_len = proto.header.body_len.clone();
        let header_buff: Vec<u8> = proto.header.into();
        let header_args_buff: Vec<u8> = proto.args.into();
        // let args_buff = args.to_vec();
        debug!("send header buffer: {:?}", &header_buff);
        match StreamUtil::write_all(writer, header_buff).await {
            Err(e) => {
                return Err(MQError::E(format!("writer header failed.")));
            }
            _ => {}
        }
        if args_buff_len > 0 {
            match StreamUtil::write_all(writer, args_buff).await {
                Err(e) => {
                    return Err(MQError::E(format!("writer protocol args buff failed.")));
                }
                _ => {}
            }
        }

        if body_buff_len > 0 {
            match StreamUtil::write_all(writer, proto.body).await {
                Err(e) => {
                    return Err(MQError::E(format!("writer protocol body buff failed.")));
                }
                _ => {}
            }
        }
        Ok(())
    }

    // 生成一个相应数据
    pub fn new(p_type: ProtocolHeaderType, args: ProtocolArgs, body: Buff) -> Self {
        let args_len = args.len() as u32;
        let body_len = body.len() as u64;
        Self {
            header: ProtocolHeader::new(p_type, args_len, body_len),
            args,
            body,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Data {
    pub code: i32,
    pub msg: String,
    pub data: serde_json::Value,
}

impl Data {
    pub fn update_error<E>(&mut self, code: i32, msg: E)
    where
        E: ToString,
    {
        self.code = code;
        self.msg = msg.to_string();
    }
}

#[test]
async fn test_protocol() {
    let generate_random_string = |length: i32| -> String {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let charset: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let result: String = (0..length)
            .map(|_| {
                let index = rng.gen_range(0..charset.len());
                charset[index] as char
            })
            .collect();
        result
    };

    let head = ProtocolHeader::new(ProtocolHeaderType::RegisterSubscriber, 3, 2);
    // let proto = Protocol::new(
    //     ProtocolHeaderType::Disconnect,
    //     ProtocolArgs::Null,
    //     generate_random_string(10).as_bytes().to_vec(),
    // );
    println!("head: {:?}", &head);
    let buff: Vec<u8> = head.clone().into();
    // let buf: [u8; PROTOCOL_HEAD_LENGTH] = buff[0..PROTOCOL_HEAD_LENGTH];
    let new_head = ProtocolHeader::try_from(buff).unwrap();
    println!("new head:{:?}", new_head);
    assert_eq!(head.version, new_head.version);
    assert_eq!(head.reverse, new_head.reverse);
    assert_eq!(head.p_type as u16, new_head.p_type as u16);
    assert_eq!(head.args_len, new_head.args_len);
    assert_eq!(head.body_len, new_head.body_len);
    // assert!(proto.args, new_proto.args);
    // assert!(proto.body, new_proto.body);
}
