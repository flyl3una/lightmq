use crate::utils::convert::{VecUtil, BuffUtil};
use crate::utils::stream::StreamUtil;
use tokio::io::{BufReader, BufWriter, AsyncReadExt, AsyncWriteExt};
use crate::err::{MQResult, MQError};
use std::convert::{From, Into, TryInto, TryFrom};
// use std::prelude::rust_2021::{TryFrom, TryInto};
use crate::utils::stream::Buff;
use std::fmt::Debug;
use serde::{Serialize, Deserialize};
use tokio::net::TcpStream;
use crate::err::ErrorCode::Success;
use crate::err::ErrorCode;


#[derive(Debug, Clone, Serialize)]
pub struct Protocol {
    // 协议头
    pub header: ProtocolHeader,
    // 协议头扩展参数
    pub args: Option<ProtocolArgs>,
    // 协议消息体
    pub body: Option<Buff>,
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
    pub p_type: ProtocolHeaderType,       // response时，type加0x100
    // 协议扩展参数长度
    pub args_len: u32,
    // 协议body长度
    pub body_len: u64,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
// #[repr(C)]
pub enum ProtocolHeaderType {
    Null = 0,   // u16
    // 注销连接
    Disconnect = 1,
    // 注册为生产者
    ResiterProducer = 11,
    // 发送数据 
    SendStr = 12,
    SendInt = 13,
    SendFloat = 14,
    SendBytes = 15,
    SendBool = 16,
    // SendStr = 12,

    // 注册为消费者
    RegisterConsumer = 21,
    // 接收数据
    RecvStr = 22,
    RecvInt = 23,
    RecvFloat = 24,
    RecvBytes = 25,
    RecvBool = 26,
}

pub const PROTOCOL_HEAD_VERSION: u8 = 0x01;

// pub const MQ_RESPONSE_TYPE_INCREASE: u8 = 0x01;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResiterProcuer{
    // pub name: String,

}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProtocolArgs {
    // 协议参数,json 格式，根据协议定
    None,
    Bool(bool),
    Str(String),
    Int(i32),
    Float(f64),
    Bytes(Vec<u8>),
    // 参数
    // RegisterProducer,
    // SendStr(String),
    // SendInt(i32),
    // SendFloat(f64),

    // ResigerConsumer,
    // RecvStr(String),
    // RecvInt(i32),
    // RecvFloat(f64),
    // TunnelStart(ProtocolTunnelStartArgs),
    // Data(Data),
}

impl ProtocolArgs {
    pub fn to_vec(self) -> Vec<u8> { 
        match self {
            ProtocolArgs::None => Vec::new(),
            ProtocolArgs::Bool(b) => {
                if b {
                    [1;1].to_vec()
                } else {
                    [0;1].to_vec()
                }
            },
            ProtocolArgs::Str(ref s) => s.as_bytes().to_vec(),
            ProtocolArgs::Int(i) => i.to_be_bytes().to_vec(),
            ProtocolArgs::Float(f) => f.to_be_bytes().to_vec(),
            ProtocolArgs::Bytes(b) => b,
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

pub const PROTOCOL_HEAD_LENGTH: usize = 16;     // 根据Protocolheader计算得出

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
        } else if value == ResiterProducer as u16 {
            ResiterProducer
        } else if value == SendStr as u16 {
            SendStr
        } else if value == SendInt as u16 {
            SendInt
        } else if value == SendFloat as u16 {
            SendFloat
        } else if value == SendBytes as u16 {
            SendBytes
        } else if value == RegisterConsumer as u16 {
            RegisterConsumer
        } else if value == RecvStr as u16 {
            RecvStr
        } else if value == RecvInt as u16 {
            RecvInt
        } else if value == RecvFloat as u16 {
            RecvFloat
        } else if value == RecvBytes as u16 {
            RecvBytes
        } else {
            Null
        }
    }
}

impl From<[u8; PROTOCOL_HEAD_LENGTH]> for ProtocolHeader {
    fn from(buff: [u8; PROTOCOL_HEAD_LENGTH]) -> Self {
        let err = format!("protocol buff index error. buff len: {}", PROTOCOL_HEAD_LENGTH);
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
        buff.push(self.p_type as u8);
        buff.extend_from_slice(&self.args_len.to_be_bytes());
        buff.extend_from_slice(&self.body_len.to_be_bytes());
        buff
    }
}

impl TryFrom<Vec<u8>> for ProtocolHeader {
    type Error = MQError;

    fn try_from(buff: Vec<u8>) -> Result<Self, Self::Error> {
        let length = buff.len();
        if length.clone() < PROTOCOL_HEAD_LENGTH {
            return Err(MQError::E(format!("the protocol buff must is {}, current length: {}", PROTOCOL_HEAD_LENGTH, &length)));
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
            ProtocolArgs::Str(s) => s.len(),
            ProtocolArgs::Bytes(b) => b.len(),
            ProtocolArgs::Int(i) => 4,
            ProtocolArgs::None => 0,
            ProtocolArgs::Bool(b) => 1,
            ProtocolArgs::Float(f) => 8,
            // ProtocolArgs::TunnelStart(arg) => {
            //     let buff = serde_json::to_vec(&arg).unwrap();
            //     buff.len()
            // }
        }
    }
    pub fn make(head_type: ProtocolHeaderType, buff: Vec<u8>) -> MQResult<Option<ProtocolArgs>> {
        use ProtocolHeaderType::*;
        let arg = match head_type {
            SendStr => {
                Some(ProtocolArgs::Str(BuffUtil::buff_to_str(buff)?))
            },
            RecvStr => {
                Some(ProtocolArgs::Str(BuffUtil::buff_to_str(buff)?))
            }
            SendFloat => {
                Some(ProtocolArgs::Float(BuffUtil::buff_to_f64(buff)?))
            },
            RecvFloat => {
                Some(ProtocolArgs::Float(BuffUtil::buff_to_f64(buff)?))
            },
            SendInt => {
                Some(ProtocolArgs::Int(BuffUtil::buff_to_i32(buff)?))
            },
            RecvInt => {
                Some(ProtocolArgs::Int(BuffUtil::buff_to_i32(buff)?))
            },
            SendBytes => {
                Some(ProtocolArgs::Bytes(buff))
            },
            RecvBytes => {
                Some(ProtocolArgs::Bytes(buff))
            },
            SendBool => {
                Some(ProtocolArgs::Bool(BuffUtil::buff_to_bool(buff)?))
            },
            RecvBool => {
                Some(ProtocolArgs::Bool(BuffUtil::buff_to_bool(buff)?))
            }
            _ => {
                warn!("the proto args not parse");
                // ProtocolArgs::None
                None
            }
            // _ => {return Err(MQError::E(format!("not support protocol head type. type")))}
        };
        Ok(arg)
    }
}


impl ProtocolHeader {
    // 接收协议头
    pub async fn read<T>(reader: &mut T) -> MQResult<Self>
        where T: AsyncReadExt + Unpin
    {
        let proto_header_buff = StreamUtil::read_exact(reader, PROTOCOL_HEAD_LENGTH).await?;
        let proto_header = ProtocolHeader::try_from(proto_header_buff)?;
        Ok(proto_header)
    }
}

impl Protocol {
    // 接收协议，并保存到缓冲区中。
    pub async fn read_buff<T>(reader: &mut T) -> MQResult<Buff>
    where T: AsyncReadExt + Unpin {
        StreamUtil::read_exact(reader, PROTOCOL_HEAD_LENGTH).await
    }

    // 接收协议
    pub async fn read<T>(reader: &mut T) -> MQResult<Self>
        where T: AsyncReadExt + Unpin,
    {
        let mut proto_args = None;
        let mut proto_body = None;
        let proto_header_buff = StreamUtil::read_exact(reader, PROTOCOL_HEAD_LENGTH).await?;

        let proto_header = ProtocolHeader::try_from(proto_header_buff)?;
        let args_len = proto_header.args_len.clone() as usize;
        if args_len != 0 {
            let proto_args_buff = StreamUtil::read_exact(reader, args_len).await?;
            let proto_args_buff_len = proto_args_buff.len();
            if args_len != proto_args_buff_len {
                return Err(MQError::E(format!("the args len must be recv proto args buffer length")));
            }
            let origin_bytes = format!("bytes: {:?}", &proto_args_buff);
           
            use ProtocolHeaderType::*;
            proto_args = ProtocolArgs::make(proto_header.p_type.clone(), proto_args_buff)?;
        }
        let body_len = proto_header.body_len.clone() as usize;
        if proto_header.body_len != 0 {
            let proto_body_buff = StreamUtil::read_exact(reader, body_len).await?;
            proto_body = Some(proto_body_buff );
        }
        let nf_proto = Protocol {
            header: proto_header,
            args: proto_args,
            body: proto_body,
        };
        Ok(nf_proto)
    }


    //发送数据
    pub async fn send<T>(writer: &mut T, mut proto: Protocol) -> MQResult<()>
        where T: AsyncWriteExt + Unpin {
        debug!("ready send protocol: {:?}", &proto);
        let mut args_buff = vec![];
        let mut args_buff_len = 0u32;
        if let Some(args) = proto.args {
            args_buff = args.to_vec();
            args_buff_len = args_buff.len() as u32;
            proto.header.args_len = args_buff_len.clone();
        }
        let header_buff: Vec<u8> = proto.header.into();

        match StreamUtil::write_all(writer, header_buff).await {
            Err(e) => {return Err(MQError::E(format!("writer header failed.")));},
            _ => {},
        }
        if args_buff_len > 0 {
            match StreamUtil::write_all(writer, args_buff).await {
                Err(e) => {return Err(MQError::E(format!("writer protocol args buff failed.")));},
                _ => {},
            }
        }
        if let Some(body_buff) = proto.body {
            match StreamUtil::write_all(writer, body_buff).await {
                Err(e) => {return Err(MQError::E(format!("writer protocol body buff failed.")));},
                _ => {},
            }
        }
        Ok(())
    }

    // 生成一个相应数据
    pub fn new(p_type: ProtocolHeaderType, args: Option<ProtocolArgs>, body: Option<Buff>) -> Self {
        let mut body_len = 0u64;
        let mut args_len = 0u32;
        if body.is_some() {
            body_len = body.clone().unwrap().len() as u64;
        }
        if args.is_some() {
            args_len = args.clone().unwrap().len() as u32;
        }

        Self{
            header: ProtocolHeader {
                version: PROTOCOL_HEAD_VERSION,
                reverse: 0u8,
                p_type,
                args_len,
                body_len,
            },
            args,
            body
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
    pub fn update_error<E>(&mut self, code: i32, msg: E) where E: ToString {
        self.code = code;
        self.msg = msg.to_string();
    }
}


