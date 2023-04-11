use std::{any::Any, convert::TryInto};
use std::error::Error;

use crate::err::{MQError, MQResult};
// use std::str::pattern::Pattern;


pub struct StringUtil{}

impl StringUtil {

    pub fn option_str2option_string(source: Option<&str>) -> Option<String> {
        match source {
            Some(s) => Some(s.to_string()),
            None => None
        }
    }

    pub fn result_str2result_string(source: Result<&str, Box<dyn Error>>) -> Result<String, Box<dyn Error>> {
        match source {
            Ok(s) => Ok(s.to_string()),
            Err(e) => Err(e),
        }
    }

    pub fn string_to_vec(source: String, split_byte: &str) -> Vec<String> {
        let s: Vec<String> = source.split(",").map(|s| s.to_string()).collect();
        s
    }

}

pub struct VecUtil{}

impl VecUtil {
    pub fn str_to_string<T>(source: Vec<T>) -> Vec<String>
    where T: ToString {
        let mut rst = vec![];
        for item in source {
            rst.push(item.to_string())
        }
        rst
    }
}

pub struct BuffUtil {}
impl BuffUtil {
    pub fn buff_to_f64(buff: Vec<u8>) -> MQResult<f64> {
        if buff.len() != 8 {
            return Err(MQError::E(format!("the args len must be float(8)")));
        }
        let buf: [u8; 8] = buff[0..8].try_into()
        .map_err(|e| MQError::ConvertError(format!("the buff try into [u8; 8] failed.\n\terror:{}", e)))?;
        let f = f64::from_be_bytes(buf);
        Ok(f)
    }

    pub fn buff_to_i32(buff: Vec<u8>) -> MQResult<i32> {
        if buff.len() != 4 {
            return Err(MQError::E(format!("the args len must be i32(4)")));
        }
        let buf: [u8; 4] = buff[0..4].try_into()
        .map_err(|e| MQError::ConvertError(format!("the buff try into [u8; 4] failed.\n\terror:{}", e)))?;
        let x = i32::from_be_bytes(buf);
        Ok(x)
    }

    pub fn buff_to_str(buff: Vec<u8>) -> MQResult<String> {
        let s = String::from_utf8(buff)
        .map_err(|e|MQError::ConvertError(format!("the buff convert str failed.\n\terror: {}", e)))?;
        Ok(s)
    }

    pub fn buff_to_bool(buff: Vec<u8>) -> MQResult<bool> {
        if buff.len() != 1 {
            return Err(MQError::E(format!("the args len must be bool(1)")));
        }
        let b = buff[0] != 0;
        Ok(b)
    }

    
}