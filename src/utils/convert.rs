use std::any::Any;
use std::error::Error;
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