use tokio::sync::mpsc::{Sender, self};

use crate::err::{MQError, MQResult};

pub const CHANNEL_BUFF_LEN: usize = 8;

pub struct ChannelUtil {}

impl ChannelUtil {
    // 通过tx发送请求，得到返回结果
    pub async fn request<S, R>(tx: &Sender<S>, data: S) -> MQResult<R> {
        let (mut tx1, mut rx1) = mpsc::channel::<R>(CHANNEL_BUFF_LEN);
        tx.send(data).await
            .map_err(|e|MQError::E(format!("send failed. error: {}", e)))?;
        // 接收响应
        // 监听rx，接收session manager回应数据
        match rx1.recv().await {
            Some(res) => {
                Ok(res)
            },
            None => {Err(MQError::E("not recv session manager response.".to_string()))}
        }
    }
}
