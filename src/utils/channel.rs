use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::err::{MQError, MQResult};

pub const CHANNEL_BUFF_LEN: usize = 8;

pub struct ChannelUtil {}

impl ChannelUtil {
    // 通过tx发送请求，得到返回结果
    pub async fn request<S, R>(tx: &Sender<S>, rx: &mut Receiver<R>, data: S) -> MQResult<R> {
        tx.send(data)
            .await
            .map_err(|e| MQError::E(format!("send failed. error: {}", e)))?;
        // 接收响应
        // 监听rx，接收session manager回应数据
        match rx.recv().await {
            Some(res) => Ok(res),
            None => Err(MQError::E("not recv session manager response.".to_string())),
        }
    }
}
