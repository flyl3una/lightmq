use crate::err::{MQResult, MQError};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone)]
pub struct ServerContext{
    // rx: 接收消息，并将消息写入到消息队列
    // pub rx: mpsc::Receiver<MQMessage>,
    // 消息队列
    // pub queue: mpsc::Receiver<MQMessage>


}

// #[derive(Clone)]
pub struct LocalContext{
    // tx: 将收到的消息发送到消息队列中
    // pub tx: mpsc::Sender<MQMessage>,
    // 客户端链接，接收协议头，区分是注册，还是发送消息。
    pub stream: TcpStream,
}

// 监听地址，并接收数据
pub struct Connector {
    pub listen: String,

}

impl Connector {
    pub fn new(listen: String) -> Connector {
        Connector { listen }
    }

    pub async fn run(&self) -> MQResult<()> {
        // 如果下一跳地址存在，则连接下一跳地址
        // 监听本地地址
        self.listen().await
    }

    pub async fn listen(&self) -> MQResult<()> {
        let listen = self.listen.clone();

        match TcpListener::bind(&listen).await {
            Ok(listener) => {
                info!("listen address: {}", &listen);
                let context = ServerContext{};
                loop {
                    match listener.accept().await {
                        Ok((mut socket, peer)) => {
                            info!("accept connect [{}:{}]", peer.ip(), peer.port());
                            let client_context = LocalContext{ stream: socket };
                            self.spawn_handle(context.clone(), client_context).await;
                        }
                        Err(e) => return Err(MQError::IoError(e.to_string())),
                    }
                }
            }
            Err(e) => Err(MQError::E(e.to_string())),
        }
    }

    pub async fn spawn_handle(
        &self,
        mut server_context: ServerContext,
        mut client_context: LocalContext,
    ) -> MQResult<()>
    {
        // let client_tls = self.proxy_param.client_tls.clone();
        tokio::spawn(async move {
            // 接收数据并处理协议
            println!("end connector.");

        });
        Ok(())
    }

}