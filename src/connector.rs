use crate::{
    connect_handle::handle_connect,
    err::{MQError, MQResult},
    protocol::Protocol,
    session::SessionManagerRequest,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::Sender,
};

#[derive(Clone)]
pub struct ServerContext {
    // rx: 接收消息，并将消息写入到消息队列
    // pub rx: mpsc::Receiver<MQMessage>,
    // 消息队列
    // pub queue: mpsc::Receiver<MQMessage>
    // 用来向session管理器发送消息。
    pub tx: Sender<SessionManagerRequest>,
}

// #[derive(Clone)]
pub struct LocalContext {
    // tx: 将收到的消息发送到消息队列中
    // pub tx: mpsc::Sender<MQMessage>,
    // 客户端链接，接收协议头，区分是注册，还是发送消息。
    pub stream: TcpStream,
}

// 监听地址，并接收数据
pub struct Connector {
    pub listen: String,
    pub context: Option<ServerContext>,
}

impl Connector {
    pub fn new(listen: String) -> Connector {
        Connector {
            listen,
            context: None,
        }
    }

    pub fn build_context(&mut self, tx: Sender<SessionManagerRequest>) {
        self.context = Some(ServerContext { tx })
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
                if self.context.is_none() {
                    return Err(MQError::E("must build context.".to_string()));
                }
                let context = self.context.clone().unwrap();
                loop {
                    match listener.accept().await {
                        Ok((mut socket, peer)) => {
                            info!("accept connect [{}:{}]", peer.ip(), peer.port());
                            let client_context = LocalContext { stream: socket };
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
    ) -> MQResult<()> {
        tokio::spawn(async move {
            // 接收数据并处理协议
            // 接收协议数据，并根据协议类型进行分发
            let peer_addr = match client_context.stream.peer_addr() {
                Ok(ref peer) => peer.to_string(),
                Err(e) => {
                    error!("get connect peer address failed.\n\terror:{}", e);
                    return;
                }
            };
            match handle_connect(server_context, client_context).await {
                Ok(t) => {}
                Err(e) => {
                    error!(
                        "handle connect failed, peer addr: {}, \n\terror: {}",
                        peer_addr, e
                    )
                }
            }
            info!("end connector.");
        });
        Ok(())
    }
}
