use crate::err::MQError;
use crate::err::MQResult;
use bytes::{BufMut, BytesMut};
use tokio::time::Duration;
// use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
// use tokio_rustls::server::TlsStream;
// use tokio_rustls::TlsStream;
use futures::task::Context;
use std::io;
use std::io::Read;
use std::pin::Pin;
use tokio::macros::support::Poll;
use tokio::net::TcpStream;

pub type Buff = Vec<u8>;
pub static NF_BUFF_LEN: usize = 1024 * 8;

pub struct StreamUtil {}

impl StreamUtil {
    //
    /// ```no_run
    /// use tokio::io::{BufReader, BufWriter};
    /// use tokio::net::{TcpStream};
    /// use tokio::net::tcp::{WriteHalf, ReadHalf};
    /// use tokio::net::TcpStream;
    /// let mut socket = TcpStream::connect("127.0.0.1:8000").await?;
    /// let (mut reader, mut writer) = socket.split();
    /// let mut socket_reader: BufReader<ReadHalf> = BufReader::new(reader);
    /// let mut socket_writer: BufWriter<WriteHalf> = BufWriter::new(writer);
    /// // 注意buff长度最大为NF_BUFF_LEN，超出长度需要多次读取。
    /// let buf = read_all(socket_reader).await;
    ///
    /// ```
    /// ReaderHalf
    pub async fn read_all<T>(stream: &mut T) -> MQResult<Buff>
    where
        T: AsyncReadExt + Unpin,
    {
        let buff_len = NF_BUFF_LEN;
        let mut buf: Vec<u8> = vec![];
        let mut buff = BytesMut::with_capacity(buff_len);

        // read_buf 读取到0时表示结束。
        match stream.read_buf(&mut buff).await {
            Ok(n) => {
                if n == 0 {
                    return Err(MQError::IoError(format!("reader stream break.")));
                }
                debug!("recv length: {}", &n);
                return Ok(buff[..n].to_vec());
            }
            // 连接已断开
            Err(e) => return Err(MQError::IoError(e.to_string())),
        }
    }

    //
    /// ```no_run
    /// use tokio::io::{BufReader, BufWriter};
    /// use tokio::net::{TcpSocket, TcpStream};
    /// let mut socket = TcpStream::connect("127.0.0.1:8000").await?;
    /// let (mut reader, mut writer) = socket.split();
    /// let mut socket_reader = BufReader::new(reader);
    /// let mut socket_writer = BufWriter::new(writer);
    /// let buf = read_exact(socket_reader, 1024).await;
    ///
    /// ```
    /// ReaderHalf
    pub async fn read_exact<T>(stream: &mut T, length: usize) -> MQResult<Buff>
    where
        T: AsyncReadExt + Unpin,
    {
        let mut buff = vec![0u8; length];
        match stream.read_exact(buff.as_mut_slice()).await {
            Ok(n) => Ok(buff.to_vec()),
            Err(e) => Err(MQError::IoError(e.to_string())),
        }
    }

    //
    /// ```no_run
    /// use tokio::io::{BufReader, BufWriter};
    /// use tokio::net::{TcpSocket, TcpStream};
    /// let mut socket = TcpStream::connect("127.0.0.1:8000").await?;
    /// let (mut reader, mut writer) = socket.split();
    /// let mut socket_reader = BufReader::new(reader);
    /// let mut socket_writer = BufWriter::new(writer);
    /// let buf = read_exact(socket_reader, 1024).await;
    ///
    /// ```
    /// ReaderHalf
    // pub async fn read_line<T>(stream: &mut T) -> MQResult<String>
    //     where
    //         T: AsyncReadExt + Unpin,
    // {
    //     let mut buff = "".to_string();
    //     match stream.read_line(&mut buff).await {
    //         Ok(n) => {
    //             Ok(buff)
    //         },
    //         Err(e) => Err(MQError::IoError(e.to_string())),
    //     }
    // }
    //
    /// ```no_run
    /// use tokio::io::{BufReader, BufWriter};
    /// use tokio::net::{TcpStream};
    /// use tokio::net::tcp::{WriteHalf, ReadHalf};
    /// let mut socket = TcpStream::connect("127.0.0.1:8000").await?;
    /// let (mut reader, mut writer) = socket.split();
    /// let mut socket_reader: BufReader<ReadHalf> = BufReader::new(reader);
    /// let mut socket_writer: BufWriter<WriteHalf> = BufWriter::new(writer);
    /// // 注意buff长度最大为NF_BUFF_LEN，超出长度需要多次读取。
    /// let buff: Vec<u8> = vec![1,2,3];
    /// let buf = write_all(socket_writer, buff).await;
    /// ```
    // OwnedWriteHalf
    pub async fn write_all<T>(stream: &mut T, buff: Buff) -> MQResult<()>
    where
        T: AsyncWriteExt + Unpin,
    {
        match stream.write_all(&buff[..]).await {
            Err(e) => Err(MQError::IoError(e.to_string())),
            _ => {
                debug!("send buff successful. buff length: {}", buff.len());
                stream.flush().await;
                Ok(())
            }
        }
    }

    pub async fn proxy_io<T>(source_socket: &mut T, target_socket: &mut T) -> MQResult<()>
    where
        T: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        loop {
            tokio::select! {
                recv_buff_opt = StreamUtil::read_all(source_socket) => {
                    StreamUtil::write_all(target_socket, recv_buff_opt?).await?
                },
                recv_buff_opt = StreamUtil::read_all(target_socket) => {
                    StreamUtil::write_all(source_socket, recv_buff_opt?).await?
                }
            }
        }
    }
}
//
// pub enum SocketStream {
//     Socket(TcpStream),
//     TlsSocket(TlsStream<TcpStream>),
// }
//
// impl SocketStream {
//
//     pub fn get_inner(&mut self) -> &mut TcpStream {
//         match self {
//             SocketStream::Socket(x) => x,
//             SocketStream::TlsSocket(x) => x.get_mut().0,
//         }
//     }
//
//
// }
//
// impl AsyncRead for SocketStream {
//     fn poll_read(
//         self: Pin<&mut Self>,
//         cx: &mut Context<'_>,
//         buf: &mut ReadBuf<'_>,
//     ) -> Poll<io::Result<()>> {
//         match self.get_mut() {
//             SocketStream::Socket(x) => {Pin::new(x).poll_read(cx, buf)},
//             SocketStream::TlsSocket(x) => {Pin::new(x).poll_read(cx, buf)}
//         }
//     }
// }
//
// impl AsyncWrite for SocketStream {
//     fn poll_write(
//         self: Pin<&mut Self>,
//         cx: &mut Context<'_>,
//         buf: &[u8],
//     ) -> Poll<io::Result<usize>> {
//         match self.get_mut() {
//             SocketStream::Socket(x) => {Pin::new(x).poll_write(cx, buf)},
//             SocketStream::TlsSocket(x) => {Pin::new(x).poll_write(cx, buf)}
//         }
//     }
//
//     fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
//         match self.get_mut() {
//             SocketStream::Socket(x) => {Pin::new(x).poll_flush(cx)},
//             SocketStream::TlsSocket(x) => {Pin::new(x).poll_flush(cx)}
//         }
//     }
//
//     fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
//         match self.get_mut() {
//             SocketStream::Socket(x) => {Pin::new(x).poll_shutdown(cx)},
//             SocketStream::TlsSocket(x) => {Pin::new(x).poll_shutdown(cx)}
//         }
//     }
// }
