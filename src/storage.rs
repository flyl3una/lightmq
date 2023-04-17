use std::{io::SeekFrom, path::PathBuf, pin::Pin};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeek, AsyncWriteExt},
};

use crate::{
    err::{MQError, MQResult},
    message::{Message, MessageIndex},
    utils::stream::StreamUtil,
};

// 存储数据
#[derive(Debug)]
pub struct Store {
    pub queue: Vec<Message>,
}

impl Store {
    pub fn new() -> Store {
        Store { queue: Vec::new() }
    }

    pub fn push(&mut self, message: Message) {
        self.queue.push(message);
    }

    pub fn pop(&mut self) -> Option<Message> {
        self.queue.pop()
    }

    // 消息个数
    pub fn message_number(&mut self) -> usize {
        self.queue.len()
    }
}

#[derive(Debug)]
pub struct FileStore {
    // 文件名
    // pub directory: String,
    pub directory: String,
    pub name: String,
    // 索引下标
    pub file: File,
    pub index_file: File,

    // 当前写入偏移量
    pub write_offset: u64,
    // 当前读取偏移量
    pub reader_offset: u64,

    pub message_length: u64,
}

impl FileStore {
    pub async fn new(directory: String, name: String) -> MQResult<FileStore> {
        let dir = PathBuf::from(&directory);
        let path = dir.join(&name);

        let file = File::open(&path)
            .await
            .map_err(|e| MQError::IoError(format!("not found file: {:?}", path.as_os_str())))?;

        let index_name = format!("{}.idx", &name);
        let index_path = PathBuf::from(&directory).join(&index_name);
        let index_file = File::open(&index_path).await.map_err(|e| {
            MQError::IoError(format!("not found file: {:?}", index_path.as_os_str()))
        })?;
        Ok(Self {
            directory,
            name,
            file,
            index_file,
            write_offset: 0u64,
            reader_offset: 0u64,
            message_length: 0u64,
        })
    }

    pub async fn push(&mut self, message: Message) -> MQResult<()> {
        // StreamUtil::write_all(&mut self.file, message)?;
        let buff: Vec<u8> = message.into();
        self.file.write_all(&buff[..]).await?;
        let message_index = MessageIndex::new(self.write_offset.clone(), buff.len() as u64);
        let index_buff: Vec<u8> = message_index.into();
        self.index_file.write_all(&index_buff[..]).await?;
        self.message_length += 1;
        Ok(())
    }

    pub async fn pop(&mut self) -> MQResult<Message> {
        // self.file.
        // let f = Pin::new(&mut self.file);
        // f.start_seek(SeekFrom::Start(8))?;
        let mut buf = vec![0u8; 16];
        self.file.read_exact(&mut buf[..]).await?;
        let message_index = MessageIndex::try_from(buf)?;

        let mut msg_buf = vec![0u8; message_index.length.clone() as usize];
        self.file.read_exact(&mut msg_buf[..]).await?;
        let message = Message::try_from(msg_buf)?;
        self.message_length -= 1;
        Ok(message)
    }

    pub fn message_number(&self) -> usize {
        self.message_length.clone() as usize
    }
}
