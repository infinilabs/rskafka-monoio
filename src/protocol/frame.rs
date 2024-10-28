//! Read and write message frames from wire.
//!
//! # References
//! - <https://kafka.apache.org/protocol#protocol_common>

use std::{future::Future, io::Cursor};

use monoio::io::AsyncReadRent;
use monoio::io::AsyncReadRentExt;
use monoio::io::AsyncWriteRentExt;
use thiserror::Error;

use super::{
    primitives::Int32,
    traits::{ReadType, WriteType},
};

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ReadError {
    #[error("Cannot read data: {0}")]
    IO(#[from] std::io::Error),

    #[error("Negative message size: {size}")]
    NegativeMessageSize { size: i32 },

    #[error("Message too large, limit is {limit} bytes but got {actual} bytes")]
    MessageTooLarge { limit: usize, actual: usize },
}

pub trait AsyncMessageRead {
    fn read_message(
        &mut self,
        max_message_size: usize,
    ) -> impl Future<Output = Result<Vec<u8>, ReadError>>;
}

impl<R> AsyncMessageRead for R
where
    R: AsyncReadRent + Unpin,
{
    #[allow(clippy::read_zero_byte_vec)] // See https://github.com/rust-lang/rust-clippy/issues/9274
    async fn read_message(&mut self, max_message_size: usize) -> Result<Vec<u8>, ReadError> {
        let (result, len_buf) = self.read_exact(vec![0_u8; 4]).await;
        result?;

        let len = Int32::read(&mut Cursor::new(len_buf))
            .expect("Reading Int32 from in-mem buffer should always work");

        let len =
            usize::try_from(len.0).map_err(|_| ReadError::NegativeMessageSize { size: len.0 })?;

        // check max message size to not blow up memory
        if len > max_message_size {
            // We need to seek so that next message is readable. However `self.seek` would require `R: AsyncSeek` which
            // doesn't hold for many types we want to work with. So do some manual seeking.
            let mut to_read = len;
            let mut in_buf = vec![]; // allocate empty buffer
            while to_read > 0 {
                let step = max_message_size.min(to_read);

                // resize buffer if required
                in_buf.resize(step, 0);

                let (result_n_read, out_buf) = self.read_exact(in_buf).await;
                result_n_read?;
                in_buf = out_buf;
                to_read -= step;
            }

            return Err(ReadError::MessageTooLarge {
                limit: max_message_size,
                actual: len,
            });
        }

        let buf = vec![0u8; len];
        let (result_n_read, buf) = self.read_exact(buf).await;
        result_n_read?;
        Ok(buf)
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum WriteError {
    #[error("Cannot write data: {0}")]
    IO(#[from] std::io::Error),

    #[error("Message too large: {size}")]
    TooLarge { size: usize },
}

pub trait AsyncMessageWrite {
    fn write_message(&mut self, msg: Vec<u8>) -> impl Future<Output = Result<(), WriteError>>;
}

impl<W> AsyncMessageWrite for W
where
    W: AsyncWriteRentExt + Unpin,
{
    async fn write_message(&mut self, msg: Vec<u8>) -> Result<(), WriteError> {
        let mut len_buf = Vec::<u8>::with_capacity(4);
        let len =
            Int32(i32::try_from(msg.len()).map_err(|_| WriteError::TooLarge { size: msg.len() })?);
        len.write(&mut len_buf)
            .expect("Int32 should always be writable to in-mem buffer");

        let (result_n_written, _len_buf) = self.write_all(len_buf).await;
        result_n_written?;

        // empty writes seem to block forever on some IOs (e.g. tokio duplex)
        if !msg.is_empty() {
            let (result, _msg_buf) = self.write_all(msg).await;
            result?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use assert_matches::assert_matches;

    #[monoio::test]
    async fn test_read_negative_size() {
        let mut data = vec![];
        Int32(-1).write(&mut data).unwrap();

        let err = data.as_slice().read_message(100).await.unwrap_err();
        assert_matches!(err, ReadError::NegativeMessageSize { .. });
        assert_eq!(err.to_string(), "Negative message size: -1");
    }

    // TODO: enable the following test once we have https://github.com/bytedance/monoio/issues/310
    // #[monoio::test]
    // async fn test_read_too_large() {
    //     let mut data : Vec<u8> = vec![];
    //     data.write_message("foooo".as_bytes()).await.unwrap();
    //     data.write_message("bar".as_bytes()).await.unwrap();

    //     let mut stream = Cursor::new(data);

    //     let err = stream.read_message(3).await.unwrap_err();
    //     assert_matches!(err, ReadError::MessageTooLarge { .. });
    //     assert_eq!(
    //         err.to_string(),
    //         "Message too large, limit is 3 bytes but got 5 bytes"
    //     );

    //     // second message should still be readable
    //     let data = stream.read_message(3).await.unwrap();
    //     assert_eq!(&data, "bar".as_bytes());
    // }

    // #[monoio::test]
    // async fn test_write_too_large() {
    //     let mut stream = vec![];
    //     let msg = vec![0u8; (i32::MAX as usize) + 1];
    //     let err = stream.write_message(&msg).await.unwrap_err();
    //     assert_matches!(err, WriteError::TooLarge { .. });
    //     assert_eq!(err.to_string(), "Message too large: 2147483648");
    // }

    // #[monoio::test]
    // async fn test_roundtrip_empty_cursor() {
    //     let mut data = Cursor::new(vec![]);
    //     data.write_message(&[]).await.unwrap();

    //     data.set_position(0);
    //     let actual = data.read_message(0).await.unwrap();
    //     assert_eq!(actual, vec![]);
    // }

    // #[monoio::test]
    // async fn test_roundtrip_empty_duplex() {
    //     let (mut server, mut client) = tokio::io::duplex(4);
    //     client.write_message(&[]).await.unwrap();

    //     let actual = server.read_message(0).await.unwrap();
    //     assert_eq!(actual, vec![]);
    // }
}
