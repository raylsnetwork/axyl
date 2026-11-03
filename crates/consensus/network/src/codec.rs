//! Codec for encoding/decoding consensus network messages.

use crate::PeerExchangeMap;
use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{request_response::Codec, StreamProtocol};
use rayls_infrastructure_types::encode_into_buffer;
use serde::{de::DeserializeOwned, Serialize};
use snap::read::FrameDecoder;
use std::{
    fmt,
    io::{Read as _, Write as _},
    marker::PhantomData,
};

#[cfg(test)]
#[path = "tests/codec_tests.rs"]
mod codec_tests;

/// Convenience type for all traits implemented for messages used for RL request-response codec.
pub trait RLMessage:
    Send + Serialize + DeserializeOwned + Clone + fmt::Debug + From<PeerExchangeMap> + 'static
{
    /// Function to intercept peer exchange messages at the network layer before passing to the
    /// application layer. Only the network layer needs peer exchange messages.
    fn peer_exchange_msg(&self) -> Option<PeerExchangeMap>;
}

/// The Rayls Network request/response codec for consensus messages between peers.
///
/// The codec reuses its buffers across messages to asynchronously read and write per the libp2p
/// [Codec] trait. All messages include a 4-byte prefix that indicates the message's uncompressed
/// length. The decode buffer grows only as decompressed bytes are produced, so a peer that
/// sends only the length prefix reserves no buffer.
#[derive(Clone, Debug)]
pub struct RLCodec<Req, Res> {
    /// The buffer for compressed message data.
    compressed_buffer: Vec<u8>,
    /// The buffer for decompressed message data.
    ///
    /// Grows only as decompressed bytes are produced; it is never pre-sized to the declared
    /// length, so a prefix-only or stalled request reserves no memory.
    decode_buffer: Vec<u8>,
    /// The maximum size (bytes) for a single message.
    ///
    /// The 4-byte message prefix does not count towards this value.
    max_chunk_size: usize,
    /// Phantom data for codec that indicates network message type.
    _phantom: PhantomData<(Req, Res)>,
}

impl<Req, Res> RLCodec<Req, Res> {
    pub fn new(max_chunk_size: usize) -> Self {
        let max_compress_len = snap::raw::max_compress_len(max_chunk_size);
        let compressed_buffer = Vec::with_capacity(max_compress_len);
        // allocate capacity for decoding max message size
        let decode_buffer = Vec::with_capacity(max_chunk_size);

        Self {
            compressed_buffer,
            decode_buffer,
            max_chunk_size,
            _phantom: PhantomData::<(Req, Res)>,
        }
    }

    /// Convenience method to keep READ logic DRY.
    ///
    /// This method is used to read requests and responses from peers.
    #[inline]
    async fn decode_message<T, M>(&mut self, io: &mut T) -> std::io::Result<M>
    where
        T: AsyncRead + Unpin + Send,
        M: RLMessage,
    {
        self.compressed_buffer.clear();
        self.decode_buffer.clear();

        let mut prefix = [0; 4];
        io.read_exact(&mut prefix).await?;

        // NOTE: cast u32 to usize is safe
        let length = u32::from_le_bytes(prefix) as usize;

        if length > self.max_chunk_size {
            return Err(std::io::Error::other("prefix indicates message size is too large"));
        }

        // take max possible compression size based on reported length
        // this is used to limit the amount read in case peer used malicious prefix
        //
        // NOTE: usize -> u64 won't lose precision (even on 32bit system)
        let max_compress_len = snap::raw::max_compress_len(length);
        io.take(max_compress_len as u64).read_to_end(&mut self.compressed_buffer).await?;

        // decompress into `decode_buffer`, capping the output at the declared length so
        // snappy amplification can't exceed it
        //
        // NOTE: the buffer is not pre-sized to the declared length; it grows as bytes arrive,
        // so a prefix-only or stalled request reserves no memory
        let reader = std::io::Cursor::new(&mut self.compressed_buffer);
        let snappy_decoder = FrameDecoder::new(reader);
        snappy_decoder.take(length as u64).read_to_end(&mut self.decode_buffer)?;

        // the prefix declares the exact uncompressed length; shorter data is a protocol violation
        if self.decode_buffer.len() < length {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "decompressed data smaller than declared prefix",
            ));
        }

        bcs::from_bytes(&self.decode_buffer).map_err(std::io::Error::other)
    }

    /// Convenience method to keep WRITE logic DRY.
    ///
    /// This method is used to write requests and responses from peers.
    #[inline]
    async fn encode_message<T, M>(&mut self, io: &mut T, msg: M) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
        M: RLMessage,
    {
        self.compressed_buffer.clear();
        self.decode_buffer.clear();

        encode_into_buffer(&mut self.decode_buffer, &msg).map_err(|e| {
            let error = format!("encode into buffer: {e}");
            std::io::Error::other(error)
        })?;

        if self.decode_buffer.len() > self.max_chunk_size {
            return Err(std::io::Error::other("encode data > max_chunk_size"));
        }

        // length prefix for uncompressed bytes
        //
        // NOTE: 32bit max 4,294,967,295
        let prefix = (self.decode_buffer.len() as u32).to_le_bytes();
        io.write_all(&prefix).await?;

        // compress data using allocated buffer
        let mut encoder = snap::write::FrameEncoder::new(&mut self.compressed_buffer);
        encoder.write_all(&self.decode_buffer)?;
        encoder.flush()?;

        io.write_all(encoder.get_ref()).await?;

        Ok(())
    }
}

#[async_trait]
impl<Req, Res> Codec for RLCodec<Req, Res>
where
    Req: RLMessage,
    Res: RLMessage,
{
    type Protocol = StreamProtocol;
    type Request = Req;
    type Response = Res;

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        self.decode_message(io).await
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        self.decode_message(io).await
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        self.encode_message(io, req).await
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        self.encode_message(io, res).await
    }
}
