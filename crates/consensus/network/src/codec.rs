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
/// The codec is stateless apart from the maximum message size. Each read or write allocates
/// temporary buffers for that message only, so memory is released as soon as the call completes
/// or is cancelled. All messages include a 4-byte prefix that indicates the message's
/// uncompressed length. Buffers grow only as data is produced, so a peer that sends only the
/// length prefix reserves no buffer.
#[derive(Clone, Debug)]
pub struct RLCodec<Req, Res> {
    /// The maximum size (bytes) for a single message.
    ///
    /// The 4-byte message prefix does not count towards this value.
    max_chunk_size: usize,
    /// Phantom data for codec that indicates network message type.
    _phantom: PhantomData<(Req, Res)>,
}

impl<Req, Res> RLCodec<Req, Res> {
    pub fn new(max_chunk_size: usize) -> Self {
        // Buffers are allocated per message rather than stored here: libp2p clones the codec
        // for every substream, and a substream exchanges only one request/response, so reuse
        // would be minimal while large buffers would be retained between calls.
        Self { max_chunk_size, _phantom: PhantomData::<(Req, Res)> }
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
        // Scoped to this message so a large or partially-read message does not pin memory on
        // the codec after the call completes or is cancelled.
        let mut compressed_buffer: Vec<u8> = Vec::new();
        let mut decode_buffer: Vec<u8> = Vec::new();

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
        io.take(max_compress_len as u64).read_to_end(&mut compressed_buffer).await?;

        // decompress into `decode_buffer`, capping the output at the declared length so
        // snappy amplification can't exceed it
        //
        // NOTE: the buffer is not pre-sized to the declared length; it grows as bytes arrive,
        // so a prefix-only or stalled request reserves no memory
        let reader = std::io::Cursor::new(&mut compressed_buffer);
        let snappy_decoder = FrameDecoder::new(reader);
        snappy_decoder.take(length as u64).read_to_end(&mut decode_buffer)?;

        // the prefix declares the exact uncompressed length; shorter data is a protocol violation
        if decode_buffer.len() < length {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "decompressed data smaller than declared prefix",
            ));
        }

        bcs::from_bytes(&decode_buffer).map_err(std::io::Error::other)
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
        // Scoped to this message so large encode buffers are not retained on the codec.
        let mut encoded: Vec<u8> = Vec::new();

        encode_into_buffer(&mut encoded, &msg).map_err(|e| {
            let error = format!("encode into buffer: {e}");
            std::io::Error::other(error)
        })?;

        if encoded.len() > self.max_chunk_size {
            return Err(std::io::Error::other("encode data > max_chunk_size"));
        }

        // `encoded` is our own size-checked message, so the maximum compressed size is known.
        // Allocate once to avoid repeated reallocations for large messages.
        let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(encoded.len()));

        // length prefix for uncompressed bytes
        //
        // NOTE: 32bit max 4,294,967,295
        let prefix = (encoded.len() as u32).to_le_bytes();
        io.write_all(&prefix).await?;

        // compress data using allocated buffer
        let mut encoder = snap::write::FrameEncoder::new(&mut compressed_buffer);
        encoder.write_all(&encoded)?;
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
