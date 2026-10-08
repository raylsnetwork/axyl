//! RLCodec tests used by the consensus network libp2p req/res protocol.

use super::*;
use crate::{
    common::{TestPrimaryRequest, TestPrimaryResponse},
    RLCodec,
};
use libp2p::StreamProtocol;
use rayls_infrastructure_types::{Certificate, CertificateDigest, Header};

#[tokio::test]
async fn test_encode_decode_same_message() {
    let max_chunk_size = 1024 * 1024; // 1mb
    let mut codec = RLCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(max_chunk_size);
    let protocol = StreamProtocol::new("/rayls-test");

    // encode request
    let mut encoded = Vec::new();
    let request = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default()],
    };
    codec
        .write_request(&protocol, &mut encoded, request.clone())
        .await
        .expect("write valid request");

    // now decode request
    let decoded =
        codec.read_request(&protocol, &mut encoded.as_ref()).await.expect("read valid request");
    assert_eq!(decoded, request);

    // encode response
    let mut encoded = Vec::new();
    let response = TestPrimaryResponse::MissingParents(vec![CertificateDigest::new([b'a'; 32])]);
    codec
        .write_response(&protocol, &mut encoded, response.clone())
        .await
        .expect("write valid response");

    // now decode response
    let decoded =
        codec.read_response(&protocol, &mut encoded.as_ref()).await.expect("read valid response");
    assert_eq!(decoded, response);
}

#[tokio::test]
async fn test_fail_to_write_message_too_big() {
    let max_chunk_size = 100; // 100 bytes is too small
    let mut codec = RLCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(max_chunk_size);
    let protocol = StreamProtocol::new("/rayls-test");

    // encode request
    let mut encoded = Vec::new();
    let request = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default()],
    };
    let res = codec.write_request(&protocol, &mut encoded, request).await;
    assert!(res.is_err());

    // encode response
    let mut encoded = Vec::new();
    let response = TestPrimaryResponse::MissingCertificates(vec![Certificate::default()]);
    let res = codec.write_response(&protocol, &mut encoded, response).await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_reject_message_prefix_too_big() {
    let max_chunk_size = 344; // 344 bytes
    let mut honest_peer = RLCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(max_chunk_size);
    let protocol = StreamProtocol::new("/rayls-test");
    // malicious peer writes legit messages that are too big
    // "legit" means correct prefix and valid data. the only problem is message too big for
    // receiving peer
    let mut malicious_peer = RLCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(1024 * 1024);

    //
    // test requests first
    //
    // sanity check
    let mut encoded = Vec::new();

    //println!("size: {}", std::mem::size_of::<TestPrimaryRequest>());
    // this is 344 bytes uncompressed (max chunk size)
    let request = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default()],
    };
    malicious_peer
        .write_request(&protocol, &mut encoded, request.clone())
        .await
        .expect("write legit and valid request");
    let decoded = honest_peer
        .read_request(&protocol, &mut encoded.as_ref())
        .await
        .expect("read valid request");
    assert_eq!(decoded, request);

    // now encode legit message that's too big for honest peer
    let mut encoded = Vec::new();
    // this is 344 bytes uncompressed
    let big_request = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default(), Certificate::default()],
    };
    malicious_peer
        .write_request(&protocol, &mut encoded, big_request)
        .await
        .expect("write legit request");
    // prefix length should cause error
    let res = honest_peer.read_request(&protocol, &mut encoded.as_ref()).await;
    assert!(res.is_err());

    //
    // test the same for responses
    //
    // sanity check that block within bounds works
    let mut encoded = Vec::new();
    // 138 bytes uncompressed
    let response = TestPrimaryResponse::MissingCertificates(vec![Certificate::default()]);
    malicious_peer
        .write_response(&protocol, &mut encoded, response.clone())
        .await
        .expect("write legit and valid response");
    let decoded = honest_peer
        .read_response(&protocol, &mut encoded.as_ref())
        .await
        .expect("read valid response");
    assert_eq!(decoded, response);

    // now encode legit message that's too big for honest peer
    let mut encoded = Vec::new();
    // > 416 bytes uncompressed
    let big_response = TestPrimaryResponse::MissingCertificates(vec![
        Certificate::default(),
        Certificate::default(),
        Certificate::default(),
        Certificate::default(),
    ]);
    malicious_peer
        .write_response(&protocol, &mut encoded, big_response)
        .await
        .expect("write legit response");
    // prefix length should cause error
    let res = honest_peer.read_response(&protocol, &mut encoded.as_ref()).await;
    assert!(res.is_err())
}

#[tokio::test]
async fn test_malicious_prefix_deceives_peer_to_read_message_and_fails() {
    let max_chunk_size = 208; // 208 bytes max message size
    let mut honest_peer = RLCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(max_chunk_size);
    let protocol = StreamProtocol::new("/rayls-test");
    // malicious peer writes legit messages that are too big
    // "legit" means correct prefix and valid data. the only problem is message too big
    let mut malicious_peer = RLCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(1024 * 1024);

    //
    // test requests first
    //
    // encode valid message that's too big and change prefix to deceive peer into trying to read
    // content
    let mut encoded = Vec::new();
    // this is 344 bytes uncompressed
    // but only 74 bytes compressed (within max size)
    let big_request = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default(), Certificate::default()],
    };
    malicious_peer
        .write_request(&protocol, &mut encoded, big_request)
        .await
        .expect("write legit request");
    // assert prefix is greater than peer's max chunk size
    let mut actual_prefix = [0; 4];
    actual_prefix.clone_from_slice(&encoded[0..4]);
    let honest_length = u32::from_le_bytes(actual_prefix) as usize;

    // sanity check
    assert!(honest_length > max_chunk_size);
    assert!(encoded.len() < max_chunk_size);

    // manipulate prefix to obfuscate actual message size is too big
    // this sets prefix to the honest peer's max message length,
    // which is considered valid and within message size bounds
    encoded[0..4].clone_from_slice(&100u32.to_le_bytes());

    // should cause an unexpected EOF
    let res = honest_peer.read_request(&protocol, &mut encoded.as_ref()).await;
    assert!(res.is_err());

    //
    // test responses first
    //
    // encode valid message that's too big and change prefix to deceive peer into trying to read
    // content
    let mut encoded = Vec::new();
    // this is 274 bytes uncompressed (more than max)
    // but only 62 bytes compressed (within max size)
    let big_response = TestPrimaryResponse::MissingCertificates(vec![
        Certificate::default(),
        Certificate::default(),
    ]);
    malicious_peer
        .write_response(&protocol, &mut encoded, big_response)
        .await
        .expect("write legit response");
    // assert prefix is greater than peer's max chunk size
    let mut actual_prefix = [0; 4];
    actual_prefix.clone_from_slice(&encoded[0..4]);
    let honest_length = u32::from_le_bytes(actual_prefix) as usize;

    // sanity check
    assert!(honest_length > max_chunk_size);
    assert!(encoded.len() < max_chunk_size);

    // manipulate prefix to obfuscate actual message size is too big
    // this sets prefix to the honest peer's max message length,
    // which is considered valid and within message size bounds
    encoded[0..4].clone_from_slice(&100u32.to_le_bytes());

    // should cause an unexpected EOF
    let res = honest_peer.read_response(&protocol, &mut encoded.as_ref()).await;
    assert!(res.is_err());
}

/// The default maximum message size a node accepts, taken from the network config rather than
/// hard-coded. A request may declare up to this many bytes, so the codec is built with this limit.
fn max_message_size() -> usize {
    rayls_infrastructure_config::LibP2pConfig::default().max_rpc_message_size
}

/// How long to let a stalled read run before checking the codec's memory. The body never arrives,
/// so the read stays blocked the whole time. This is far longer than a healthy local read needs.
const STALLED_READ_WAIT: std::time::Duration = std::time::Duration::from_millis(200);

/// A 4-byte little-endian prefix that declares a message of `size` bytes.
fn size_prefix(size: usize) -> [u8; 4] {
    (size as u32).to_le_bytes()
}

/// Total memory reserved by the codec buffers.
fn reserved_bytes(codec: &RLCodec<TestPrimaryRequest, TestPrimaryResponse>) -> usize {
    codec.decode_buffer.capacity() + codec.compressed_buffer.capacity()
}

/// A stream that sends a length prefix and then never sends anything else.
struct StalledStream {
    prefix: [u8; 4],
    sent: usize,
}

impl futures::AsyncRead for StalledStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.sent >= self.prefix.len() {
            // The peer stops sending, so the read waits forever.
            return std::task::Poll::Pending;
        }
        let n = buf.len().min(self.prefix.len() - self.sent);
        let start = self.sent;
        buf[..n].copy_from_slice(&self.prefix[start..start + n]);
        self.sent += n;
        std::task::Poll::Ready(Ok(n))
    }
}

// A correct codec grows its buffer as bytes arrive, so for a request with no body it holds almost
// nothing. Half the declared size is a generous ceiling: it still fails the current code, which
// reserves the full declared size up front, yet passes once the buffer grows only as bytes arrive.
#[tokio::test]
async fn test_prefix_only_request_does_not_reserve_declared_size() {
    let protocol = StreamProtocol::new("/rayls-test");
    let max_size = max_message_size();
    let codec = RLCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(max_size);
    // libp2p clones the codec for every inbound stream.
    let mut stream_codec = codec.clone();
    assert!(reserved_bytes(&stream_codec) < max_size / 2);

    // The peer declares a full-size message, sends no body, and closes the stream.
    let prefix = size_prefix(max_size);
    let res = stream_codec.read_request(&protocol, &mut prefix.as_ref()).await;
    assert!(res.is_err());

    // The codec must not reserve memory for bytes that never arrived.
    let reserved = reserved_bytes(&stream_codec);
    assert!(
        reserved < max_size / 2,
        "codec reserved {reserved} bytes for a request with no body"
    );
}

#[tokio::test]
async fn test_stalled_request_does_not_hold_declared_size() {
    let protocol = StreamProtocol::new("/rayls-test");
    let max_size = max_message_size();
    let codec = RLCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(max_size);
    // libp2p clones the codec for every inbound stream.
    let mut stream_codec = codec.clone();
    assert!(reserved_bytes(&stream_codec) < max_size / 2);

    // The peer declares a full-size message and then stops sending.
    let mut stream = StalledStream { prefix: size_prefix(max_size), sent: 0 };
    let res = tokio::time::timeout(
        STALLED_READ_WAIT,
        stream_codec.read_request(&protocol, &mut stream),
    )
    .await;
    assert!(res.is_err(), "read should still be waiting for the body");
    assert_eq!(stream.sent, 4, "the prefix should have been read");

    // While the peer stalls, the codec must not hold memory for the declared size.
    let reserved = reserved_bytes(&stream_codec);
    assert!(
        reserved < max_size / 2,
        "codec held {reserved} bytes while waiting for a body that never came"
    );
}
