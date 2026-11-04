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

    let mut encoded = Vec::new();
    let request = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default()],
    };
    codec
        .write_request(&protocol, &mut encoded, request.clone())
        .await
        .expect("write valid request");

    let decoded =
        codec.read_request(&protocol, &mut encoded.as_ref()).await.expect("read valid request");
    assert_eq!(decoded, request);

    let mut encoded = Vec::new();
    let response = TestPrimaryResponse::MissingParents(vec![CertificateDigest::new([b'a'; 32])]);
    codec
        .write_response(&protocol, &mut encoded, response.clone())
        .await
        .expect("write valid response");

    let decoded =
        codec.read_response(&protocol, &mut encoded.as_ref()).await.expect("read valid response");
    assert_eq!(decoded, response);
}

#[tokio::test]
async fn test_fail_to_write_message_too_big() {
    let max_chunk_size = 100; // 100 bytes is too small
    let mut codec = RLCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(max_chunk_size);
    let protocol = StreamProtocol::new("/rayls-test");

    let mut encoded = Vec::new();
    let request = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default()],
    };
    let res = codec.write_request(&protocol, &mut encoded, request).await;
    assert!(res.is_err());

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

/// A request that sends only the 4-byte length prefix and then closes the stream must be
/// rejected as a protocol violation.
#[tokio::test]
async fn test_prefix_only_request_is_rejected() {
    let max_chunk_size = 2 * 1024 * 1024; // 2 MiB
    let mut codec = RLCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(max_chunk_size);
    let protocol = StreamProtocol::new("/rayls-test");

    // A 4-byte prefix declaring the maximum size, with no body following.
    let mut prefix_only = vec![0u8; 4];
    prefix_only[..].copy_from_slice(&(max_chunk_size as u32).to_le_bytes());

    let error = codec
        .read_request(&protocol, &mut prefix_only.as_slice())
        .await
        .expect_err("prefix-only request should be rejected");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
}
