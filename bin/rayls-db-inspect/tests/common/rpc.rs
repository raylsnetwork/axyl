// SPDX-License-Identifier: BUSL-1.1
//! An in-process `rayls_*` JSON-RPC server serving fixture data, for `--rpc` tests.

use jsonrpsee::{
    server::{Server, ServerHandle},
    types::ErrorObjectOwned,
    RpcModule,
};
use rayls_infrastructure_types::{
    BlockHash, ConsensusHeader, Epoch, EpochCertificate, EpochRecord,
};

/// What the mock node holds.
#[derive(Clone)]
struct State {
    headers: Vec<ConsensusHeader>,
    epochs: Vec<(EpochRecord, Option<EpochCertificate>)>,
    /// What `rayls_latestHeader` answers; `None` serves the newest of `headers`. Validators keep
    /// this watch stale in practice, so tests can serve a lagging or default header.
    latest: Option<ConsensusHeader>,
}

/// A running mock node; stops when dropped.
pub struct MockRpc {
    pub url: String,
    handle: ServerHandle,
    thread: Option<std::thread::JoinHandle<()>>,
}

fn not_found() -> ErrorObjectOwned {
    ErrorObjectOwned::owned(401, "Not Found.", None::<()>)
}

impl MockRpc {
    /// Serves `headers` and `epochs` like a node does: `rayls_latestHeader`, `rayls_epochRecord`
    /// and `rayls_epochRecordByHash` (certified records only), plus `rayls_consensusHeaderByNumber`
    /// and `rayls_consensusHeaderByHash` unless `headers_by_number` is false (an older node).
    pub fn start(
        headers: Vec<ConsensusHeader>,
        epochs: Vec<(EpochRecord, Option<EpochCertificate>)>,
        headers_by_number: bool,
    ) -> Self {
        Self::start_with_latest(headers, epochs, headers_by_number, None)
    }

    /// Like `start`, but `rayls_latestHeader` answers `latest` instead of the newest header.
    pub fn start_with_latest(
        headers: Vec<ConsensusHeader>,
        epochs: Vec<(EpochRecord, Option<EpochCertificate>)>,
        headers_by_number: bool,
        latest: Option<ConsensusHeader>,
    ) -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime =
                tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async move {
                let server = Server::builder().build("127.0.0.1:0").await.unwrap();
                let addr = server.local_addr().unwrap();
                let mut module = RpcModule::new(State { headers, epochs, latest });
                module
                    .register_method("rayls_latestHeader", |_, st, _| {
                        st.latest
                            .clone()
                            .or_else(|| st.headers.last().cloned())
                            .ok_or_else(not_found)
                    })
                    .unwrap();
                module
                    .register_method("rayls_epochRecord", |params, st, _| {
                        let epoch: Epoch = params.one()?;
                        st.epochs
                            .iter()
                            .find_map(|(r, c)| {
                                (r.epoch == epoch).then(|| c.clone().map(|c| (r.clone(), c)))
                            })
                            .flatten()
                            .ok_or_else(not_found)
                    })
                    .unwrap();
                module
                    .register_method("rayls_epochRecordByHash", |params, st, _| {
                        let hash: BlockHash = params.one()?;
                        st.epochs
                            .iter()
                            .find_map(|(r, c)| {
                                (r.digest() == hash).then(|| c.clone().map(|c| (r.clone(), c)))
                            })
                            .flatten()
                            .ok_or_else(not_found)
                    })
                    .unwrap();
                if headers_by_number {
                    module
                        .register_method("rayls_consensusHeaderByNumber", |params, st, _| {
                            let number: u64 = params.one()?;
                            st.headers
                                .iter()
                                .find(|h| h.number == number)
                                .cloned()
                                .ok_or_else(not_found)
                        })
                        .unwrap();
                    module
                        .register_method("rayls_consensusHeaderByHash", |params, st, _| {
                            let hash: BlockHash = params.one()?;
                            st.headers
                                .iter()
                                .find(|h| h.digest() == hash)
                                .cloned()
                                .ok_or_else(not_found)
                        })
                        .unwrap();
                }
                let handle = server.start(module);
                tx.send((addr, handle.clone())).unwrap();
                handle.stopped().await;
            });
        });
        let (addr, handle) = rx.recv().expect("mock rpc server starts");
        Self { url: format!("http://{addr}"), handle, thread: Some(thread) }
    }
}

impl Drop for MockRpc {
    fn drop(&mut self) {
        let _ = self.handle.stop();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
