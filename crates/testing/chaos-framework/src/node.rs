//! Process-level node management for validator and observer nodes.

use escargot::CargoRun;
use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use std::{
    path::{Path, PathBuf},
    process::Child,
    time::Duration,
};
use tracing::{error, info};

/// A handle to a running validator or observer process.
pub struct NodeHandle {
    /// Validator index (0-based). Validator i uses datadir `validator-{i+1}`.
    pub index: usize,
    /// The child process (None if the node is currently stopped).
    child: Option<Child>,
    /// Base directory for all validators.
    base_dir: PathBuf,
    /// RPC port assigned to this node.
    rpc_port: u16,
    /// Whether this is an observer node.
    is_observer: bool,
    /// The RPC URL for this node.
    rpc_url: String,
}

impl NodeHandle {
    /// Create a new handle and spawn the validator process.
    pub fn spawn_validator(
        index: usize,
        bin: &'static CargoRun,
        base_dir: &Path,
        rpc_port: u16,
        passphrase: &str,
    ) -> Self {
        // Each node gets its own reserved `--http.port` (from cluster.rs) and runs
        // with `--ipcdisable`; `--instance` is intentionally NOT used (see
        // spawn_validator_process for why).
        let rpc_url = format!("http://127.0.0.1:{rpc_port}");
        let child = spawn_validator_process(index, bin, base_dir, rpc_port, passphrase);
        Self {
            index,
            child: Some(child),
            base_dir: base_dir.to_path_buf(),
            rpc_port,
            is_observer: false,
            rpc_url,
        }
    }

    /// Create a new handle and spawn an observer process.
    pub fn spawn_observer(
        index: usize,
        bin: &'static CargoRun,
        base_dir: &Path,
        rpc_port: u16,
        passphrase: &str,
    ) -> Self {
        let rpc_url = format!("http://127.0.0.1:{rpc_port}");
        let child = spawn_observer_process(index, bin, base_dir, rpc_port, passphrase);
        Self {
            index,
            child: Some(child),
            base_dir: base_dir.to_path_buf(),
            rpc_port,
            is_observer: true,
            rpc_url,
        }
    }

    /// Get the RPC URL for this node.
    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
    }

    /// Check if the node process is currently running.
    pub fn is_alive(&mut self) -> bool {
        match &mut self.child {
            Some(child) => matches!(child.try_wait(), Ok(None)),
            None => false,
        }
    }

    /// Send SIGTERM to the node process (graceful shutdown).
    pub fn graceful_stop(&mut self) {
        if let Some(child) = &mut self.child {
            let pid = i32::try_from(child.id()).expect("child PID fits in i32");
            if let Err(e) = signal::kill(Pid::from_raw(pid), Signal::SIGTERM) {
                error!(target: "chaos", index = self.index, ?e, "error sending SIGTERM");
            }
        }
    }

    /// Kill the node process, first trying SIGTERM then escalating to SIGKILL.
    pub fn kill(&mut self) {
        if let Some(child) = &mut self.child {
            kill_child(child, self.index);
            self.child = None;
        }
    }

    /// Hard kill the node process with SIGKILL (no graceful shutdown).
    pub fn hard_kill(&mut self) {
        if let Some(child) = &mut self.child {
            if let Err(e) = child.kill() {
                error!(target: "chaos", index = self.index, ?e, "error sending SIGKILL");
            }
            let _ = child.wait();
            self.child = None;
        }
    }

    /// Restart the node process using the stored binary reference.
    ///
    /// The caller must provide the binary reference since we cannot store
    /// a `&'static CargoRun` across restarts without it.
    pub fn restart(&mut self, bin: &'static CargoRun, passphrase: &str) {
        // Kill first if still running.
        self.kill();

        let child = if self.is_observer {
            spawn_observer_process(self.index, bin, &self.base_dir, self.rpc_port, passphrase)
        } else {
            spawn_validator_process(self.index, bin, &self.base_dir, self.rpc_port, passphrase)
        };
        self.child = Some(child);
        info!(target: "chaos", index = self.index, "node restarted");
    }
}

impl Drop for NodeHandle {
    fn drop(&mut self) {
        self.kill();
    }
}

impl std::fmt::Debug for NodeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeHandle")
            .field("index", &self.index)
            .field("rpc_url", &self.rpc_url)
            .field("alive", &self.child.is_some())
            .field("is_observer", &self.is_observer)
            .finish()
    }
}

/// Send SIGTERM then wait, escalating to SIGKILL if needed.
fn kill_child(child: &mut Child, index: usize) {
    let pid = i32::try_from(child.id()).expect("child PID fits in i32");
    if let Err(e) = signal::kill(Pid::from_raw(pid), Signal::SIGTERM) {
        error!(target: "chaos", index, ?e, "error sending SIGTERM to child");
    }

    for _ in 0..6 {
        match child.try_wait() {
            Ok(Some(_)) => {
                info!(target: "chaos", index, "child exited after SIGTERM");
                return;
            }
            Ok(None) => {}
            Err(e) => error!(target: "chaos", index, ?e, "error waiting on child"),
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    // Escalate to SIGKILL.
    if let Err(e) = child.kill() {
        error!(target: "chaos", index, ?e, "error sending SIGKILL");
    }
    if let Err(e) = child.wait() {
        error!(target: "chaos", index, ?e, "error waiting for child after SIGKILL");
    }
}

/// Spawn a validator process.
fn spawn_validator_process(
    instance: usize,
    bin: &'static CargoRun,
    base_dir: &Path,
    rpc_port: u16,
    passphrase: &str,
) -> Child {
    let data_dir = base_dir.join(format!("validator-{}", instance + 1));
    let mut command = bin.command();

    // Do NOT pass `--instance`: reth applies its per-instance offset on top of the
    // explicit `--http.port`, collapsing the distinct ports the caller reserved
    // onto one — and `--instance` also shifts the IPC path. Instead, give each node
    // its reserved `--http.port` and `--ipcdisable` so the default IPC socket path
    // can't collide across the cluster.
    command
        .env("RL_BLS_PASSPHRASE", passphrase)
        .arg("node")
        .arg("--datadir")
        .arg(&*data_dir.to_string_lossy())
        .arg("--ipcdisable")
        .arg("--http")
        .arg("--http.port")
        .arg(format!("{rpc_port}"))
        // v1 (plain) storage is no longer supported -- the node refuses to start without this
        .arg("--storage.v2");

    command.spawn().expect("failed to spawn validator process")
}

/// Spawn an observer process.
///
/// `_instance` is unused (the observer always uses the single `observer` datadir);
/// it is kept for signature symmetry with `spawn_validator_process`.
fn spawn_observer_process(
    _instance: usize,
    bin: &'static CargoRun,
    base_dir: &Path,
    rpc_port: u16,
    passphrase: &str,
) -> Child {
    let data_dir = base_dir.join("observer");
    let mut command = bin.command();

    // See spawn_validator_process: no `--instance`, explicit `--http.port`,
    // and `--ipcdisable` to avoid IPC-path collisions across the cluster.
    command
        .env("RL_BLS_PASSPHRASE", passphrase)
        .arg("node")
        .arg("--observer")
        .arg("--datadir")
        .arg(&*data_dir.to_string_lossy())
        .arg("--ipcdisable")
        .arg("--http")
        .arg("--http.port")
        .arg(format!("{rpc_port}"))
        // v1 (plain) storage is no longer supported -- the node refuses to start without this
        .arg("--storage.v2");

    command.spawn().expect("failed to spawn observer process")
}
