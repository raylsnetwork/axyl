//! Shared setup for tests that kill a child process and reopen its database.
//!
//! The parent test runs a child test from the same binary in a new process.
//! The child holds the database write lock, does its work and reports one line on stdout.
//! The parent then kills it with SIGKILL and reopens the same directory.

use rand::{rngs::StdRng, SeedableRng as _};
use rayls_infrastructure_storage::{mem_db::MemDatabase, DatabaseType};
use rayls_infrastructure_types::Database as _;
use rayls_testing_test_utils_committee::CommitteeFixture;
use std::{
    io::{BufRead as _, BufReader, Read as _, Write as _},
    os::unix::process::ExitStatusExt as _,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{mpsc, Arc, Mutex},
    time::{Duration, Instant},
};

/// Seed for the committee keys, so the child and the parent build the same committee.
const COMMITTEE_SEED: u64 = 7;
/// Signal number of SIGKILL on Unix, as sent by the out of memory killer or `kill -9`.
const SIGKILL: i32 = 9;
/// How long the child may take to start and open its database.
pub(crate) const CHILD_START_TIMEOUT: Duration = Duration::from_secs(60);
/// How long the child holds the write lock before it releases it.
/// Code that sends before saving sends within this time.
/// Code that waits for the disk sends only after the release.
pub(crate) const GATE_HOLD: Duration = Duration::from_secs(2);

/// Name libtest gives a test function in this crate.
/// Naming a function that does not exist fails to compile.
macro_rules! test_name {
    ($test:ident) => {{
        let _: fn() = $test;
        concat!(module_path!(), "::", stringify!($test))
            .split_once("::")
            .expect("module path starts with the crate name")
            .1
    }};
}
pub(crate) use test_name;

/// The seeded committee every process builds.
pub(crate) fn committee() -> CommitteeFixture<MemDatabase> {
    CommitteeFixture::builder(MemDatabase::default)
        .with_rng(StdRng::seed_from_u64(COMMITTEE_SEED))
        .build()
}

/// Returns the database directory when this process is a child started by the parent.
/// A plain `cargo test` run does not set `env`, so the child test returns at once.
pub(crate) fn child_dir(env: &str) -> Option<PathBuf> {
    std::env::var_os(env).map(PathBuf::from)
}

/// Prints one line for the parent and flushes it.
pub(crate) fn report(line: &str) {
    let mut out = std::io::stdout().lock();
    writeln!(out, "{line}").expect("stdout");
    out.flush().expect("flush");
}

/// Keeps the child alive until the parent kills it.
pub(crate) fn wait_for_kill() -> ! {
    loop {
        std::thread::park();
    }
}

/// Holds the only MDBX write transaction, so no queued write can reach disk.
/// This stands in for a background writer that is behind, as seen under load.
pub(crate) struct WriteGate {
    release: mpsc::Sender<()>,
    released: mpsc::Receiver<()>,
}

impl WriteGate {
    /// Takes the write lock and returns once it is held.
    pub(crate) fn close(db: &DatabaseType) -> Self {
        let mdbx = db.inner().clone();
        let (locked_tx, locked_rx) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let (released_tx, released) = mpsc::channel();
        std::thread::spawn(move || {
            let txn = mdbx.write_txn().expect("write gate transaction");
            locked_tx.send(()).expect("signal gate closed");
            let _ = release_rx.recv();
            drop(txn);
            let _ = released_tx.send(());
        });
        locked_rx.recv().expect("write gate closed");
        Self { release, released }
    }

    /// Releases the write lock and returns once queued writes can reach disk again.
    pub(crate) fn open(self) {
        self.release.send(()).expect("release write gate");
        self.released.recv().expect("write gate opened");
    }
}

/// A child test running in its own process.
pub(crate) struct Child {
    process: std::process::Child,
    lines: mpsc::Receiver<String>,
    stderr: Arc<Mutex<String>>,
}

impl Child {
    /// Runs the test `name` alone in a new process, with `env` set to `dir`.
    pub(crate) fn spawn(name: &str, env: &str, dir: &Path) -> Self {
        let mut process = Command::new(std::env::current_exe().expect("test binary"))
            .args([name, "--exact", "--nocapture", "--test-threads=1"])
            .env(env, dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn child");

        // Read both outputs on threads so every wait has a time limit.
        let stdout = process.stdout.take().expect("child stdout");
        let (lines_tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if lines_tx.send(line).is_err() {
                    break;
                }
            }
        });
        let mut pipe = process.stderr.take().expect("child stderr");
        let stderr = Arc::new(Mutex::new(String::new()));
        let sink = stderr.clone();
        std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            while let Ok(n @ 1..) = pipe.read(&mut buf) {
                sink.lock().expect("stderr lock").push_str(&String::from_utf8_lossy(&buf[..n]));
            }
        });

        Self { process, lines, stderr }
    }

    /// Waits up to `timeout` for a child line that contains one of `tags`.
    /// Returns the line from that tag on, or None if the time ran out or the child exited.
    pub(crate) fn wait_for(&self, tags: &[&str], timeout: Duration) -> Option<String> {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.checked_duration_since(Instant::now())?;
            let line = self.lines.recv_timeout(left).ok()?;
            // libtest prints the test name on the same line, so search inside it.
            if let Some(at) = tags.iter().find_map(|tag| line.find(tag)) {
                return Some(line[at..].trim().to_owned());
            }
        }
    }

    /// What the child wrote to stderr so far, such as a panic message.
    pub(crate) fn stderr(&self) -> String {
        self.stderr.lock().expect("stderr lock").clone()
    }

    /// Kills the child with SIGKILL and checks that the signal is what ended it.
    pub(crate) fn kill(mut self) {
        self.process.kill().expect("kill child");
        let status = self.process.wait().expect("wait child");
        assert_eq!(
            status.signal(),
            Some(SIGKILL),
            "child must die from SIGKILL, it exited with {status}: {}",
            self.stderr()
        );
    }
}

impl Drop for Child {
    // Never leave a child running when the parent fails early.
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// Parses a `key=value` field from a child line.
pub(crate) fn field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    line.split_whitespace().find_map(|f| f.strip_prefix(key)?.strip_prefix('='))
}
