use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::io::Write;
use std::process::{ExitCode, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{RecvTimeoutError, Sender, SyncSender, TrySendError, channel, sync_channel};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use rand::Rng;
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::runtime::Builder;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{oneshot, watch};
use tokio::task::{JoinHandle, JoinSet, spawn_blocking};
use tokio::time::{Instant, sleep, sleep_until, timeout};

mod process_tree;

use process_tree::{ProcessIdentity, ProcessSnapshot};

const MAX_OUTPUT_FRAME_BYTES: usize = 64 * 1024;
const MAX_OUTPUT_PREFIX_BYTES: usize = MAX_OUTPUT_FRAME_BYTES / 2;
const OUTPUT_QUEUE_BYTES_PER_STREAM: usize = 4 * 1024 * 1024;
const OUTPUT_CHANNEL_CAPACITY: usize = OUTPUT_QUEUE_BYTES_PER_STREAM / MAX_OUTPUT_FRAME_BYTES;
const MIN_RESTART_DELAY: Duration = Duration::from_millis(100);
const OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);
const OUTPUT_FLUSH_TIMEOUT: Duration = Duration::from_millis(500);
const PROCESS_SCAN_INTERVAL: Duration = Duration::from_millis(50);
const POST_KILL_WAIT: Duration = Duration::from_secs(2);
const SIGNAL_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const SIGNAL_RETRY_LIMIT: usize = 3;
const SUDO_SIGNAL_TIMEOUT: Duration = Duration::from_secs(2);

static OUTPUT_SINKS: OnceLock<OutputSinks> = OnceLock::new();

macro_rules! eprintln {
    ($($arg:tt)*) => {{
        try_log_stderr(format_args!($($arg)*));
    }};
}

#[derive(Clone, Copy)]
enum OutputStream {
    Stdout,
    Stderr,
}

struct OutputSinks {
    stdout: SyncSender<OutputMessage>,
    stderr: SyncSender<OutputMessage>,
}

enum OutputMessage {
    Frame(Vec<u8>),
    Flush(SyncSender<()>),
}

enum ProcessScannerRequest {
    Refresh(oneshot::Sender<std::io::Result<Arc<ProcessSnapshot>>>),
    Stop,
}

struct BoundedOutput {
    bytes: Vec<u8>,
    limit: usize,
}

impl BoundedOutput {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(256)),
            limit,
        }
    }
}

impl std::fmt::Write for BoundedOutput {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        let remaining = self.limit.saturating_sub(self.bytes.len());
        let mut end = value.len().min(remaining);
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        self.bytes.extend_from_slice(&value.as_bytes()[..end]);
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct JobConfig {
    command: String,
    #[serde(default)]
    disabled: bool,
    #[serde(default)]
    color: String,
    #[serde(default)]
    cwd: String,
    #[serde(default)]
    delay: u64,
    #[serde(default)]
    env: Option<HashMap<String, String>>,
    #[serde(default, alias = "sudoUser")]
    sudo_user: Option<String>,
    #[serde(default = "default_restart_delay", alias = "restartDelay")]
    restart_delay: u64,
    #[serde(default)]
    datetime: DateTimeConfig,
    #[serde(default = "default_process_group", alias = "processGroup")]
    process_group: bool,
    #[serde(default = "default_shutdown_timeout", alias = "shutdownTimeout")]
    shutdown_timeout: u64,
}

fn default_shutdown_timeout() -> u64 {
    5
}

fn default_process_group() -> bool {
    true
}

#[derive(Debug, Deserialize, Clone, Default)]
struct DateTimeConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_datetime_format")]
    format: String,
    #[serde(default)]
    local: bool,
}

fn default_restart_delay() -> u64 {
    15_000
}

fn lock_or_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| {
        eprintln!("Supervisor state mutex was poisoned");
        PoisonError::into_inner(error)
    })
}

fn default_datetime_format() -> String {
    String::from("%Y-%m-%d %H:%M:%S %Z")
}

fn is_valid_username(user: &str) -> bool {
    !user.is_empty()
        && user
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn is_valid_env_key(key: &str) -> bool {
    !key.is_empty() && !key.contains(['=', '\0'])
}

#[derive(Debug, Deserialize)]
struct Config {
    jobs: BTreeMap<String, JobConfig>,
}

#[derive(Clone, Copy, Debug)]
struct PidInfo {
    is_process_group: bool,
    identity: ProcessIdentity,
}

#[derive(Clone, Debug)]
struct TrackedRun {
    info: PidInfo,
    descendants: HashSet<ProcessIdentity>,
}

#[derive(Default)]
struct PendingWrapperSignals {
    termination: [Option<libc::c_int>; 2],
    usr1: bool,
    usr2: bool,
}

impl PendingWrapperSignals {
    fn record(&mut self, signal: libc::c_int) {
        match signal {
            libc::SIGUSR1 => self.usr1 = true,
            libc::SIGUSR2 => self.usr2 = true,
            _ if self.termination[0].is_none() => self.termination[0] = Some(signal),
            _ if self.termination[1].is_none() => self.termination[1] = Some(signal),
            _ => {}
        }
    }

    fn into_signals(self) -> Vec<libc::c_int> {
        self.termination
            .into_iter()
            .flatten()
            .chain(self.usr1.then_some(libc::SIGUSR1))
            .chain(self.usr2.then_some(libc::SIGUSR2))
            .collect()
    }
}

#[derive(Default)]
enum WrapperSignalPhase {
    #[default]
    Idle,
    Publishing(PendingWrapperSignals),
    Running(ProcessIdentity),
}

#[derive(Default)]
struct WrapperSignalState {
    phase: WrapperSignalPhase,
}

impl WrapperSignalState {
    fn begin_publication(&mut self) {
        self.phase = WrapperSignalPhase::Publishing(PendingWrapperSignals::default());
    }

    fn receive(&mut self, signal: libc::c_int) -> Option<(ProcessIdentity, libc::c_int)> {
        match &mut self.phase {
            WrapperSignalPhase::Idle => None,
            WrapperSignalPhase::Publishing(pending) => {
                pending.record(signal);
                None
            }
            WrapperSignalPhase::Running(inner) => Some((*inner, signal)),
        }
    }

    fn publish(&mut self, inner: ProcessIdentity) -> Vec<libc::c_int> {
        match std::mem::replace(&mut self.phase, WrapperSignalPhase::Running(inner)) {
            WrapperSignalPhase::Publishing(pending) => pending.into_signals(),
            WrapperSignalPhase::Idle | WrapperSignalPhase::Running(_) => Vec::new(),
        }
    }

    fn cancel_publication(&mut self) {
        if matches!(self.phase, WrapperSignalPhase::Publishing(_)) {
            self.phase = WrapperSignalPhase::Idle;
        }
    }

    fn clear(&mut self, inner: ProcessIdentity) {
        if matches!(self.phase, WrapperSignalPhase::Running(current) if current == inner) {
            self.phase = WrapperSignalPhase::Idle;
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShutdownState {
    Running,
    Graceful,
    Force,
}

enum ProcessRunError {
    Retry(std::io::Error),
    Fatal(std::io::Error),
}

impl From<std::io::Error> for ProcessRunError {
    fn from(error: std::io::Error) -> Self {
        Self::Retry(error)
    }
}

#[derive(Clone)]
struct PrefixWriter<'a> {
    prefix: String,
    datetime_items: Vec<chrono::format::Item<'a>>,
    datetime_local: bool,
}

impl<'a> PrefixWriter<'a> {
    pub fn from(prefix: String, config: &'a DateTimeConfig) -> Self {
        let datetime_items = if config.enabled {
            chrono::format::StrftimeItems::new(&config.format).collect()
        } else {
            vec![]
        };
        Self {
            prefix,
            datetime_items,
            datetime_local: config.local,
        }
    }
}

impl<'a> std::fmt::Display for PrefixWriter<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.datetime_items.is_empty() {
            return write!(f, "{}", self.prefix);
        }

        let timestamp = if self.datetime_local {
            chrono::offset::Local::now().format_with_items(self.datetime_items.iter())
        } else {
            chrono::offset::Utc::now().format_with_items(self.datetime_items.iter())
        };
        write!(f, "{} > {}", self.prefix, timestamp)
    }
}

async fn start_proc(
    prefix: String,
    opts: JobConfig,
    active_pids: Arc<Mutex<HashMap<u32, TrackedRun>>>,
    mut shutdown: watch::Receiver<ShutdownState>,
    mut snapshots: watch::Receiver<Arc<ProcessSnapshot>>,
    base_env: Arc<HashMap<String, String>>,
) -> std::io::Result<()> {
    let status_prefix = PrefixWriter::from(format!("{} (s)", prefix), &opts.datetime);
    let mut initial = true;

    loop {
        if *shutdown.borrow() != ShutdownState::Running {
            break;
        }

        let is_initial = initial;
        let configured_delay = Duration::from_millis(if is_initial {
            initial = false;
            opts.delay
        } else {
            opts.restart_delay
        });
        let delay = if is_initial {
            configured_delay
        } else {
            configured_delay.max(MIN_RESTART_DELAY)
        };

        if !delay.is_zero() {
            eprintln!(
                "{} > Starting in {:.3}s",
                status_prefix,
                delay.as_secs_f64()
            );
            tokio::select! {
                _ = sleep(delay) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() != ShutdownState::Running {
                        break;
                    }
                }
            }
        }

        match start_proc_internal(
            &status_prefix,
            &prefix,
            &opts,
            &active_pids,
            &mut shutdown,
            &mut snapshots,
            &base_env,
        )
        .await
        {
            Ok(true) => break,
            Ok(false) => {}
            Err(ProcessRunError::Retry(e)) => {
                eprintln!("{} > Process error: {}", status_prefix, e);
            }
            Err(ProcessRunError::Fatal(e)) => {
                eprintln!("{} > Fatal process supervision error: {}", status_prefix, e);
                return Err(e);
            }
        }
    }
    eprintln!("{} > Shutting down..", status_prefix);
    Ok(())
}

async fn start_proc_internal(
    status_prefix: &PrefixWriter<'_>,
    prefix: &str,
    opts: &JobConfig,
    active_pids: &Arc<Mutex<HashMap<u32, TrackedRun>>>,
    shutdown: &mut watch::Receiver<ShutdownState>,
    snapshots: &mut watch::Receiver<Arc<ProcessSnapshot>>,
    base_env: &Arc<HashMap<String, String>>,
) -> Result<bool, ProcessRunError> {
    if *shutdown.borrow() != ShutdownState::Running {
        return Ok(true);
    }

    let cwd_path = std::path::Path::new(&opts.cwd);
    if !cwd_path.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Working directory does not exist: {}", opts.cwd),
        )
        .into());
    }

    if let Some(username) = &opts.sudo_user
        && !is_valid_username(username)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "Invalid sudo_user '{}': must contain only alphanumeric characters, hyphens, or underscores",
                username
            ),
        )
        .into());
    }
    if let Some(additional_env) = &opts.env
        && let Some(invalid_key) = additional_env.keys().find(|key| !is_valid_env_key(key))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid environment variable name: {invalid_key:?}"),
        )
        .into());
    }

    let use_process_group = opts.process_group || opts.sudo_user.is_some();
    let mut cmd = if let Some(username) = &opts.sudo_user {
        let mut cmd = Command::new("sudo");
        cmd.args(["-n", "-u", username, "--"]);
        cmd.args(["env", "--"]);
        for key in ["CWD", "RUNNER_CWD"] {
            if let Some(value) = base_env.get(key) {
                cmd.arg(format!("{key}={value}"));
            }
        }
        if let Some(additional_env) = &opts.env {
            for (key, value) in additional_env {
                cmd.arg(format!("{key}={value}"));
            }
        }
        cmd.args(["sh", "-c", &opts.command]);
        cmd
    } else {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", &opts.command]);
        cmd
    };

    cmd.current_dir(opts.cwd.clone())
        .envs(base_env.iter())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if opts.sudo_user.is_none()
        && let Some(additional_env) = &opts.env
    {
        cmd.envs(additional_env);
    }

    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }

    let mut child = cmd.spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| std::io::Error::other("Could not get child pid"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("Could not get stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| std::io::Error::other("Could not get stderr"))?;
    let stdout_handle = spawn_output_handler(
        stdout,
        format!("{} (1)", prefix),
        opts.datetime.clone(),
        OutputStream::Stdout,
    );
    let stderr_handle = spawn_output_handler(
        stderr,
        format!("{} (2)", prefix),
        opts.datetime.clone(),
        OutputStream::Stderr,
    );

    let identity = match process_tree::identity(pid) {
        Ok(identity) => identity,
        Err(identity_error) => {
            match child.try_wait() {
                Ok(Some(status)) => {
                    report_child_status(status_prefix, status);
                    drain_output_handlers([stdout_handle, stderr_handle]).await;
                    return Ok(false);
                }
                Ok(None) => {}
                Err(error) => {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    drain_output_handlers([stdout_handle, stderr_handle]).await;
                    return Err(ProcessRunError::Retry(error));
                }
            }

            let _ = child.start_kill();
            let status = child.wait().await;
            drain_output_handlers([stdout_handle, stderr_handle]).await;
            let status = status?;
            report_child_status(status_prefix, status);
            return Err(std::io::Error::new(
                identity_error.kind(),
                format!("Could not inspect child process {pid}: {identity_error}"),
            )
            .into());
        }
    };
    let info = PidInfo {
        is_process_group: use_process_group,
        identity,
    };
    let mut known = if use_process_group {
        related_processes(snapshots, identity, &HashSet::new())
    } else {
        HashSet::new()
    };

    let shutdown_at_registration = {
        let mut pids = lock_or_recover(active_pids);
        let state = *shutdown.borrow();
        if state == ShutdownState::Running {
            pids.insert(
                pid,
                TrackedRun {
                    info,
                    descendants: known.clone(),
                },
            );
        }
        state
    };

    eprintln!("{} > Started (pid {})", status_prefix, pid);

    let shutdown_state = if shutdown_at_registration != ShutdownState::Running {
        Some(shutdown_at_registration)
    } else {
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() {
                        break Some(ShutdownState::Force);
                    }
                    let state = *shutdown.borrow();
                    if state != ShutdownState::Running {
                        break Some(state);
                    }
                }
                snapshot_changed = snapshots.changed() => {
                    if snapshot_changed.is_err() {
                        break Some(ShutdownState::Force);
                    }
                    if use_process_group {
                        known = related_processes(snapshots, identity, &known);
                        update_active_descendants(active_pids, info, &known);
                    }
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            let exit_observed_at = std::time::Instant::now();
                            remove_active_pid(active_pids, info);
                            report_child_status(status_prefix, status);
                            if use_process_group {
                                wait_for_snapshot_after(snapshots, exit_observed_at).await;
                                known = related_processes(snapshots, identity, &known);
                            }
                            if let Err(error) =
                                cleanup_after_natural_exit(status_prefix, opts, info, &known).await
                            {
                                drain_output_handlers([stdout_handle, stderr_handle]).await;
                                return Err(ProcessRunError::Fatal(error));
                            }
                            drain_output_handlers([stdout_handle, stderr_handle]).await;
                            return Ok(false);
                        }
                        Ok(None) => {}
                        Err(error) => {
                            remove_active_pid(active_pids, info);
                            let _ = child.start_kill();
                            let _ = child.wait().await;
                            drain_output_handlers([stdout_handle, stderr_handle]).await;
                            return Err(ProcessRunError::Retry(error));
                        }
                    }
                }
            }
        }
    };

    if use_process_group {
        known = related_processes(snapshots, identity, &known);
        update_active_descendants(active_pids, info, &known);
    }
    let mut child_wait = Box::pin(child.wait());
    let mut child_status = None;
    let mut child_exit_observed_at = None;
    let mut child_wait_error = None;
    let mut kill_sent_at = None;
    let mut signal_delivery_error = None;
    let mut snapshots_closed = false;
    let mut post_kill_timed_out = false;
    let deadline = Instant::now() + Duration::from_secs(opts.shutdown_timeout);
    let signal_log_prefix = status_prefix.to_string();
    let mut deadline_sleep = Box::pin(sleep_until(deadline));
    let mut post_kill_sleep = Box::pin(sleep_until(deadline));

    if shutdown_state == Some(ShutdownState::Graceful) {
        if let Err(error) =
            signal_run_with_retry(&signal_log_prefix, opts, info, &known, libc::SIGTERM).await
        {
            eprintln!("{} > Could not deliver SIGTERM: {}", status_prefix, error);
            signal_delivery_error = Some(error);
        }
    } else {
        if let Err(error) =
            signal_run_with_retry(&signal_log_prefix, opts, info, &known, libc::SIGKILL).await
        {
            eprintln!("{} > Could not deliver SIGKILL: {}", status_prefix, error);
            signal_delivery_error = Some(error);
        }
        let sent_at = Instant::now();
        kill_sent_at = Some(sent_at);
        post_kill_sleep.as_mut().reset(sent_at + POST_KILL_WAIT);
    }

    loop {
        if use_process_group {
            known = related_processes(snapshots, identity, &known);
            update_active_descendants(active_pids, info, &known);
        }
        let descendants_running = known.iter().copied().any(process_tree::is_running);
        let post_exit_snapshot_ready = snapshots_closed
            || child_exit_observed_at
                .is_none_or(|observed| snapshots.borrow().started_after(observed));
        if child_status.is_some()
            && post_exit_snapshot_ready
            && (!use_process_group || !descendants_running)
        {
            break;
        }
        if kill_sent_at.is_some_and(|sent| sent.elapsed() >= POST_KILL_WAIT) {
            eprintln!(
                "{} > Timed out waiting for killed process tree to exit",
                status_prefix
            );
            post_kill_timed_out = true;
            break;
        }

        tokio::select! {
            status = &mut child_wait, if child_status.is_none() => {
                match status {
                    Ok(status) => {
                        child_status = Some(status);
                        child_exit_observed_at = Some(std::time::Instant::now());
                        remove_active_pid(active_pids, info);
                    }
                    Err(error) => {
                        remove_active_pid(active_pids, info);
                        child_wait_error = Some(error);
                        break;
                    }
                }
            }
            snapshot_changed = snapshots.changed(), if !snapshots_closed => {
                if snapshot_changed.is_err() {
                    snapshots_closed = true;
                    if kill_sent_at.is_none() {
                        eprintln!(
                            "{} > Process scanner stopped during shutdown, sending SIGKILL..",
                            status_prefix
                        );
                        if let Err(error) =
                            signal_run_with_retry(&signal_log_prefix, opts, info, &known, libc::SIGKILL).await
                        {
                            eprintln!("{} > Could not deliver SIGKILL: {}", status_prefix, error);
                            signal_delivery_error = Some(error);
                        }
                        let sent_at = Instant::now();
                        kill_sent_at = Some(sent_at);
                        post_kill_sleep
                            .as_mut()
                            .reset(sent_at + POST_KILL_WAIT);
                    }
                }
            }
            _ = &mut post_kill_sleep, if kill_sent_at.is_some() => {
                eprintln!(
                    "{} > Timed out waiting for killed process tree to exit",
                    status_prefix
                );
                post_kill_timed_out = true;
                break;
            }
            _ = &mut deadline_sleep, if kill_sent_at.is_none() => {
                if use_process_group {
                    known = related_processes(snapshots, identity, &known);
                    update_active_descendants(active_pids, info, &known);
                }
                eprintln!(
                    "{} > Process tree exceeded {}s shutdown timeout, sending SIGKILL..",
                    status_prefix,
                    opts.shutdown_timeout
                );
                if let Err(error) =
                    signal_run_with_retry(&signal_log_prefix, opts, info, &known, libc::SIGKILL).await
                {
                    eprintln!("{} > Could not deliver SIGKILL: {}", status_prefix, error);
                    signal_delivery_error = Some(error);
                }
                let sent_at = Instant::now();
                kill_sent_at = Some(sent_at);
                post_kill_sleep
                    .as_mut()
                    .reset(sent_at + POST_KILL_WAIT);
            }
            changed = shutdown.changed(), if kill_sent_at.is_none() => {
                if changed.is_err() || *shutdown.borrow() == ShutdownState::Force {
                    if use_process_group {
                        known = related_processes(snapshots, identity, &known);
                        update_active_descendants(active_pids, info, &known);
                    }
                    eprintln!("{} > Shutdown escalated, sending SIGKILL..", status_prefix);
                    if let Err(error) =
                        signal_run_with_retry(&signal_log_prefix, opts, info, &known, libc::SIGKILL).await
                    {
                        eprintln!("{} > Could not deliver SIGKILL: {}", status_prefix, error);
                        signal_delivery_error = Some(error);
                    }
                    let sent_at = Instant::now();
                    kill_sent_at = Some(sent_at);
                    post_kill_sleep
                        .as_mut()
                        .reset(sent_at + POST_KILL_WAIT);
                }
            }
        }
    }

    drop(child_wait);
    if let Some(error) = child_wait_error {
        let _ = child.start_kill();
        let _ = timeout(POST_KILL_WAIT, child.wait()).await;
        remove_active_pid(active_pids, info);
        drain_output_handlers([stdout_handle, stderr_handle]).await;
        return Err(ProcessRunError::Fatal(error));
    }
    if child_status.is_none() {
        if post_kill_timed_out {
            match child.try_wait() {
                Ok(status) => child_status = status,
                Err(error) => {
                    remove_active_pid(active_pids, info);
                    drain_output_handlers([stdout_handle, stderr_handle]).await;
                    return Err(ProcessRunError::Fatal(error));
                }
            }
        } else {
            match child.wait().await {
                Ok(status) => child_status = Some(status),
                Err(error) => {
                    remove_active_pid(active_pids, info);
                    drain_output_handlers([stdout_handle, stderr_handle]).await;
                    return Err(ProcessRunError::Fatal(error));
                }
            }
        }
    }
    remove_active_pid(active_pids, info);
    if let Some(status) = child_status {
        report_child_status(status_prefix, status);
    }
    let timeout_error = post_kill_timed_out
        .then(|| post_kill_timeout_error(child_status.is_none(), &known))
        .flatten();
    drain_output_handlers([stdout_handle, stderr_handle]).await;
    if post_kill_timed_out && child_status.is_none() {
        match child.try_wait() {
            Ok(Some(status)) => {
                report_child_status(status_prefix, status);
            }
            Ok(None) => {}
            Err(error) => return Err(ProcessRunError::Fatal(error)),
        }
    }
    let unresolved_signal_error = signal_delivery_error.filter(|_| {
        process_tree::is_running(info.identity)
            || known.iter().copied().any(process_tree::is_running)
    });
    if let Some(error) = timeout_error {
        return Err(ProcessRunError::Fatal(error));
    }
    if let Some(error) = unresolved_signal_error {
        return Err(ProcessRunError::Fatal(error));
    }
    Ok(true)
}

fn post_kill_timeout_error(
    direct_child_running: bool,
    descendants: &HashSet<ProcessIdentity>,
) -> Option<std::io::Error> {
    post_kill_timeout_error_with(
        direct_child_running,
        descendants,
        process_tree::is_running_checked,
    )
}

fn post_kill_timeout_error_with<P>(
    direct_child_running: bool,
    descendants: &HashSet<ProcessIdentity>,
    mut probe: P,
) -> Option<std::io::Error>
where
    P: FnMut(ProcessIdentity) -> std::io::Result<bool>,
{
    let mut live_descendants = 0;
    for process in descendants {
        match probe(*process) {
            Ok(true) => live_descendants += 1,
            Ok(false) => {}
            Err(error) => {
                return Some(std::io::Error::new(
                    error.kind(),
                    format!(
                        "could not verify known descendant {} after SIGKILL: {error}",
                        process.pid
                    ),
                ));
            }
        }
    }
    if !direct_child_running && live_descendants == 0 {
        return None;
    }

    Some(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!(
            "process tree remained alive after SIGKILL (direct child: {direct_child_running}, known descendants: {live_descendants})"
        ),
    ))
}

fn related_processes(
    snapshots: &watch::Receiver<Arc<ProcessSnapshot>>,
    root: ProcessIdentity,
    known: &HashSet<ProcessIdentity>,
) -> HashSet<ProcessIdentity> {
    snapshots.borrow().related_processes(root, true, known)
}

async fn wait_for_snapshot_after(
    snapshots: &mut watch::Receiver<Arc<ProcessSnapshot>>,
    instant: std::time::Instant,
) {
    while !snapshots.borrow().started_after(instant) {
        if snapshots.changed().await.is_err() {
            break;
        }
    }
}

fn update_active_descendants(
    active_pids: &Mutex<HashMap<u32, TrackedRun>>,
    info: PidInfo,
    descendants: &HashSet<ProcessIdentity>,
) {
    let mut pids = lock_or_recover(active_pids);
    if let Some(tracked) = pids.get_mut(&info.identity.pid)
        && tracked.info.identity == info.identity
    {
        tracked.descendants.clone_from(descendants);
    }
}

fn remove_active_pid(active_pids: &Mutex<HashMap<u32, TrackedRun>>, info: PidInfo) {
    let mut pids = lock_or_recover(active_pids);
    if pids
        .get(&info.identity.pid)
        .is_some_and(|registered| registered.info.identity == info.identity)
    {
        pids.remove(&info.identity.pid);
    }
}

async fn cleanup_after_natural_exit(
    status_prefix: &PrefixWriter<'_>,
    opts: &JobConfig,
    info: PidInfo,
    known: &HashSet<ProcessIdentity>,
) -> std::io::Result<()> {
    if !info.is_process_group {
        return Ok(());
    }

    let descendants: HashSet<_> = known
        .iter()
        .copied()
        .filter(|process| process_tree::is_running(*process))
        .collect();
    if descendants.is_empty() {
        return Ok(());
    }

    eprintln!(
        "{} > Cleaning up processes left behind by exited job",
        status_prefix
    );
    let result = signal_run_with_retry(
        &status_prefix.to_string(),
        opts,
        info,
        &descendants,
        libc::SIGKILL,
    )
    .await;
    if let Err(error) = result
        && descendants.iter().copied().any(process_tree::is_running)
    {
        return Err(error);
    }
    Ok(())
}

async fn drain_output_handlers(mut handles: [JoinHandle<std::io::Result<()>>; 2]) {
    let deadline = Instant::now() + OUTPUT_DRAIN_TIMEOUT;
    for handle in &mut handles {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if timeout(remaining, &mut *handle).await.is_err() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

fn report_child_status(status_prefix: &PrefixWriter<'_>, status: ExitStatus) {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        let exit_msg = if let Some(code) = status.code() {
            format!("Exited with code {}", code)
        } else if let Some(signal) = status.signal() {
            format!("Killed by signal {}", signal)
        } else {
            "Exited with unknown status".to_string()
        };
        eprintln!("{} > {}", status_prefix, exit_msg);
    }

    #[cfg(not(unix))]
    {
        let exit_msg = match status.code() {
            Some(code) => format!("Exited with code {}", code),
            None => "Exited with unknown status".to_string(),
        };
        eprintln!("{} > {}", status_prefix, exit_msg);
    }
}

fn spawn_output_handler<R: 'static + AsyncRead + Send + Unpin>(
    reader: R,
    output_prefix: String,
    datetime: DateTimeConfig,
    stream: OutputStream,
) -> JoinHandle<std::io::Result<()>> {
    tokio::spawn(async move {
        let output_prefix = PrefixWriter::from(output_prefix, &datetime);
        let sender = output_sender(stream);
        let mut reader = reader;
        let mut input = [0_u8; 8192];
        let mut line = Vec::new();
        let mut forced_chunk_at_boundary = false;

        loop {
            let read = reader.read(&mut input).await?;
            if read == 0 {
                if !line.is_empty() {
                    send_output(&sender, &output_prefix, &line);
                }
                break;
            }

            for byte in &input[..read] {
                if *byte == b'\n' {
                    if !forced_chunk_at_boundary || !line.is_empty() {
                        send_output(&sender, &output_prefix, &line);
                    }
                    line.clear();
                    forced_chunk_at_boundary = false;
                } else {
                    forced_chunk_at_boundary = false;
                    line.push(*byte);
                    if line.len() == MAX_OUTPUT_FRAME_BYTES {
                        send_output(&sender, &output_prefix, &line);
                        line.clear();
                        forced_chunk_at_boundary = true;
                    }
                }
            }
        }

        Ok(())
    })
}

fn output_sender(stream: OutputStream) -> SyncSender<OutputMessage> {
    let sinks = OUTPUT_SINKS.get_or_init(|| OutputSinks {
        stdout: spawn_output_sink(OutputStream::Stdout),
        stderr: spawn_output_sink(OutputStream::Stderr),
    });
    match stream {
        OutputStream::Stdout => sinks.stdout.clone(),
        OutputStream::Stderr => sinks.stderr.clone(),
    }
}

fn spawn_output_sink(stream: OutputStream) -> SyncSender<OutputMessage> {
    let (sender, receiver) = sync_channel::<OutputMessage>(OUTPUT_CHANNEL_CAPACITY);
    std::thread::Builder::new()
        .name(
            match stream {
                OutputStream::Stdout => "job-runner-stdout",
                OutputStream::Stderr => "job-runner-stderr",
            }
            .to_string(),
        )
        .spawn(move || {
            let mut writer: Box<dyn Write> = match stream {
                OutputStream::Stdout => Box::new(std::io::stdout()),
                OutputStream::Stderr => Box::new(std::io::stderr()),
            };
            let mut writable = true;
            while let Ok(message) = receiver.recv() {
                match message {
                    OutputMessage::Frame(output) => {
                        if writable && writer.write_all(&output).is_err() {
                            writable = false;
                        }
                        if writable {
                            let _ = writer.flush();
                        }
                    }
                    OutputMessage::Flush(acknowledge) => {
                        if writable {
                            let _ = writer.flush();
                        }
                        let _ = acknowledge.try_send(());
                    }
                }
            }
        })
        .expect("failed to start output writer thread");
    sender
}

fn bounded_format(arguments: std::fmt::Arguments<'_>, limit: usize) -> Vec<u8> {
    let mut output = BoundedOutput::new(limit);
    let _ = std::fmt::write(&mut output, arguments);
    output.bytes
}

fn send_output(sender: &SyncSender<OutputMessage>, output_prefix: &PrefixWriter<'_>, line: &[u8]) {
    let prefix = bounded_format(
        format_args!("{} > ", output_prefix),
        MAX_OUTPUT_PREFIX_BYTES,
    );
    let payload_limit = MAX_OUTPUT_FRAME_BYTES - prefix.len() - 1;

    if line.is_empty() {
        send_output_frame(sender, &prefix, line);
        return;
    }
    for chunk in line.chunks(payload_limit) {
        send_output_frame(sender, &prefix, chunk);
    }
}

fn send_output_frame(sender: &SyncSender<OutputMessage>, prefix: &[u8], payload: &[u8]) {
    let mut output = Vec::with_capacity(prefix.len() + payload.len() + 1);
    output.extend_from_slice(prefix);
    output.extend_from_slice(payload);
    output.push(b'\n');
    debug_assert!(output.len() <= MAX_OUTPUT_FRAME_BYTES);
    let _ = sender.try_send(OutputMessage::Frame(output));
}

fn try_log_stderr(arguments: std::fmt::Arguments<'_>) {
    let mut output = bounded_format(arguments, MAX_OUTPUT_FRAME_BYTES - 1);
    output.push(b'\n');
    let _ = output_sender(OutputStream::Stderr).try_send(OutputMessage::Frame(output));
}

fn flush_output_sinks() {
    let Some(sinks) = OUTPUT_SINKS.get() else {
        return;
    };
    flush_output_sink(&sinks.stdout);
    flush_output_sink(&sinks.stderr);
}

fn flush_output_sink(sender: &SyncSender<OutputMessage>) {
    let deadline = std::time::Instant::now() + OUTPUT_FLUSH_TIMEOUT;
    let (acknowledge, receiver) = sync_channel(1);
    let mut message = OutputMessage::Flush(acknowledge);

    loop {
        match sender.try_send(message) {
            Ok(()) => break,
            Err(TrySendError::Full(returned)) => {
                if std::time::Instant::now() >= deadline {
                    return;
                }
                message = returned;
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(TrySendError::Disconnected(_)) => return,
        }
    }

    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    let _ = receiver.recv_timeout(remaining);
}

fn create_prefix(color: &str, tag: &str) -> String {
    let colors = [
        ("red", "\x1b[31m"),
        ("green", "\x1b[32m"),
        ("yellow", "\x1b[33m"),
        ("blue", "\x1b[34m"),
        ("magenta", "\x1b[35m"),
        ("cyan", "\x1b[36m"),
        ("white", "\x1b[37m"),
        ("gray", "\x1b[90m"),
    ];

    let code = match colors.iter().find(|(name, _)| *name == color) {
        Some((_, code)) => code,
        None => {
            std::eprintln!("No such color, ignoring: {}", color);
            return tag.to_string();
        }
    };

    let reset = "\x1b[0m";
    format!("{}{}{}", code, tag, reset)
}

fn main() -> ExitCode {
    if unsafe { libc::getpid() } == 1 {
        std::eprintln!(
            "Refusing to run as PID 1; use an external init such as `tini -- job-runner-rs ...`"
        );
        return ExitCode::from(1);
    }

    let args = env::args().collect::<Vec<_>>();
    if args.len() < 2 || args.iter().any(|arg| arg.as_str() == "--help") {
        std::eprintln!("Usage: {} [--wrap] [-t NUM_THREADS] config.yaml", args[0]);
        std::eprintln!("  --wrap          Run in wrapper mode (auto-restart on exit)");
        std::eprintln!("  -t NUM_THREADS  Number of runtime threads (default: 1)");
        return ExitCode::from(1);
    }

    let mut num_threads: usize = 1;
    let mut config_path: Option<&str> = None;
    let mut wrap_mode = false;
    let mut i = 1;

    while i < args.len() {
        match args[i].as_str() {
            "--wrap" => {
                wrap_mode = true;
                i += 1;
            }
            "-t" => {
                if i + 1 >= args.len() {
                    std::eprintln!("Error: -t requires a number argument");
                    return ExitCode::from(1);
                }
                match args[i + 1].parse::<usize>() {
                    Ok(n) if n >= 1 => num_threads = n,
                    _ => {
                        std::eprintln!("Error: -t requires a positive integer");
                        return ExitCode::from(1);
                    }
                }
                i += 2;
            }
            arg if arg.starts_with('-') => {
                std::eprintln!("Unknown option: {}", arg);
                return ExitCode::from(1);
            }
            _ => {
                config_path = Some(&args[i]);
                i += 1;
            }
        }
    }

    let Some(config_path) = config_path else {
        std::eprintln!("No config file provided.");
        return ExitCode::from(1);
    };

    if wrap_mode {
        let wrapper_args: Vec<String> = args[1..]
            .iter()
            .filter(|a| *a != "--wrap")
            .cloned()
            .collect();
        let exit_code = run_wrapper_mode(&args[0], &wrapper_args);
        flush_output_sinks();
        return exit_code;
    }

    let home_path = env::var("HOME").unwrap_or_else(|_| ".".to_string());
    let runner_cwd_path = env::current_dir()
        .ok()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|| ".".to_string());

    let config_data = match std::fs::read_to_string(config_path) {
        Ok(data) => data,
        Err(e) => {
            std::eprintln!("Could not read config file: {}", e);
            return ExitCode::from(1);
        }
    };
    let config_parent_path = std::fs::canonicalize(config_path)
        .ok()
        .and_then(|path| path.parent().map(std::path::Path::to_path_buf))
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|| runner_cwd_path.clone());

    let strip_json_comments = |data: &str| -> String {
        data.lines()
            .filter(|s| !s.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    };

    let is_json_ext = config_path.ends_with(".json");
    let is_yaml_ext = config_path.ends_with(".yaml") || config_path.ends_with(".yml");

    let mut config: Config = if is_json_ext {
        match serde_json::from_str(&strip_json_comments(&config_data)) {
            Ok(c) => c,
            Err(e) => {
                std::eprintln!("Could not parse JSON config file: {}", e);
                return ExitCode::from(1);
            }
        }
    } else if is_yaml_ext {
        match serde_yaml::from_str(&config_data) {
            Ok(c) => c,
            Err(e) => {
                std::eprintln!("Could not parse YAML config file: {}", e);
                return ExitCode::from(1);
            }
        }
    } else {
        match serde_yaml::from_str(&config_data) {
            Ok(c) => c,
            Err(yaml_err) => match serde_json::from_str(&strip_json_comments(&config_data)) {
                Ok(c) => c,
                Err(json_err) => {
                    std::eprintln!("Could not parse config file.");
                    std::eprintln!("  As YAML: {}", yaml_err);
                    std::eprintln!("  As JSON: {}", json_err);
                    return ExitCode::from(1);
                }
            },
        }
    };

    let print_colors = [
        "red", "green", "yellow", "blue", "magenta", "cyan", "white", "gray",
    ];

    let mut rng = rand::rng();
    let mut next_random_color_index = rng.random_range(0..print_colors.len());

    for job_config in config.jobs.values_mut() {
        if job_config.color.is_empty() {
            job_config.color = print_colors[next_random_color_index].to_string();
            next_random_color_index = (next_random_color_index + 1) % print_colors.len();
        }
        if job_config.cwd.is_empty() {
            job_config.cwd = match env::current_dir() {
                Ok(p) => p.to_string_lossy().into_owned(),
                Err(e) => {
                    std::eprintln!(
                        "Could not read current directory, defaulting to $HOME: {}",
                        e
                    );
                    home_path.clone()
                }
            };
        }
        job_config.cwd = job_config
            .cwd
            .replace("#cwd#", &config_parent_path)
            .replace("#home#", &home_path)
            .replace("#runner_cwd#", &runner_cwd_path);
        job_config.cwd = match job_config.cwd.as_str() {
            "$CWD" => config_parent_path.clone(),
            "$HOME" => home_path.clone(),
            "$RUNNER_CWD" => runner_cwd_path.clone(),
            _ => job_config.cwd.clone(),
        };

        job_config.command = job_config
            .command
            .replace("#cwd#", &config_parent_path)
            .replace("#home#", &home_path)
            .replace("#runner_cwd#", &runner_cwd_path);
    }

    let mut base_env: HashMap<String, String> = env::vars().collect();
    base_env.insert("CWD".to_string(), config_parent_path);
    base_env.insert("RUNNER_CWD".to_string(), runner_cwd_path);
    let base_env = Arc::new(base_env);

    let runtime = {
        let mut builder = if num_threads == 1 {
            Builder::new_current_thread()
        } else {
            let mut mt = Builder::new_multi_thread();
            mt.worker_threads(num_threads);
            mt
        };
        match builder.enable_io().enable_time().build() {
            Ok(rt) => rt,
            Err(e) => {
                std::eprintln!("Could not build tokio runtime: {}", e);
                return ExitCode::from(1);
            }
        }
    };

    let exit_code = match runtime.block_on(run_supervisor(config, base_env)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            std::eprintln!("Failed to run supervisor: {}", error);
            ExitCode::from(1)
        }
    };
    flush_output_sinks();
    exit_code
}

async fn run_supervisor(
    config: Config,
    base_env: Arc<HashMap<String, String>>,
) -> std::io::Result<()> {
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sighup = signal(SignalKind::hangup())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigquit = signal(SignalKind::quit())?;
    let mut sigusr1 = signal(SignalKind::user_defined1())?;
    let mut sigusr2 = signal(SignalKind::user_defined2())?;

    let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownState::Running);
    let (snapshot_tx, snapshot_rx) = watch::channel(Arc::new(ProcessSnapshot::empty()));
    let (scanner_request_tx, scanner_request_rx) = channel();
    let mut scanner_handle = spawn_blocking(move || {
        run_process_scanner(
            snapshot_tx,
            scanner_request_rx,
            PROCESS_SCAN_INTERVAL,
            ProcessSnapshot::capture,
        )
    });
    if let Err(error) = refresh_process_snapshot(&scanner_request_tx).await {
        let _ = scanner_handle.await;
        return Err(error);
    }
    let mut scanner_finished = false;
    let mut pid_map: HashMap<String, Arc<Mutex<HashMap<u32, TrackedRun>>>> = HashMap::new();
    let mut job_handles = JoinSet::new();

    for (name, job_config) in config.jobs {
        if job_config.disabled {
            eprintln!("! Skipping disabled job: {}", name);
            continue;
        }
        if job_config.command.is_empty() {
            eprintln!("! Skipping job with missing command: {}", name);
            continue;
        }

        eprintln!("Initializing job \"{}\"", name);
        let prefix = create_prefix(&job_config.color, &name);
        let sudo_user = job_config.sudo_user.clone().unwrap_or_default();
        let pid_set = match pid_map.entry(sudo_user) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => entry.insert(Arc::new(Mutex::new(HashMap::new()))).clone(),
        };
        let job_shutdown = shutdown_rx.clone();
        let job_snapshots = snapshot_rx.clone();
        let job_env = base_env.clone();
        job_handles.spawn(async move {
            start_proc(
                prefix,
                job_config,
                pid_set,
                job_shutdown,
                job_snapshots,
                job_env,
            )
            .await
        });
    }

    let (first_signal, mut invariant_failed) = loop {
        tokio::select! {
            _ = sigterm.recv() => {
                match refresh_process_snapshot(&scanner_request_tx).await {
                    Ok(_) => break ("TERM", false),
                    Err(error) => {
                        eprintln!("Could not refresh process snapshot: {}", error);
                        break ("SCANNER FAILURE", true);
                    }
                }
            }
            _ = sighup.recv() => {
                match refresh_process_snapshot(&scanner_request_tx).await {
                    Ok(_) => break ("HUP", false),
                    Err(error) => {
                        eprintln!("Could not refresh process snapshot: {}", error);
                        break ("SCANNER FAILURE", true);
                    }
                }
            }
            _ = sigint.recv() => {
                match refresh_process_snapshot(&scanner_request_tx).await {
                    Ok(_) => break ("INT", false),
                    Err(error) => {
                        eprintln!("Could not refresh process snapshot: {}", error);
                        break ("SCANNER FAILURE", true);
                    }
                }
            }
            _ = sigquit.recv() => {
                match refresh_process_snapshot(&scanner_request_tx).await {
                    Ok(_) => break ("QUIT", false),
                    Err(error) => {
                        eprintln!("Could not refresh process snapshot: {}", error);
                        break ("SCANNER FAILURE", true);
                    }
                }
            }
            _ = sigusr1.recv() => {
                eprintln!("Received USR1 signal, forwarding to all jobs..");
                match refresh_process_snapshot(&scanner_request_tx).await {
                    Ok(snapshot) => {
                        if let Err(error) = forward_signal_to_jobs(&pid_map, &snapshot, libc::SIGUSR1).await {
                            eprintln!("Signal forwarding failed: {}", error);
                            break ("SIGNAL FAILURE", true);
                        }
                    }
                    Err(error) => {
                        eprintln!("Could not refresh process snapshot: {}", error);
                        break ("SCANNER FAILURE", true);
                    }
                }
            }
            _ = sigusr2.recv() => {
                eprintln!("Received USR2 signal, forwarding to all jobs..");
                match refresh_process_snapshot(&scanner_request_tx).await {
                    Ok(snapshot) => {
                        if let Err(error) = forward_signal_to_jobs(&pid_map, &snapshot, libc::SIGUSR2).await {
                            eprintln!("Signal forwarding failed: {}", error);
                            break ("SIGNAL FAILURE", true);
                        }
                    }
                    Err(error) => {
                        eprintln!("Could not refresh process snapshot: {}", error);
                        break ("SCANNER FAILURE", true);
                    }
                }
            }
            result = job_handles.join_next(), if !job_handles.is_empty() => {
                match result {
                    Some(Ok(Ok(()))) => eprintln!("Job supervisor task stopped unexpectedly"),
                    Some(Ok(Err(error))) => eprintln!("Job supervisor failed: {}", error),
                    Some(Err(error)) => eprintln!("Job supervisor task failed: {}", error),
                    None => {}
                }
                break ("SUPERVISOR FAILURE", true);
            }
            result = &mut scanner_handle, if !scanner_finished => {
                scanner_finished = true;
                log_unexpected_scanner_stop(result);
                break ("SCANNER FAILURE", true);
            }
        }
    };

    eprintln!(
        "\nReceived {} signal, shutting down active processes..",
        first_signal
    );
    let _ = shutdown_tx.send(ShutdownState::Graceful);

    let mut escalated = false;
    while !job_handles.is_empty() {
        tokio::select! {
            _ = sigterm.recv() => escalate_shutdown(&shutdown_tx, &mut escalated),
            _ = sighup.recv() => escalate_shutdown(&shutdown_tx, &mut escalated),
            _ = sigint.recv() => escalate_shutdown(&shutdown_tx, &mut escalated),
            _ = sigquit.recv() => escalate_shutdown(&shutdown_tx, &mut escalated),
            _ = sigusr1.recv() => {}
            _ = sigusr2.recv() => {}
            result = job_handles.join_next() => {
                match result {
                    Some(Ok(Ok(()))) | None => {}
                    Some(Ok(Err(error))) => {
                        eprintln!("Job supervisor failed during shutdown: {}", error);
                        invariant_failed = true;
                    }
                    Some(Err(error)) => {
                        eprintln!("Job supervisor task failed during shutdown: {}", error);
                        invariant_failed = true;
                    }
                }
            }
            result = &mut scanner_handle, if !scanner_finished => {
                scanner_finished = true;
                log_unexpected_scanner_stop(result);
                invariant_failed = true;
            }
        }
    }

    if !scanner_finished {
        let _ = scanner_request_tx.send(ProcessScannerRequest::Stop);
        match scanner_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                eprintln!("Process scanner failed: {}", error);
                invariant_failed = true;
            }
            Err(error) => {
                eprintln!("Process scanner task failed: {}", error);
                invariant_failed = true;
            }
        }
    }

    if invariant_failed {
        Err(std::io::Error::other("job supervisor task failed"))
    } else {
        Ok(())
    }
}

fn run_process_scanner<C>(
    snapshot_tx: watch::Sender<Arc<ProcessSnapshot>>,
    scanner_request_rx: std::sync::mpsc::Receiver<ProcessScannerRequest>,
    scan_interval: Duration,
    mut capture: C,
) -> std::io::Result<()>
where
    C: FnMut() -> std::io::Result<ProcessSnapshot>,
{
    loop {
        match scanner_request_rx.recv_timeout(scan_interval) {
            Ok(ProcessScannerRequest::Refresh(acknowledge)) => match capture() {
                Ok(snapshot) => {
                    let snapshot = Arc::new(snapshot);
                    let published = snapshot_tx.send(snapshot.clone()).is_ok();
                    let _ = acknowledge.send(Ok(snapshot));
                    if !published {
                        return Ok(());
                    }
                }
                Err(error) => {
                    let scanner_error = std::io::Error::new(error.kind(), error.to_string());
                    let _ = acknowledge.send(Err(error));
                    return Err(scanner_error);
                }
            },
            Ok(ProcessScannerRequest::Stop) | Err(RecvTimeoutError::Disconnected) => return Ok(()),
            Err(RecvTimeoutError::Timeout) => {
                if snapshot_tx.send(Arc::new(capture()?)).is_err() {
                    return Ok(());
                }
            }
        }
    }
}

fn log_unexpected_scanner_stop(result: Result<std::io::Result<()>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => eprintln!("Process scanner stopped unexpectedly"),
        Ok(Err(error)) => eprintln!("Process scanner failed: {}", error),
        Err(error) => eprintln!("Process scanner task failed: {}", error),
    }
}

async fn refresh_process_snapshot(
    scanner: &Sender<ProcessScannerRequest>,
) -> std::io::Result<Arc<ProcessSnapshot>> {
    let (acknowledge, receiver) = oneshot::channel();
    scanner
        .send(ProcessScannerRequest::Refresh(acknowledge))
        .map_err(|_| std::io::Error::other("process scanner is unavailable"))?;
    receiver
        .await
        .map_err(|error| std::io::Error::other(format!("process scanner failed: {error}")))?
}

fn escalate_shutdown(shutdown: &watch::Sender<ShutdownState>, escalated: &mut bool) {
    if !*escalated {
        *escalated = true;
        eprintln!("Received another termination signal, forcing shutdown..");
        let _ = shutdown.send(ShutdownState::Force);
    }
}

fn signal_number_to_name(signal: libc::c_int) -> &'static str {
    match signal {
        libc::SIGTERM => "TERM",
        libc::SIGKILL => "KILL",
        libc::SIGUSR1 => "USR1",
        libc::SIGUSR2 => "USR2",
        libc::SIGHUP => "HUP",
        libc::SIGINT => "INT",
        libc::SIGQUIT => "QUIT",
        _ => "UNKNOWN",
    }
}

fn send_signal_target(target: i32, signal: libc::c_int) -> std::io::Result<()> {
    if unsafe { libc::kill(target, signal) } == 0 {
        return Ok(());
    }
    Err(std::io::Error::last_os_error())
}

async fn forward_signal_to_jobs(
    pid_map: &HashMap<String, Arc<Mutex<HashMap<u32, TrackedRun>>>>,
    snapshot: &ProcessSnapshot,
    signal: libc::c_int,
) -> std::io::Result<()> {
    let mut targets = Vec::new();
    let mut delivery_error = None;
    for (sudo_user, pids) in pid_map {
        targets.extend(
            lock_or_recover(pids)
                .values()
                .cloned()
                .map(|tracked| (sudo_user.clone(), tracked)),
        );
    }

    for (sudo_user, tracked) in targets {
        let descendants = if tracked.info.is_process_group {
            snapshot.related_processes(tracked.info.identity, true, &tracked.descendants)
        } else {
            HashSet::new()
        };
        if !process_tree::exists(tracked.info.identity)
            && !descendants.iter().copied().any(process_tree::is_running)
        {
            continue;
        }
        if let Err(error) = signal_process_tree(
            "Signal forwarding",
            (!sudo_user.is_empty()).then_some(sudo_user.as_str()),
            tracked.info,
            &descendants,
            signal,
        )
        .await
        {
            delivery_error.get_or_insert(error);
        }
    }
    delivery_error.map_or(Ok(()), Err)
}

async fn signal_run(
    log_prefix: &str,
    opts: &JobConfig,
    info: PidInfo,
    descendants: &HashSet<ProcessIdentity>,
    signal: libc::c_int,
) -> std::io::Result<()> {
    signal_process_tree(
        log_prefix,
        opts.sudo_user.as_deref(),
        info,
        descendants,
        signal,
    )
    .await
}

async fn signal_run_with_retry(
    log_prefix: &str,
    opts: &JobConfig,
    info: PidInfo,
    descendants: &HashSet<ProcessIdentity>,
    signal: libc::c_int,
) -> std::io::Result<()> {
    let mut last_error = None;
    let attempts = if opts.sudo_user.is_some() {
        1
    } else {
        SIGNAL_RETRY_LIMIT
    };
    for attempt in 0..attempts {
        match signal_run(log_prefix, opts, info, descendants, signal).await {
            Ok(()) => return Ok(()),
            Err(error) => last_error = Some(error),
        }
        if attempt + 1 < attempts {
            sleep(SIGNAL_RETRY_INTERVAL).await;
        }
    }
    Err(last_error.unwrap_or_else(|| std::io::Error::other("signal delivery failed")))
}

async fn signal_process_tree(
    log_prefix: &str,
    sudo_user: Option<&str>,
    info: PidInfo,
    descendants: &HashSet<ProcessIdentity>,
    signal: libc::c_int,
) -> std::io::Result<()> {
    let root = info.identity;
    let root_exists = process_tree::exists(root);
    if let Some(username) = sudo_user {
        let mut delivery_error = None;
        if signal == libc::SIGKILL
            && root_exists
            && let Ok(pid) = i32::try_from(root.pid)
            && let Err(error) = send_signal_target(pid, signal)
            && error.raw_os_error() != Some(libc::ESRCH)
        {
            delivery_error = Some(error);
        }

        let targets: Vec<ProcessIdentity> = descendants
            .iter()
            .copied()
            .filter(|process| process_tree::is_running(*process))
            .collect();
        if targets.is_empty() {
            if signal != libc::SIGKILL
                && root_exists
                && let Ok(pid) = i32::try_from(root.pid)
                && let Err(error) = send_signal_target(pid, signal)
                && error.raw_os_error() != Some(libc::ESRCH)
            {
                delivery_error = Some(error);
            }
        } else {
            for process in targets {
                if let Err(error) =
                    signal_identity_as_user(log_prefix, username, signal, process).await
                {
                    delivery_error.get_or_insert(error);
                }
            }
        }
        return delivery_error.map_or(Ok(()), Err);
    }

    let group_is_owned = root_exists
        || descendants
            .iter()
            .copied()
            .any(|process| process.process_group == root.pid && process_tree::is_running(process));
    let mut targets = Vec::new();

    if info.is_process_group && group_is_owned {
        if let Ok(pid) = i32::try_from(root.pid) {
            targets.push(-pid);
        }
    } else if !info.is_process_group
        && root_exists
        && let Ok(pid) = i32::try_from(root.pid)
    {
        targets.push(pid);
    }

    if info.is_process_group {
        for process in descendants {
            if process.process_group == root.pid || !process_tree::is_running(*process) {
                continue;
            }
            let Ok(pid) = i32::try_from(process.pid) else {
                continue;
            };
            targets.push(pid);
        }
    }

    if targets.is_empty() {
        return Ok(());
    }
    let mut delivery_error = None;
    for target in targets {
        if let Err(error) = send_signal_target(target, signal)
            && error.raw_os_error() != Some(libc::ESRCH)
        {
            eprintln!(
                "{} > Failed to send {} to target {}: {}",
                log_prefix,
                signal_number_to_name(signal),
                target,
                error
            );
            delivery_error.get_or_insert(error);
        }
    }
    delivery_error.map_or(Ok(()), Err)
}

async fn signal_as_user(
    log_prefix: &str,
    username: &str,
    signal: libc::c_int,
    targets: &[i32],
) -> std::io::Result<()> {
    let mut command = Command::new("sudo");
    command
        .args([
            "-n",
            "-u",
            username,
            "--",
            "sh",
            "-c",
            "signal=$1; shift; failed=0; for target do kill \"$signal\" \"$target\" || failed=1; done; exit \"$failed\"",
            "job-runner-signal",
            &format!("-{}", signal_number_to_name(signal)),
        ])
        .args(targets.iter().map(i32::to_string))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);

    let mut child = command.spawn().map_err(|error| {
        std::io::Error::new(
            error.kind(),
            format!("{log_prefix} > Could not run sudo signal helper: {error}"),
        )
    })?;
    match timeout(SUDO_SIGNAL_TIMEOUT, child.wait()).await {
        Ok(Ok(status)) if status.success() => Ok(()),
        Ok(Ok(status)) => Err(std::io::Error::other(format!(
            "{log_prefix} > sudo signal helper exited with {status}"
        ))),
        Ok(Err(error)) => Err(std::io::Error::new(
            error.kind(),
            format!("{log_prefix} > sudo signal helper failed: {error}"),
        )),
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("{log_prefix} > sudo signal helper timed out"),
            ))
        }
    }
}

async fn signal_identity_as_user(
    log_prefix: &str,
    username: &str,
    signal: libc::c_int,
    process: ProcessIdentity,
) -> std::io::Result<()> {
    let pid = i32::try_from(process.pid)
        .map_err(|_| std::io::Error::other("process ID does not fit signal target"))?;
    let mut last_error = None;
    for attempt in 0..SIGNAL_RETRY_LIMIT {
        if !process_tree::is_running(process) {
            return Ok(());
        }
        match signal_as_user(log_prefix, username, signal, &[pid]).await {
            Ok(()) => return Ok(()),
            Err(error) => last_error = Some(error),
        }
        if attempt + 1 < SIGNAL_RETRY_LIMIT {
            sleep(SIGNAL_RETRY_INTERVAL).await;
        }
    }
    if !process_tree::is_running(process) {
        return Ok(());
    }
    Err(last_error.unwrap_or_else(|| std::io::Error::other("sudo signal delivery failed")))
}

fn run_wrapper_mode(self_path: &str, inner_args: &[String]) -> ExitCode {
    use std::os::unix::process::CommandExt;

    let signal_state = Arc::new(Mutex::new(WrapperSignalState::default()));
    let shutdown_state = Arc::new(AtomicU8::new(0));
    let (ready_sender, ready_receiver) = sync_channel(1);
    let handler_signal_state = signal_state.clone();
    let handler_shutdown = shutdown_state.clone();

    std::thread::spawn(move || {
        let runtime = match Builder::new_current_thread().enable_io().build() {
            Ok(runtime) => runtime,
            Err(error) => {
                let _ = ready_sender.send(Err(error.to_string()));
                return;
            }
        };

        macro_rules! register_wrapper_signal {
            ($kind:expr) => {
                match signal($kind) {
                    Ok(signal) => signal,
                    Err(error) => {
                        let _ = ready_sender.send(Err(error.to_string()));
                        return;
                    }
                }
            };
        }

        let runtime_guard = runtime.enter();
        let mut sigterm = register_wrapper_signal!(SignalKind::terminate());
        let mut sigint = register_wrapper_signal!(SignalKind::interrupt());
        let mut sighup = register_wrapper_signal!(SignalKind::hangup());
        let mut sigquit = register_wrapper_signal!(SignalKind::quit());
        let mut sigusr1 = register_wrapper_signal!(SignalKind::user_defined1());
        let mut sigusr2 = register_wrapper_signal!(SignalKind::user_defined2());
        drop(runtime_guard);
        if ready_sender.send(Ok(())).is_err() {
            return;
        }

        runtime.block_on(async move {
            loop {
                let (received, terminates) = tokio::select! {
                    _ = sigterm.recv() => (libc::SIGTERM, true),
                    _ = sigint.recv() => (libc::SIGINT, true),
                    _ = sighup.recv() => (libc::SIGHUP, true),
                    _ = sigquit.recv() => (libc::SIGQUIT, true),
                    _ = sigusr1.recv() => (libc::SIGUSR1, false),
                    _ = sigusr2.recv() => (libc::SIGUSR2, false),
                };

                if terminates {
                    let previous = handler_shutdown.swap(1, Ordering::AcqRel);
                    if previous == 0 {
                        eprintln!(
                            "\n[Wrapper] Received termination signal, forwarding to inner process.."
                        );
                    } else {
                        handler_shutdown.store(2, Ordering::Release);
                        eprintln!(
                            "\n[Wrapper] Received another termination signal, escalating inner shutdown.."
                        );
                    }
                }
                forward_or_queue_wrapper_signal(&handler_signal_state, received);
            }
        });
    });

    match ready_receiver.recv() {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            eprintln!("[Wrapper] Failed to register signal handlers: {error}");
            return ExitCode::from(1);
        }
        Err(error) => {
            eprintln!("[Wrapper] Signal handler thread failed: {error}");
            return ExitCode::from(1);
        }
    }

    loop {
        if shutdown_state.load(Ordering::Acquire) != 0 {
            eprintln!("[Wrapper] Shutdown requested, exiting..");
            return ExitCode::SUCCESS;
        }

        eprintln!("[Wrapper] Starting inner process..");
        lock_or_recover(&signal_state).begin_publication();
        if shutdown_state.load(Ordering::Acquire) != 0 {
            lock_or_recover(&signal_state).cancel_publication();
            eprintln!("[Wrapper] Shutdown requested, exiting..");
            return ExitCode::SUCCESS;
        }
        let mut command = std::process::Command::new(self_path);
        command
            .args(inner_args)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = match command.spawn() {
            Ok(c) => c,
            Err(e) => {
                lock_or_recover(&signal_state).cancel_publication();
                eprintln!("[Wrapper] Failed to spawn inner process: {}", e);
                wrapper_delay(&shutdown_state, Duration::from_secs(15));
                continue;
            }
        };

        let identity = match process_tree::identity(child.id()) {
            Ok(identity) => {
                publish_wrapper_inner(&signal_state, identity);
                identity
            }
            Err(error) => {
                lock_or_recover(&signal_state).cancel_publication();
                eprintln!("[Wrapper] Could not inspect inner process: {}", error);
                let _ = child.kill();
                let _ = child.wait();
                if shutdown_state.load(Ordering::Acquire) != 0 {
                    eprintln!("[Wrapper] Shutdown complete, exiting..");
                    return ExitCode::SUCCESS;
                }
                eprintln!(
                    "[Wrapper] Inner process could not be supervised, waiting 15 seconds before restarting.."
                );
                wrapper_delay(&shutdown_state, Duration::from_secs(15));
                continue;
            }
        };

        match child.wait() {
            Ok(exit_status) => match exit_status.code() {
                Some(code) => eprintln!("[Wrapper] Exited with status code: {code}"),
                None => eprintln!("[Wrapper] Process terminated by signal"),
            },
            Err(e) => {
                eprintln!("[Wrapper] Failed to read exit status: {}", e);
            }
        }
        lock_or_recover(&signal_state).clear(identity);

        if shutdown_state.load(Ordering::Acquire) != 0 {
            eprintln!("[Wrapper] Shutdown complete, exiting..");
            return ExitCode::SUCCESS;
        }

        eprintln!("[Wrapper] Inner process exited, waiting 15 seconds before restarting..");
        wrapper_delay(&shutdown_state, Duration::from_secs(15));
    }
}

fn forward_or_queue_wrapper_signal(state: &Mutex<WrapperSignalState>, signal: libc::c_int) {
    let mut state = lock_or_recover(state);
    if let Some((identity, signal)) = state.receive(signal) {
        signal_wrapper_identity(identity, signal);
    }
}

fn publish_wrapper_inner(state: &Mutex<WrapperSignalState>, identity: ProcessIdentity) {
    let mut state = lock_or_recover(state);
    for signal in state.publish(identity) {
        signal_wrapper_identity(identity, signal);
    }
}

fn signal_wrapper_identity(identity: ProcessIdentity, signal: libc::c_int) {
    if process_tree::exists(identity)
        && let Ok(pid) = i32::try_from(identity.pid)
    {
        let _ = send_signal_target(pid, signal);
    }
}

fn wrapper_delay(shutdown: &AtomicU8, duration: Duration) {
    let deadline = std::time::Instant::now() + duration;
    while shutdown.load(Ordering::Acquire) == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_group_defaults_to_enabled() {
        let config: Config =
            serde_yaml::from_str("jobs:\n  example:\n    command: echo ok\n").unwrap();
        let job = &config.jobs["example"];

        assert!(job.process_group);
        assert_eq!(job.shutdown_timeout, 5);
        assert_eq!(job.restart_delay, 15_000);
    }

    #[test]
    fn process_group_can_be_explicitly_disabled() {
        let config: Config = serde_yaml::from_str(
            "jobs:\n  example:\n    command: echo ok\n    process_group: false\n",
        )
        .unwrap();
        assert!(!config.jobs["example"].process_group);
    }

    #[test]
    fn validates_usernames_and_environment_keys() {
        assert!(is_valid_username("service-user_2"));
        assert!(!is_valid_username(""));
        assert!(!is_valid_username("root --preserve-env"));

        assert!(is_valid_env_key("-LEADING_DASH"));
        assert!(!is_valid_env_key(""));
        assert!(!is_valid_env_key("A=B"));
        assert!(!is_valid_env_key("A\0B"));
    }

    #[test]
    fn maps_supported_signal_numbers_to_names() {
        assert_eq!(signal_number_to_name(libc::SIGTERM), "TERM");
        assert_eq!(signal_number_to_name(libc::SIGKILL), "KILL");
        assert_eq!(signal_number_to_name(libc::SIGUSR1), "USR1");
    }

    #[test]
    fn complete_output_frames_are_bounded() {
        let datetime = DateTimeConfig::default();
        let prefix = PrefixWriter::from("x".repeat(MAX_OUTPUT_FRAME_BYTES * 2), &datetime);
        let line = vec![b'y'; MAX_OUTPUT_FRAME_BYTES * 2];
        let (sender, receiver) = sync_channel(8);

        send_output(&sender, &prefix, &line);
        drop(sender);

        let frames: Vec<Vec<u8>> = receiver
            .into_iter()
            .map(|message| match message {
                OutputMessage::Frame(frame) => frame,
                OutputMessage::Flush(_) => panic!("unexpected flush barrier"),
            })
            .collect();
        assert!(frames.len() > 1);
        assert!(
            frames
                .iter()
                .all(|frame| frame.len() <= MAX_OUTPUT_FRAME_BYTES)
        );
    }

    #[test]
    fn diagnostics_are_formatted_without_exceeding_frame_limit() {
        let diagnostic = "é".repeat(MAX_OUTPUT_FRAME_BYTES);
        let output = bounded_format(format_args!("{diagnostic}"), MAX_OUTPUT_FRAME_BYTES - 1);

        assert!(output.len() < MAX_OUTPUT_FRAME_BYTES);
        assert!(std::str::from_utf8(&output).is_ok());
    }

    #[test]
    fn wrapper_signal_handoff_preserves_each_signal_once() {
        let identity = ProcessIdentity {
            pid: 123,
            process_group: 123,
            start_time: 456,
        };
        let mut state = WrapperSignalState::default();

        assert_eq!(state.receive(libc::SIGUSR1), None);
        state.begin_publication();
        assert_eq!(state.receive(libc::SIGUSR1), None);
        assert_eq!(state.receive(libc::SIGUSR1), None);
        assert_eq!(state.receive(libc::SIGINT), None);
        assert_eq!(state.receive(libc::SIGTERM), None);
        assert_eq!(state.receive(libc::SIGHUP), None);
        assert_eq!(
            state.publish(identity),
            [libc::SIGINT, libc::SIGTERM, libc::SIGUSR1]
        );
        assert_eq!(
            state.receive(libc::SIGUSR2),
            Some((identity, libc::SIGUSR2))
        );
        state.clear(identity);
        assert_eq!(state.receive(libc::SIGTERM), None);

        state.begin_publication();
        assert_eq!(state.receive(libc::SIGUSR2), None);
        state.cancel_publication();
        state.begin_publication();
        assert!(state.publish(identity).is_empty());
    }

    #[test]
    fn scanner_refresh_propagates_capture_failure() {
        let (snapshot_tx, _snapshot_rx) = watch::channel(Arc::new(ProcessSnapshot::empty()));
        let (request_tx, request_rx) = channel();
        let scanner = std::thread::spawn(move || {
            run_process_scanner(snapshot_tx, request_rx, Duration::from_secs(60), || {
                Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
            })
        });
        let runtime = Builder::new_current_thread().build().unwrap();

        let error = runtime
            .block_on(refresh_process_snapshot(&request_tx))
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(
            scanner.join().unwrap().unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn periodic_capture_failure_stops_the_scanner() {
        let (snapshot_tx, _snapshot_rx) = watch::channel(Arc::new(ProcessSnapshot::empty()));
        let (_request_tx, request_rx) = channel();

        let error = run_process_scanner(snapshot_tx, request_rx, Duration::ZERO, || {
            Err(std::io::Error::from(std::io::ErrorKind::NotFound))
        })
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn closed_snapshot_channel_forces_bounded_job_shutdown() {
        let temp = tempfile::tempdir().unwrap();
        let ready = temp.path().join("ready");
        let config: Config = serde_yaml::from_str(&format!(
            "jobs:\n  closed_scanner:\n    command: |\n      trap '' TERM\n      touch {}\n      while :; do sleep 1; done\n    shutdown_timeout: 30\n",
            ready.display()
        ))
        .unwrap();
        let (_, mut job_config) = config.jobs.into_iter().next().unwrap();
        job_config.cwd = temp.path().to_string_lossy().into_owned();
        let active_pids = Arc::new(Mutex::new(HashMap::new()));
        let active_pids_for_job = active_pids.clone();
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownState::Running);
        let initial_snapshot = Arc::new(ProcessSnapshot::capture().unwrap());
        let (snapshot_tx, snapshot_rx) = watch::channel(initial_snapshot);
        let base_env = Arc::new(env::vars().collect());
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();

        runtime.block_on(async move {
            let job = tokio::spawn(start_proc(
                "closed_scanner".to_string(),
                job_config,
                active_pids_for_job,
                shutdown_rx,
                snapshot_rx,
                base_env,
            ));
            timeout(Duration::from_secs(3), async {
                while !ready.exists() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("job did not start");

            shutdown_tx.send(ShutdownState::Graceful).unwrap();
            drop(snapshot_tx);
            timeout(Duration::from_secs(5), job)
                .await
                .expect("closed snapshot channel stalled job shutdown")
                .unwrap()
                .unwrap();
        });
        assert!(lock_or_recover(&active_pids).is_empty());
    }

    #[test]
    fn post_kill_timeout_is_fatal_for_any_validated_survivor() {
        assert!(post_kill_timeout_error(false, &HashSet::new()).is_none());
        assert_eq!(
            post_kill_timeout_error(true, &HashSet::new())
                .unwrap()
                .kind(),
            std::io::ErrorKind::TimedOut
        );

        let current = process_tree::identity(std::process::id()).unwrap();
        assert_eq!(
            post_kill_timeout_error(false, &HashSet::from([current]))
                .unwrap()
                .kind(),
            std::io::ErrorKind::TimedOut
        );
    }

    #[test]
    fn post_kill_timeout_is_fatal_when_survivor_inspection_fails() {
        let process = ProcessIdentity {
            pid: 123,
            process_group: 123,
            start_time: 456,
        };
        let error = post_kill_timeout_error_with(false, &HashSet::from([process]), |_| {
            Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
        })
        .unwrap();

        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("known descendant 123"));
    }
}
