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
const PROCESS_SCAN_INTERVAL: Duration = Duration::from_millis(250);
const PROCESS_CLEANUP_POLL_INTERVAL: Duration = Duration::from_millis(50);
const PROCESS_SCAN_FAILURE_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const PROCESS_SCAN_FAILURE_WINDOW: Duration = Duration::from_secs(1);
const PROCESS_SCAN_WARNING_CHANGE_FLOOR: Duration = Duration::from_secs(1);
const PROCESS_SCAN_WARNING_INTERVAL: Duration = Duration::from_secs(60);
const POST_KILL_WAIT: Duration = Duration::from_secs(2);
const RECONCILE_CONVERGENCE_RESERVE: Duration = Duration::from_millis(250);
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
    UseCleanupInterval,
    Stop,
}

struct ProcessScannerHandle {
    snapshots: watch::Receiver<Arc<ProcessSnapshot>>,
    requests: Sender<ProcessScannerRequest>,
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

fn strip_json_comments(data: &str) -> String {
    data.lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
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
    mut scanner: ProcessScannerHandle,
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
            &mut scanner,
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
    scanner: &mut ProcessScannerHandle,
    base_env: &Arc<HashMap<String, String>>,
) -> Result<bool, ProcessRunError> {
    if *shutdown.borrow() != ShutdownState::Running {
        return Ok(true);
    }
    let snapshots = &mut scanner.snapshots;
    let scanner_requests = &scanner.requests;

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
    // In-process tests execute through the libtest binary, which cannot enter this hidden mode.
    let use_job_shim = use_process_group && !cfg!(test);
    let mut cmd = if use_job_shim {
        let mut cmd = Command::new(env::current_exe()?);
        cmd.args([
            "--job-shim",
            "--shutdown-timeout",
            &opts.shutdown_timeout.to_string(),
        ]);
        if let Some(username) = &opts.sudo_user {
            cmd.args(["--sudo-user", username]);
            for key in ["CWD", "RUNNER_CWD"] {
                if let Some(value) = base_env.get(key) {
                    cmd.args(["--job-env", key, value]);
                }
            }
            if let Some(additional_env) = &opts.env {
                for (key, value) in additional_env {
                    cmd.args(["--job-env", key, value]);
                }
            }
        }
        cmd.arg("--").arg(&opts.command);
        cmd
    } else if let Some(username) = &opts.sudo_user {
        let mut cmd = Command::new("sudo");
        cmd.args(["-n", "-u", username, "--", "env", "--"]);
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

    if !use_job_shim {
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
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
            let snapshot_changed = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() {
                        break Some(ShutdownState::Force);
                    }
                    let state = *shutdown.borrow();
                    if state != ShutdownState::Running {
                        break Some(state);
                    }
                    continue;
                }
                snapshot_changed = snapshots.changed() => {
                    if snapshot_changed.is_err() {
                        break Some(ShutdownState::Force);
                    }
                    true
                }
                _ = sleep(PROCESS_CLEANUP_POLL_INTERVAL) => false,
            };
            if snapshot_changed && use_process_group {
                known = related_processes(snapshots, identity, &known);
                update_active_descendants(active_pids, info, &known);
            }
            match child.try_wait() {
                Ok(Some(status)) => {
                    remove_active_pid(active_pids, info);
                    report_child_status(status_prefix, status);
                    if use_process_group {
                        let snapshot = match refresh_process_snapshot(scanner_requests).await {
                            Ok(snapshot) => snapshot,
                            Err(refresh_error) => {
                                let cleanup_error =
                                    cleanup_after_natural_exit(status_prefix, opts, info, &known)
                                        .await
                                        .err();
                                drain_output_handlers([stdout_handle, stderr_handle]).await;
                                return Err(ProcessRunError::Fatal(
                                    cleanup_error.unwrap_or(refresh_error),
                                ));
                            }
                        };
                        known = snapshot.related_processes(identity, true, &known);
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
    let mut kill_attempted = HashSet::new();
    let mut signal_delivery_error = None;
    let mut consecutive_inspection_failures = 0;
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
            log_signal_error(status_prefix, libc::SIGTERM, &error);
            signal_delivery_error = Some(error.error);
        }
    } else {
        if let Err(error) =
            signal_run_with_retry(&signal_log_prefix, opts, info, &known, libc::SIGKILL).await
        {
            log_signal_error(status_prefix, libc::SIGKILL, &error);
            signal_delivery_error = Some(error.error);
        }
        let sent_at = Instant::now();
        kill_sent_at = Some(sent_at);
        kill_attempted.extend(known.iter().copied());
        post_kill_sleep.as_mut().reset(sent_at + POST_KILL_WAIT);
    }

    loop {
        if use_process_group {
            known = related_processes(snapshots, identity, &known);
            update_active_descendants(active_pids, info, &known);
        }
        if kill_sent_at.is_some() {
            let new_targets: HashSet<_> = known.difference(&kill_attempted).copied().collect();
            if !new_targets.is_empty() {
                kill_attempted.extend(new_targets.iter().copied());
                if let Err(error) = signal_run_with_retry(
                    &signal_log_prefix,
                    opts,
                    info,
                    &new_targets,
                    libc::SIGKILL,
                )
                .await
                {
                    log_signal_error(status_prefix, libc::SIGKILL, &error);
                    signal_delivery_error = Some(error.error);
                }
            }
        }
        let mut inspection_errors = SignalErrors::default();
        let observed_descendant = any_process_running_best_effort(&known, &mut inspection_errors);
        let descendants_running = observed_descendant || inspection_errors.error.is_some();
        if inspection_failure_requires_kill(
            &mut consecutive_inspection_failures,
            inspection_errors.error.is_some(),
            kill_sent_at.is_some(),
        ) {
            eprintln!(
                "{} > Could not verify process tree during shutdown, sending SIGKILL..",
                status_prefix
            );
            if let Err(error) =
                signal_run_with_retry(&signal_log_prefix, opts, info, &known, libc::SIGKILL).await
            {
                log_signal_error(status_prefix, libc::SIGKILL, &error);
                signal_delivery_error = Some(error.error);
            }
            let sent_at = Instant::now();
            kill_sent_at = Some(sent_at);
            kill_attempted.extend(known.iter().copied());
            post_kill_sleep.as_mut().reset(sent_at + POST_KILL_WAIT);
        }
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
                            log_signal_error(status_prefix, libc::SIGKILL, &error);
                            signal_delivery_error = Some(error.error);
                        }
                        let sent_at = Instant::now();
                        kill_sent_at = Some(sent_at);
                        kill_attempted.extend(known.iter().copied());
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
                    log_signal_error(status_prefix, libc::SIGKILL, &error);
                    signal_delivery_error = Some(error.error);
                }
                let sent_at = Instant::now();
                kill_sent_at = Some(sent_at);
                kill_attempted.extend(known.iter().copied());
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
                        log_signal_error(status_prefix, libc::SIGKILL, &error);
                        signal_delivery_error = Some(error.error);
                    }
                    let sent_at = Instant::now();
                    kill_sent_at = Some(sent_at);
                    kill_attempted.extend(known.iter().copied());
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
    if let Some(error) = timeout_error {
        return Err(ProcessRunError::Fatal(error));
    }
    let unresolved_signal_error = if let Some(error) = signal_delivery_error {
        let tree_is_running = process_tree::is_running_checked(info.identity)
            .and_then(|root| {
                any_process_running_checked(&known).map(|descendant| root || descendant)
            })
            .map_err(ProcessRunError::Fatal)?;
        tree_is_running.then_some(error)
    } else {
        None
    };
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

fn any_process_running_checked(processes: &HashSet<ProcessIdentity>) -> std::io::Result<bool> {
    any_process_running_checked_with(processes, process_tree::is_running_checked)
}

fn inspection_failure_requires_kill(
    consecutive_failures: &mut usize,
    inspection_failed: bool,
    kill_was_sent: bool,
) -> bool {
    *consecutive_failures = if inspection_failed {
        consecutive_failures.saturating_add(1)
    } else {
        0
    };
    *consecutive_failures >= 2 && !kill_was_sent
}

fn any_process_running_checked_with<P>(
    processes: &HashSet<ProcessIdentity>,
    mut probe: P,
) -> std::io::Result<bool>
where
    P: FnMut(ProcessIdentity) -> std::io::Result<bool>,
{
    for process in processes {
        if probe(*process)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn any_process_running_best_effort(
    processes: &HashSet<ProcessIdentity>,
    errors: &mut SignalErrors,
) -> bool {
    any_process_running_best_effort_with(processes, errors, process_tree::is_running_checked)
}

fn any_process_running_best_effort_with<P>(
    processes: &HashSet<ProcessIdentity>,
    errors: &mut SignalErrors,
    mut probe: P,
) -> bool
where
    P: FnMut(ProcessIdentity) -> std::io::Result<bool>,
{
    let mut any_running = false;
    for process in processes {
        if process_is_running_best_effort_with(*process, errors, &mut probe) {
            any_running = true;
        }
    }
    any_running
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

    let signal_prefix = status_prefix.to_string();
    cleanup_known_processes_with(
        known,
        process_tree::is_running_checked,
        |targets| async move {
            eprintln!(
                "{} > Cleaning up processes left behind by exited job",
                status_prefix
            );
            signal_run_with_retry(&signal_prefix, opts, info, &targets, libc::SIGKILL)
                .await
                .map_err(|error| error.error)
        },
    )
    .await
}

async fn cleanup_known_processes_with<P, S, F>(
    known: &HashSet<ProcessIdentity>,
    mut probe: P,
    signal: S,
) -> std::io::Result<()>
where
    P: FnMut(ProcessIdentity) -> std::io::Result<bool>,
    S: FnOnce(HashSet<ProcessIdentity>) -> F,
    F: std::future::Future<Output = std::io::Result<()>>,
{
    let mut initial_errors = SignalErrors::default();
    let any_running = any_process_running_best_effort_with(known, &mut initial_errors, &mut probe);
    if !any_running && initial_errors.error.is_none() {
        return Ok(());
    }

    let result = signal(known.clone()).await;
    let mut validation_errors = SignalErrors::default();
    let survivors = any_process_running_best_effort_with(known, &mut validation_errors, &mut probe);
    if let Some(error) = validation_errors.error {
        return Err(error);
    }
    if let Err(error) = result
        && survivors
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

const JOB_SHIM_SIGNALS: [libc::c_int; 6] = [
    libc::SIGTERM,
    libc::SIGHUP,
    libc::SIGINT,
    libc::SIGQUIT,
    libc::SIGUSR1,
    libc::SIGUSR2,
];
static JOB_SHIM_TERMINATION_RECEIVED: AtomicU8 = AtomicU8::new(0);

extern "C" fn job_shim_signal_handler(signal: libc::c_int) {
    if matches!(
        signal,
        libc::SIGTERM | libc::SIGHUP | libc::SIGINT | libc::SIGQUIT
    ) {
        JOB_SHIM_TERMINATION_RECEIVED.store(1, Ordering::Relaxed);
    }
}

fn run_job_shim(args: &[String]) -> ExitCode {
    use std::os::unix::process::ExitStatusExt;

    let mut sudo_user = None;
    let mut shutdown_timeout = None;
    let mut job_env = Vec::new();
    let mut command = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--sudo-user" if index + 1 < args.len() => {
                sudo_user = Some(args[index + 1].clone());
                index += 2;
            }
            "--shutdown-timeout" if index + 1 < args.len() => {
                shutdown_timeout = args[index + 1].parse::<u64>().ok();
                index += 2;
            }
            "--job-env" if index + 2 < args.len() => {
                job_env.push((args[index + 1].clone(), args[index + 2].clone()));
                index += 3;
            }
            "--" if index + 2 == args.len() => {
                command = Some(args[index + 1].clone());
                break;
            }
            argument => {
                std::eprintln!("Invalid job shim argument: {argument}");
                return ExitCode::from(1);
            }
        }
    }
    let Some(command_text) = command else {
        std::eprintln!("Job shim command is missing");
        return ExitCode::from(1);
    };
    let Some(shutdown_timeout) = shutdown_timeout else {
        std::eprintln!("Job shim shutdown timeout is missing or invalid");
        return ExitCode::from(1);
    };

    if unsafe { libc::setsid() } == -1 {
        std::eprintln!(
            "Job shim could not create a session: {}",
            std::io::Error::last_os_error()
        );
        return ExitCode::from(1);
    }
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } == -1 {
        std::eprintln!(
            "Job shim could not become a child subreaper: {}",
            std::io::Error::last_os_error()
        );
        return ExitCode::from(1);
    }
    for signal in JOB_SHIM_SIGNALS {
        if unsafe {
            libc::signal(
                signal,
                job_shim_signal_handler as *const () as libc::sighandler_t,
            )
        } == libc::SIG_ERR
        {
            std::eprintln!(
                "Job shim could not install a signal handler: {}",
                std::io::Error::last_os_error()
            );
            return ExitCode::from(1);
        }
    }
    let identity = match process_tree::identity(std::process::id()) {
        Ok(identity) => identity,
        Err(error) => {
            std::eprintln!("Job shim could not inspect itself: {error}");
            return ExitCode::from(1);
        }
    };

    let mut child = if let Some(username) = &sudo_user {
        let mut child = std::process::Command::new("sudo");
        child.args(["-n", "-u", username, "--", "env", "--"]);
        for (key, value) in &job_env {
            child.arg(format!("{key}={value}"));
        }
        child.args(["sh", "-c", &command_text]);
        child
    } else {
        let mut child = std::process::Command::new("sh");
        child.args(["-c", &command_text]);
        child
    };
    let child = match child.spawn() {
        Ok(child) => child,
        Err(error) => {
            std::eprintln!("Job shim could not start command: {error}");
            return ExitCode::from(1);
        }
    };
    let status = match wait_for_job_shim_main_child(&child) {
        Ok(status) => status,
        Err(error) => {
            std::eprintln!("Job shim could not wait for command: {error}");
            return ExitCode::from(1);
        }
    };
    let termination_observed = JOB_SHIM_TERMINATION_RECEIVED.load(Ordering::Relaxed) != 0
        || status.signal().is_some_and(|signal| {
            matches!(
                signal,
                libc::SIGTERM | libc::SIGHUP | libc::SIGINT | libc::SIGQUIT
            )
        });
    let cleanup = if termination_observed {
        match wait_for_descendants(identity, Duration::from_secs(shutdown_timeout)) {
            Ok(true) => Ok(()),
            Ok(false) => reconcile_descendants(identity, sudo_user.as_deref(), "Job shim"),
            Err(error) => Err(error),
        }
    } else {
        reconcile_descendants(identity, sudo_user.as_deref(), "Job shim")
    };
    if let Err(error) = cleanup {
        std::eprintln!("Job shim could not supervise descendants: {error}");
        return ExitCode::from(1);
    }

    if let Some(code) = status.code() {
        return ExitCode::from(code as u8);
    }
    if let Some(signal) = status.signal() {
        unsafe {
            libc::signal(signal, libc::SIG_DFL);
            libc::raise(signal);
        }
        return ExitCode::from((128 + signal).min(255) as u8);
    }
    ExitCode::from(1)
}

fn wait_for_job_shim_main_child(
    child: &std::process::Child,
) -> std::io::Result<std::process::ExitStatus> {
    use std::os::unix::process::ExitStatusExt;

    let main_pid = i32::try_from(child.id())
        .map_err(|_| std::io::Error::other("process ID does not fit wait target"))?;
    loop {
        let mut raw_status = 0;
        let waited = unsafe { libc::waitpid(-1, &mut raw_status, 0) };
        if waited == main_pid {
            return Ok(std::process::ExitStatus::from_raw(raw_status));
        }
        if waited > 0 {
            continue;
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EINTR) {
            return Err(error);
        }
    }
}

fn wait_for_descendants(root: ProcessIdentity, duration: Duration) -> std::io::Result<bool> {
    let deadline = std::time::Instant::now() + duration;
    let mut known = HashSet::new();
    let mut last_capture_failure_log = None;
    loop {
        let capture_failure_window = PROCESS_SCAN_FAILURE_WINDOW.min(
            deadline
                .saturating_duration_since(std::time::Instant::now())
                .saturating_sub(PROCESS_CLEANUP_POLL_INTERVAL),
        );
        let snapshot = capture_snapshot_with_retry(
            &mut ProcessSnapshot::capture,
            PROCESS_SCAN_FAILURE_RETRY_INTERVAL,
            capture_failure_window,
            &mut last_capture_failure_log,
        )?;
        known = snapshot.related_processes(root, false, &known);
        let mut any_running = false;
        for process in &known {
            if process_tree::is_running_checked(*process)? {
                any_running = true;
            }
        }
        reap_adopted_children()?;
        if !any_running {
            return Ok(true);
        }
        if std::time::Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(PROCESS_CLEANUP_POLL_INTERVAL);
    }
}

fn main() -> ExitCode {
    if unsafe { libc::getpid() } == 1 {
        std::eprintln!(
            "Refusing to run as PID 1; use an external init such as `tini -- job-runner-rs ...`"
        );
        return ExitCode::from(1);
    }

    let args = env::args().collect::<Vec<_>>();
    if args.get(1).is_some_and(|argument| argument == "--job-shim") {
        return run_job_shim(&args[2..]);
    }
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
    process_tree::validate_procfs().map_err(|error| {
        std::io::Error::new(
            error.kind(),
            format!("could not validate Linux procfs: {error}"),
        )
    })?;
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
        let job_scanner = ProcessScannerHandle {
            snapshots: snapshot_rx.clone(),
            requests: scanner_request_tx.clone(),
        };
        let job_env = base_env.clone();
        job_handles.spawn(async move {
            start_proc(
                prefix,
                job_config,
                pid_set,
                job_shutdown,
                job_scanner,
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
    let _ = scanner_request_tx.send(ProcessScannerRequest::UseCleanupInterval);
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
    initial_scan_interval: Duration,
    mut capture: C,
) -> std::io::Result<()>
where
    C: FnMut() -> std::io::Result<ProcessSnapshot>,
{
    let mut last_anomaly_log = None;
    let mut last_capture_failure_log = None;
    let mut scan_interval = initial_scan_interval;
    loop {
        match scanner_request_rx.recv_timeout(scan_interval) {
            Ok(ProcessScannerRequest::Refresh(acknowledge)) => {
                let mut acknowledgements = vec![acknowledge];
                let mut stop_requested = false;
                while let Ok(request) = scanner_request_rx.try_recv() {
                    match request {
                        ProcessScannerRequest::Refresh(acknowledge) => {
                            acknowledgements.push(acknowledge);
                        }
                        ProcessScannerRequest::UseCleanupInterval => {
                            scan_interval = PROCESS_CLEANUP_POLL_INTERVAL;
                        }
                        ProcessScannerRequest::Stop => stop_requested = true,
                    }
                }
                match capture_snapshot_with_retry(
                    &mut capture,
                    PROCESS_SCAN_FAILURE_RETRY_INTERVAL,
                    PROCESS_SCAN_FAILURE_WINDOW,
                    &mut last_capture_failure_log,
                ) {
                    Ok(snapshot) => {
                        log_process_scan_anomalies(&snapshot, &mut last_anomaly_log);
                        let snapshot = Arc::new(snapshot);
                        let published = snapshot_tx.send(snapshot.clone()).is_ok();
                        for acknowledge in acknowledgements {
                            let _ = acknowledge.send(Ok(snapshot.clone()));
                        }
                        if !published || stop_requested {
                            return Ok(());
                        }
                    }
                    Err(error) => {
                        let kind = error.kind();
                        let message = error.to_string();
                        let scanner_error = std::io::Error::new(kind, message.clone());
                        for acknowledge in acknowledgements {
                            let _ =
                                acknowledge.send(Err(std::io::Error::new(kind, message.clone())));
                        }
                        return Err(scanner_error);
                    }
                }
            }
            Ok(ProcessScannerRequest::UseCleanupInterval) => {
                scan_interval = PROCESS_CLEANUP_POLL_INTERVAL;
            }
            Ok(ProcessScannerRequest::Stop) | Err(RecvTimeoutError::Disconnected) => return Ok(()),
            Err(RecvTimeoutError::Timeout) => {
                let snapshot = capture_snapshot_with_retry(
                    &mut capture,
                    PROCESS_SCAN_FAILURE_RETRY_INTERVAL,
                    PROCESS_SCAN_FAILURE_WINDOW,
                    &mut last_capture_failure_log,
                )?;
                log_process_scan_anomalies(&snapshot, &mut last_anomaly_log);
                if snapshot_tx.send(Arc::new(snapshot)).is_err() {
                    return Ok(());
                }
            }
        }
    }
}

fn capture_snapshot_with_retry<C>(
    capture: &mut C,
    retry_interval: Duration,
    failure_window: Duration,
    last_log: &mut Option<std::time::Instant>,
) -> std::io::Result<ProcessSnapshot>
where
    C: FnMut() -> std::io::Result<ProcessSnapshot>,
{
    let failed_at = std::time::Instant::now();
    loop {
        match capture() {
            Ok(snapshot) => return Ok(snapshot),
            Err(error) => {
                let now = std::time::Instant::now();
                if last_log
                    .is_none_or(|last| now.duration_since(last) >= PROCESS_SCAN_WARNING_INTERVAL)
                {
                    eprintln!("Process scanner warning: capture failed, retrying: {error}");
                    *last_log = Some(now);
                }
                let elapsed = failed_at.elapsed();
                if elapsed >= failure_window {
                    return Err(error);
                }
                std::thread::sleep(retry_interval.min(failure_window - elapsed));
            }
        }
    }
}

fn log_process_scan_anomalies(
    snapshot: &ProcessSnapshot,
    last_log: &mut Option<(Instant, HashSet<std::io::ErrorKind>)>,
) {
    let kinds = snapshot.anomaly_kinds();
    if kinds.is_empty() {
        return;
    }
    let should_log = last_log.as_ref().is_none_or(|(last, previous_kinds)| {
        last.elapsed() >= PROCESS_SCAN_WARNING_INTERVAL
            || (kinds != previous_kinds && last.elapsed() >= PROCESS_SCAN_WARNING_CHANGE_FLOOR)
    });
    if should_log && let Some(summary) = snapshot.anomaly_summary() {
        eprintln!("Process scanner warning: {summary}");
        *last_log = Some((Instant::now(), kinds.clone()));
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
    let mut delivery_error = SignalErrors::default();
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
        if let Err(error) = signal_process_tree(
            "Signal forwarding",
            (!sudo_user.is_empty()).then_some(sudo_user.as_str()),
            tracked.info,
            &descendants,
            signal,
        )
        .await
        {
            delivery_error.record_signal(error);
        }
    }
    delivery_error.finish().map_err(|error| error.error)
}

async fn signal_run(
    log_prefix: &str,
    opts: &JobConfig,
    info: PidInfo,
    descendants: &HashSet<ProcessIdentity>,
    signal: libc::c_int,
) -> Result<(), SignalError> {
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
) -> Result<(), SignalError> {
    let mut last_error = None;
    for attempt in 0..SIGNAL_RETRY_LIMIT {
        match signal_run(log_prefix, opts, info, descendants, signal).await {
            Ok(()) => return Ok(()),
            Err(error) if error.delivery_failed => last_error = Some(error),
            Err(error) => return Err(error),
        }
        if attempt + 1 < SIGNAL_RETRY_LIMIT {
            sleep(SIGNAL_RETRY_INTERVAL).await;
        }
    }
    Err(last_error
        .unwrap_or_else(|| SignalError::delivery(std::io::Error::other("signal delivery failed"))))
}

fn log_signal_error(status_prefix: &PrefixWriter<'_>, signal: libc::c_int, error: &SignalError) {
    let operation = if error.delivery_failed {
        "deliver"
    } else {
        "verify targets for"
    };
    eprintln!(
        "{} > Could not {} SIG{}: {}",
        status_prefix,
        operation,
        signal_number_to_name(signal),
        error.error
    );
}

async fn signal_process_tree(
    log_prefix: &str,
    sudo_user: Option<&str>,
    info: PidInfo,
    descendants: &HashSet<ProcessIdentity>,
    signal: libc::c_int,
) -> Result<(), SignalError> {
    let root = info.identity;
    let mut delivery_error = SignalErrors::default();
    let root_exists = process_exists_best_effort(root, &mut delivery_error);
    if let Some(username) = sudo_user {
        if root_exists
            && let Ok(pid) = i32::try_from(root.pid)
            && let Err(error) = send_signal_target(pid, signal)
            && error.raw_os_error() != Some(libc::ESRCH)
        {
            delivery_error.record_delivery(error);
        }

        let targets: HashSet<ProcessIdentity> = descendants
            .iter()
            .copied()
            .filter(|process| process_is_running_best_effort(*process, &mut delivery_error))
            .collect();
        if !targets.is_empty()
            && let Err(error) =
                signal_identities_as_user(log_prefix, username, signal, &targets).await
        {
            delivery_error.record_signal(error);
        }
        return delivery_error.finish();
    }

    let mut group_is_owned = root_exists;
    if !group_is_owned {
        for process in descendants {
            if process.process_group == root.pid
                && process_is_running_best_effort(*process, &mut delivery_error)
            {
                group_is_owned = true;
                break;
            }
        }
    }
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
            if process.process_group == root.pid
                || !process_is_running_best_effort(*process, &mut delivery_error)
            {
                continue;
            }
            let Ok(pid) = i32::try_from(process.pid) else {
                continue;
            };
            targets.push(pid);
        }
    }

    if targets.is_empty() {
        return delivery_error.finish();
    }
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
            delivery_error.record_delivery(error);
        }
    }
    delivery_error.finish()
}

struct SignalError {
    error: std::io::Error,
    delivery_failed: bool,
}

impl SignalError {
    fn delivery(error: std::io::Error) -> Self {
        Self {
            error,
            delivery_failed: true,
        }
    }
}

#[derive(Default)]
struct SignalErrors {
    error: Option<std::io::Error>,
    delivery_failed: bool,
}

impl SignalErrors {
    fn record_inspection(&mut self, error: std::io::Error) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }

    fn record_delivery(&mut self, error: std::io::Error) {
        if !self.delivery_failed {
            self.error = Some(error);
        }
        self.delivery_failed = true;
    }

    fn record_signal(&mut self, error: SignalError) {
        if error.delivery_failed {
            self.record_delivery(error.error);
        } else {
            self.record_inspection(error.error);
        }
    }

    fn finish(self) -> Result<(), SignalError> {
        self.error.map_or(Ok(()), |error| {
            Err(SignalError {
                error,
                delivery_failed: self.delivery_failed,
            })
        })
    }
}

fn process_exists_best_effort(
    process: ProcessIdentity,
    inspection_error: &mut SignalErrors,
) -> bool {
    match process_tree::exists_checked(process) {
        Ok(exists) => exists,
        Err(error) => {
            inspection_error.record_inspection(error);
            false
        }
    }
}

fn process_is_running_best_effort(
    process: ProcessIdentity,
    inspection_error: &mut SignalErrors,
) -> bool {
    process_is_running_best_effort_with(process, inspection_error, process_tree::is_running_checked)
}

fn process_is_running_best_effort_with<P>(
    process: ProcessIdentity,
    inspection_error: &mut SignalErrors,
    mut probe: P,
) -> bool
where
    P: FnMut(ProcessIdentity) -> std::io::Result<bool>,
{
    match probe(process) {
        Ok(running) => running,
        Err(error) => {
            inspection_error.record_inspection(error);
            false
        }
    }
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

async fn signal_identities_as_user(
    log_prefix: &str,
    username: &str,
    signal: libc::c_int,
    processes: &HashSet<ProcessIdentity>,
) -> Result<(), SignalError> {
    let mut errors = SignalErrors::default();
    let mut targets = Vec::with_capacity(processes.len());
    for process in processes {
        match process_tree::is_running_checked(*process) {
            Ok(true) => match i32::try_from(process.pid) {
                Ok(pid) => targets.push(pid),
                Err(_) => errors.record_delivery(std::io::Error::other(
                    "process ID does not fit signal target",
                )),
            },
            Ok(false) => {}
            Err(error) => errors.record_inspection(error),
        }
    }
    if !targets.is_empty()
        && let Err(error) = signal_as_user(log_prefix, username, signal, &targets).await
    {
        errors.record_delivery(error);
    }
    errors.finish()
}

fn run_wrapper_mode(self_path: &str, inner_args: &[String]) -> ExitCode {
    use std::os::unix::process::CommandExt;

    if let Err(error) = process_tree::validate_procfs() {
        eprintln!("[Wrapper] Could not validate Linux procfs: {error}");
        return ExitCode::from(1);
    }
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } == -1 {
        eprintln!(
            "[Wrapper] Could not become a child subreaper: {}",
            std::io::Error::last_os_error()
        );
        return ExitCode::from(1);
    }
    let wrapper_identity = match process_tree::identity(std::process::id()) {
        Ok(identity) => identity,
        Err(error) => {
            eprintln!("[Wrapper] Could not inspect wrapper process: {error}");
            return ExitCode::from(1);
        }
    };

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

        if let Err(error) = reconcile_descendants_with_sudo_users(
            wrapper_identity,
            configured_wrapper_sudo_users(inner_args),
            "Wrapper",
        ) {
            eprintln!("[Wrapper] Could not clean up previous-generation jobs: {error}");
            return ExitCode::from(1);
        }

        if shutdown_state.load(Ordering::Acquire) != 0 {
            eprintln!("[Wrapper] Shutdown complete, exiting..");
            return ExitCode::SUCCESS;
        }

        eprintln!("[Wrapper] Inner process exited, waiting 15 seconds before restarting..");
        wrapper_delay(&shutdown_state, Duration::from_secs(15));
    }
}

fn reconcile_descendants(
    root: ProcessIdentity,
    sudo_user: Option<&str>,
    label: &str,
) -> std::io::Result<()> {
    reconcile_descendants_with_sudo_users(
        root,
        sudo_user.into_iter().map(str::to_owned).collect(),
        label,
    )
}

fn reconcile_descendants_with_sudo_users(
    root: ProcessIdentity,
    mut sudo_users: HashSet<String>,
    label: &str,
) -> std::io::Result<()> {
    let deadline = std::time::Instant::now() + POST_KILL_WAIT;
    let mut known = HashSet::new();
    let mut cleanup_logged = false;
    let mut last_capture_failure_log = None;
    let mut last_kill_error = None;
    loop {
        let capture_failure_window = PROCESS_SCAN_FAILURE_WINDOW.min(
            deadline
                .saturating_duration_since(std::time::Instant::now())
                .saturating_sub(RECONCILE_CONVERGENCE_RESERVE),
        );
        let snapshot = capture_snapshot_with_retry(
            &mut ProcessSnapshot::capture,
            PROCESS_SCAN_FAILURE_RETRY_INTERVAL,
            capture_failure_window,
            &mut last_capture_failure_log,
        )?;
        known = snapshot.related_processes(root, false, &known);
        let mut running = Vec::new();
        for process in &known {
            if process_tree::is_running_checked(*process)? {
                running.push(*process);
            }
        }
        if running.is_empty() {
            reap_adopted_children()?;
            return Ok(());
        }
        if !cleanup_logged {
            std::eprintln!(
                "[{label}] Cleaning up {} descendant process{}..",
                running.len(),
                if running.len() == 1 { "" } else { "es" }
            );
            cleanup_logged = true;
        }
        discover_job_shim_sudo_users(&running, &mut sudo_users);
        if let Err(error) = kill_reconciled_processes(&running, &sudo_users, deadline) {
            last_kill_error = Some(error);
        }
        reap_adopted_children()?;
        if std::time::Instant::now() >= deadline {
            let mut survivors = Vec::new();
            for process in running {
                if process_tree::is_running_checked(process)? {
                    survivors.push(process);
                }
            }
            if survivors.is_empty() {
                reap_adopted_children()?;
                return Ok(());
            }
            let survivor_pids = survivors
                .iter()
                .map(|process| process.pid.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let detail = last_kill_error
                .as_ref()
                .map(|error| format!("; last signal error: {error}"))
                .unwrap_or_default();
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "previous-generation processes remained alive after SIGKILL: {survivor_pids}{detail}"
                ),
            ));
        }
        std::thread::sleep(PROCESS_CLEANUP_POLL_INTERVAL);
    }
}

fn configured_wrapper_sudo_users(inner_args: &[String]) -> HashSet<String> {
    let mut config_path = None;
    let mut skip_next = false;
    for argument in inner_args {
        if skip_next {
            skip_next = false;
        } else if argument == "-t" {
            skip_next = true;
        } else if !argument.starts_with('-') {
            config_path = Some(argument);
        }
    }
    let Some(config_path) = config_path else {
        return HashSet::new();
    };
    let Ok(data) = std::fs::read_to_string(config_path) else {
        return HashSet::new();
    };
    let without_json_comments = strip_json_comments(&data);
    let config = serde_yaml::from_str::<Config>(&data)
        .ok()
        .or_else(|| serde_json::from_str::<Config>(&without_json_comments).ok());
    config
        .into_iter()
        .flat_map(|config| config.jobs.into_values())
        .filter_map(|job| job.sudo_user)
        .filter(|username| is_valid_username(username))
        .collect()
}

fn kill_reconciled_processes(
    processes: &[ProcessIdentity],
    sudo_users: &HashSet<String>,
    deadline: std::time::Instant,
) -> std::io::Result<()> {
    let mut targets = Vec::with_capacity(processes.len());
    for process in processes {
        if process_tree::is_running_checked(*process)? {
            targets.push(
                i32::try_from(process.pid)
                    .map_err(|_| std::io::Error::other("process ID does not fit signal target"))?,
            );
        }
    }
    if targets.is_empty() {
        return Ok(());
    }
    let mut first_error = None;
    for (index, username) in sudo_users.iter().enumerate() {
        let helpers_left = u32::try_from(sudo_users.len() - index).unwrap_or(u32::MAX);
        let remaining = deadline
            .saturating_duration_since(std::time::Instant::now())
            .saturating_sub(RECONCILE_CONVERGENCE_RESERVE);
        let helper_timeout = (remaining / helpers_left).min(SUDO_SIGNAL_TIMEOUT);
        if let Err(error) = run_sudo_reconcile_helper(username, &targets, helper_timeout)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    for target in targets {
        if let Err(error) = send_signal_target(target, libc::SIGKILL)
            && error.raw_os_error() != Some(libc::ESRCH)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn discover_job_shim_sudo_users(processes: &[ProcessIdentity], sudo_users: &mut HashSet<String>) {
    let Ok(current_executable) = std::fs::read_link("/proc/self/exe") else {
        return;
    };
    for process in processes {
        let Ok(executable) = std::fs::read_link(format!("/proc/{}/exe", process.pid)) else {
            continue;
        };
        if executable != current_executable {
            continue;
        }
        let Ok(command_line) = std::fs::read(format!("/proc/{}/cmdline", process.pid)) else {
            continue;
        };
        if !process_tree::exists_checked(*process).unwrap_or(false) {
            continue;
        }
        let arguments: Vec<&[u8]> = command_line
            .split(|byte| *byte == 0)
            .filter(|argument| !argument.is_empty())
            .collect();
        if !arguments.iter().any(|argument| *argument == b"--job-shim") {
            continue;
        }
        for pair in arguments.windows(2) {
            if pair[0] != b"--sudo-user" {
                continue;
            }
            let Ok(username) = std::str::from_utf8(pair[1]) else {
                continue;
            };
            if is_valid_username(username) {
                sudo_users.insert(username.to_string());
            }
        }
    }
}

fn run_sudo_reconcile_helper(
    username: &str,
    targets: &[i32],
    duration: Duration,
) -> std::io::Result<()> {
    let mut child = std::process::Command::new("sudo")
        .args([
            "-n",
            "-u",
            username,
            "--",
            "sh",
            "-c",
            "failed=0; for target do kill -KILL \"$target\" || failed=1; done; exit \"$failed\"",
            "job-runner-reconcile",
        ])
        .args(targets.iter().map(i32::to_string))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let deadline = std::time::Instant::now() + duration;
    loop {
        if let Some(status) = child.try_wait()? {
            return if status.success() {
                Ok(())
            } else {
                Err(std::io::Error::other(format!(
                    "sudo cleanup helper exited with {status}"
                )))
            };
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "sudo cleanup helper timed out",
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn reap_adopted_children() -> std::io::Result<()> {
    loop {
        let waited = unsafe { libc::waitpid(-1, std::ptr::null_mut(), libc::WNOHANG) };
        if waited > 0 {
            continue;
        }
        if waited == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::ECHILD) => return Ok(()),
            Some(libc::EINTR) => {}
            _ => return Err(error),
        }
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
        let _ = reap_adopted_children();
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
    fn scanner_refresh_recovers_from_a_transient_capture_failure() {
        let (snapshot_tx, _snapshot_rx) = watch::channel(Arc::new(ProcessSnapshot::empty()));
        let (request_tx, request_rx) = channel();
        let scanner = std::thread::spawn(move || {
            let mut attempts = 0;
            run_process_scanner(snapshot_tx, request_rx, Duration::from_secs(60), || {
                attempts += 1;
                if attempts == 1 {
                    Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
                } else {
                    Ok(ProcessSnapshot::empty())
                }
            })
        });
        let runtime = Builder::new_current_thread().enable_time().build().unwrap();

        runtime
            .block_on(async {
                timeout(
                    Duration::from_secs(1),
                    refresh_process_snapshot(&request_tx),
                )
                .await
                .expect("explicit refresh did not complete before the timeout")
            })
            .unwrap();
        request_tx.send(ProcessScannerRequest::Stop).unwrap();
        scanner.join().unwrap().unwrap();
    }

    #[test]
    fn background_process_scanner_uses_the_normal_interval() {
        let (snapshot_tx, _snapshot_rx) = watch::channel(Arc::new(ProcessSnapshot::empty()));
        let (request_tx, request_rx) = channel();
        let (ready_tx, ready_rx) = sync_channel(1);
        let (capture_tx, capture_rx) = channel();
        let scanner = std::thread::spawn(move || {
            ready_tx.send(()).unwrap();
            run_process_scanner(snapshot_tx, request_rx, PROCESS_SCAN_INTERVAL, || {
                let _ = capture_tx.send(());
                Ok(ProcessSnapshot::empty())
            })
        });

        ready_rx.recv().unwrap();
        let early_capture = capture_rx.recv_timeout(PROCESS_CLEANUP_POLL_INTERVAL * 2);
        let eventually_captured =
            early_capture.is_ok() || capture_rx.recv_timeout(PROCESS_SCAN_INTERVAL * 4).is_ok();
        request_tx.send(ProcessScannerRequest::Stop).unwrap();
        scanner.join().unwrap().unwrap();

        assert!(matches!(early_capture, Err(RecvTimeoutError::Timeout)));
        assert!(eventually_captured, "background scanner did not run");
    }

    #[test]
    fn scanner_switches_to_cleanup_cadence_on_request() {
        let (snapshot_tx, _snapshot_rx) = watch::channel(Arc::new(ProcessSnapshot::empty()));
        let (request_tx, request_rx) = channel();
        let (capture_tx, capture_rx) = channel();
        let scanner = std::thread::spawn(move || {
            run_process_scanner(snapshot_tx, request_rx, Duration::from_secs(60), || {
                let _ = capture_tx.send(());
                Ok(ProcessSnapshot::empty())
            })
        });

        request_tx
            .send(ProcessScannerRequest::UseCleanupInterval)
            .unwrap();
        capture_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("scanner did not adopt the cleanup cadence");
        request_tx.send(ProcessScannerRequest::Stop).unwrap();
        scanner.join().unwrap().unwrap();
    }

    #[test]
    fn queued_scanner_refreshes_share_one_capture_before_stop() {
        use std::sync::atomic::AtomicUsize;

        let captures = Arc::new(AtomicUsize::new(0));
        let captures_for_scanner = captures.clone();
        let (snapshot_tx, _snapshot_rx) = watch::channel(Arc::new(ProcessSnapshot::empty()));
        let (request_tx, request_rx) = channel();
        let mut receivers = Vec::new();
        for index in 0..8 {
            if index == 4 {
                request_tx.send(ProcessScannerRequest::Stop).unwrap();
            }
            let (acknowledge, receiver) = oneshot::channel();
            request_tx
                .send(ProcessScannerRequest::Refresh(acknowledge))
                .unwrap();
            receivers.push(receiver);
        }
        let scanner = std::thread::spawn(move || {
            run_process_scanner(snapshot_tx, request_rx, Duration::from_secs(60), || {
                captures_for_scanner.fetch_add(1, Ordering::Relaxed);
                Ok(ProcessSnapshot::empty())
            })
        });

        let runtime = Builder::new_current_thread().enable_time().build().unwrap();
        runtime.block_on(async {
            for receiver in receivers {
                timeout(Duration::from_secs(1), receiver)
                    .await
                    .expect("coalesced refresh was not acknowledged")
                    .unwrap()
                    .unwrap();
            }
        });
        scanner.join().unwrap().unwrap();
        assert_eq!(captures.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn scanner_refresh_never_uses_a_capture_started_before_the_request() {
        use std::sync::atomic::AtomicUsize;

        let captures = Arc::new(AtomicUsize::new(0));
        let captures_for_scanner = captures.clone();
        let (snapshot_tx, _snapshot_rx) = watch::channel(Arc::new(ProcessSnapshot::empty()));
        let (request_tx, request_rx) = channel();
        let (capture_started_tx, capture_started_rx) = sync_channel(1);
        let (release_capture_tx, release_capture_rx) = sync_channel(1);
        let scanner = std::thread::spawn(move || {
            run_process_scanner(snapshot_tx, request_rx, Duration::from_secs(60), || {
                let capture_index = captures_for_scanner.fetch_add(1, Ordering::Relaxed);
                let snapshot = ProcessSnapshot::empty();
                if capture_index == 0 {
                    capture_started_tx.send(()).unwrap();
                    release_capture_rx.recv().unwrap();
                }
                Ok(snapshot)
            })
        });

        let (first_acknowledge, first_receiver) = oneshot::channel();
        request_tx
            .send(ProcessScannerRequest::Refresh(first_acknowledge))
            .unwrap();
        capture_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("scanner did not start the first capture");
        let requested_at = std::time::Instant::now();
        let (second_acknowledge, second_receiver) = oneshot::channel();
        request_tx
            .send(ProcessScannerRequest::Refresh(second_acknowledge))
            .unwrap();
        release_capture_tx.send(()).unwrap();

        let runtime = Builder::new_current_thread().enable_time().build().unwrap();
        let second_snapshot = runtime.block_on(async {
            timeout(Duration::from_secs(1), first_receiver)
                .await
                .expect("first refresh was not acknowledged")
                .unwrap()
                .unwrap();
            timeout(Duration::from_secs(1), second_receiver)
                .await
                .expect("second refresh was not acknowledged")
                .unwrap()
                .unwrap()
        });
        request_tx.send(ProcessScannerRequest::Stop).unwrap();
        scanner.join().unwrap().unwrap();

        assert!(second_snapshot.started_after(requested_at));
        assert_eq!(captures.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn scanner_applies_cleanup_interval_queued_with_a_refresh() {
        let (snapshot_tx, _snapshot_rx) = watch::channel(Arc::new(ProcessSnapshot::empty()));
        let (request_tx, request_rx) = channel();
        let (capture_tx, capture_rx) = channel();
        let (acknowledge, receiver) = oneshot::channel();
        request_tx
            .send(ProcessScannerRequest::Refresh(acknowledge))
            .unwrap();
        request_tx
            .send(ProcessScannerRequest::UseCleanupInterval)
            .unwrap();
        let scanner = std::thread::spawn(move || {
            run_process_scanner(snapshot_tx, request_rx, Duration::from_secs(60), || {
                let _ = capture_tx.send(());
                Ok(ProcessSnapshot::empty())
            })
        });

        let runtime = Builder::new_current_thread().enable_time().build().unwrap();
        runtime.block_on(async {
            timeout(Duration::from_secs(1), receiver)
                .await
                .expect("refresh was not acknowledged")
                .unwrap()
                .unwrap();
        });
        capture_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("queued refresh did not capture a snapshot");
        capture_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("queued cleanup interval was not applied");
        request_tx.send(ProcessScannerRequest::Stop).unwrap();
        scanner.join().unwrap().unwrap();
    }

    #[test]
    fn natural_exit_requests_a_fresh_snapshot_before_cleanup() {
        use std::sync::atomic::AtomicUsize;

        let temp = tempfile::tempdir().unwrap();
        let config: Config = serde_yaml::from_str(
            "jobs:\n  refresh_cleanup:\n    command: sleep 0.2\n    restart_delay: 10000\n",
        )
        .unwrap();
        let (_, mut job_config) = config.jobs.into_iter().next().unwrap();
        job_config.cwd = temp.path().to_string_lossy().into_owned();

        let captures = Arc::new(AtomicUsize::new(0));
        let captures_for_scanner = captures.clone();
        let (snapshot_tx, snapshot_rx) = watch::channel(Arc::new(ProcessSnapshot::empty()));
        let (request_tx, request_rx) = channel();
        let scanner = std::thread::spawn(move || {
            run_process_scanner(snapshot_tx, request_rx, Duration::from_secs(60), || {
                captures_for_scanner.fetch_add(1, Ordering::Relaxed);
                Ok(ProcessSnapshot::empty())
            })
        });

        let active_pids = Arc::new(Mutex::new(HashMap::new()));
        let active_pids_for_job = active_pids.clone();
        let active_pids_for_test = active_pids.clone();
        let captures_for_test = captures.clone();
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownState::Running);
        let request_tx_for_job = request_tx.clone();
        let job_scanner = ProcessScannerHandle {
            snapshots: snapshot_rx,
            requests: request_tx_for_job,
        };
        let base_env = Arc::new(env::vars().collect());
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();

        runtime.block_on(async move {
            let job = tokio::spawn(start_proc(
                "refresh_cleanup".to_string(),
                job_config,
                active_pids_for_job,
                shutdown_rx,
                job_scanner,
                base_env,
            ));
            timeout(Duration::from_secs(2), async {
                while lock_or_recover(&active_pids_for_test).is_empty() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("job did not start");
            let root = lock_or_recover(&active_pids_for_test)
                .values()
                .next()
                .unwrap()
                .info
                .identity;
            timeout(Duration::from_secs(2), async {
                while process_tree::is_running_checked(root).unwrap() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("job did not exit");
            timeout(Duration::from_secs(1), async {
                while captures_for_test.load(Ordering::Relaxed) == 0 {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("natural-exit cleanup did not request a fresh snapshot");
            shutdown_tx.send(ShutdownState::Graceful).unwrap();
            timeout(Duration::from_secs(2), job)
                .await
                .expect("job supervisor did not stop")
                .unwrap()
                .unwrap();
        });

        request_tx.send(ProcessScannerRequest::Stop).unwrap();
        scanner.join().unwrap().unwrap();
        assert_eq!(captures.load(Ordering::Relaxed), 1);
        assert!(lock_or_recover(&active_pids).is_empty());
    }

    #[test]
    fn natural_exit_refresh_failure_cleans_known_descendants() {
        let temp = tempfile::tempdir().unwrap();
        let ready = temp.path().join("ready");
        let child_pid = temp.path().join("child.pid");
        let config: Config = serde_yaml::from_str(&format!(
            "jobs:\n  failed_refresh:\n    command: |\n      sleep 30 &\n      echo $! > {}\n      touch {}\n      sleep 1\n      printf tail\n    restart_delay: 10000\n",
            child_pid.display(),
            ready.display()
        ))
        .unwrap();
        let (_, mut job_config) = config.jobs.into_iter().next().unwrap();
        job_config.cwd = temp.path().to_string_lossy().into_owned();

        let active_pids = Arc::new(Mutex::new(HashMap::new()));
        let active_pids_for_job = active_pids.clone();
        let active_pids_for_test = active_pids.clone();
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownState::Running);
        let (snapshot_tx, snapshot_rx) = watch::channel(Arc::new(ProcessSnapshot::empty()));
        let (scanner_tx, scanner_rx) = channel();
        drop(scanner_rx);
        let job_scanner = ProcessScannerHandle {
            snapshots: snapshot_rx,
            requests: scanner_tx,
        };
        let base_env = Arc::new(env::vars().collect());
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();

        let error = runtime.block_on(async move {
            let job = tokio::spawn(start_proc(
                "failed_refresh".to_string(),
                job_config,
                active_pids_for_job,
                shutdown_rx,
                job_scanner,
                base_env,
            ));
            timeout(Duration::from_secs(2), async {
                while !ready.exists() || lock_or_recover(&active_pids_for_test).is_empty() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("job did not start");
            let child_pid = std::fs::read_to_string(&child_pid)
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            let child = process_tree::identity(child_pid).unwrap();
            snapshot_tx
                .send(Arc::new(ProcessSnapshot::capture().unwrap()))
                .unwrap();
            timeout(Duration::from_secs(1), async {
                while lock_or_recover(&active_pids_for_test)
                    .values()
                    .all(|run| run.descendants.is_empty())
                {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("descendant was not tracked before the job exited");
            let error = timeout(Duration::from_secs(3), job)
                .await
                .expect("job did not report the scanner failure")
                .unwrap()
                .unwrap_err();
            timeout(Duration::from_secs(2), async {
                while process_tree::is_running_checked(child).unwrap() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("known descendant survived refresh failure cleanup");
            let _ = shutdown_tx.send(ShutdownState::Graceful);
            error
        });

        assert!(error.to_string().contains("process scanner is unavailable"));
        assert!(lock_or_recover(&active_pids).is_empty());
    }

    #[test]
    fn periodic_scanner_recovers_from_a_transient_capture_failure() {
        let (snapshot_tx, mut snapshot_rx) = watch::channel(Arc::new(ProcessSnapshot::empty()));
        let (request_tx, request_rx) = channel();
        let scanner = std::thread::spawn(move || {
            let mut attempts = 0;
            run_process_scanner(snapshot_tx, request_rx, Duration::ZERO, || {
                attempts += 1;
                if attempts == 1 {
                    Err(std::io::Error::from(std::io::ErrorKind::NotFound))
                } else {
                    Ok(ProcessSnapshot::empty())
                }
            })
        });
        let runtime = Builder::new_current_thread().enable_time().build().unwrap();

        runtime
            .block_on(async { timeout(Duration::from_secs(2), snapshot_rx.changed()).await })
            .expect("scanner did not publish after a transient failure")
            .unwrap();
        request_tx.send(ProcessScannerRequest::Stop).unwrap();
        scanner.join().unwrap().unwrap();
    }

    #[test]
    fn persistent_capture_failure_remains_fatal() {
        let mut last_log = None;
        let mut attempts = 0;
        let error = capture_snapshot_with_retry(
            &mut || {
                attempts += 1;
                Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
            },
            Duration::from_millis(5),
            Duration::from_millis(25),
            &mut last_log,
        )
        .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(attempts > 1, "persistent failure was not retried");
    }

    #[test]
    fn scanner_anomaly_logs_are_rate_limited_by_error_kind() {
        let denied = ProcessSnapshot::with_anomaly(
            1,
            std::io::ErrorKind::PermissionDenied,
            "permission denied",
        );
        let invalid =
            ProcessSnapshot::with_anomaly(1, std::io::ErrorKind::InvalidData, "invalid data");
        let mixed = ProcessSnapshot::with_anomaly_kinds(
            2,
            HashSet::from([
                std::io::ErrorKind::PermissionDenied,
                std::io::ErrorKind::InvalidData,
            ]),
            "mixed failures",
        );
        let mut last_log = None;

        log_process_scan_anomalies(&denied, &mut last_log);
        let first_log = last_log.clone();
        log_process_scan_anomalies(&denied, &mut last_log);
        assert_eq!(last_log, first_log);

        log_process_scan_anomalies(&invalid, &mut last_log);
        assert_eq!(last_log, first_log);
        last_log.as_mut().unwrap().0 = Instant::now()
            .checked_sub(PROCESS_SCAN_WARNING_CHANGE_FLOOR)
            .unwrap();
        log_process_scan_anomalies(&invalid, &mut last_log);
        assert_eq!(
            last_log.as_ref().map(|(_, kinds)| kinds),
            Some(&HashSet::from([std::io::ErrorKind::InvalidData]))
        );

        log_process_scan_anomalies(&mixed, &mut last_log);
        assert_eq!(last_log.as_ref().unwrap().1.len(), 1);
        last_log.as_mut().unwrap().0 = Instant::now()
            .checked_sub(PROCESS_SCAN_WARNING_CHANGE_FLOOR)
            .unwrap();
        log_process_scan_anomalies(&mixed, &mut last_log);
        assert_eq!(last_log.as_ref().unwrap().1.len(), 2);
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
        let (scanner_tx, scanner_rx) = channel();
        drop(scanner_rx);
        let job_scanner = ProcessScannerHandle {
            snapshots: snapshot_rx,
            requests: scanner_tx,
        };
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
                job_scanner,
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
    fn shutdown_kills_a_descendant_first_observed_after_sigkill() {
        use std::os::unix::process::CommandExt;

        let temp = tempfile::tempdir().unwrap();
        let ready = temp.path().join("ready");
        let config: Config = serde_yaml::from_str(&format!(
            "jobs:\n  late_descendant:\n    command: |\n      trap '' TERM\n      touch {}\n      while :; do sleep 1; done\n    shutdown_timeout: 30\n",
            ready.display()
        ))
        .unwrap();
        let (_, mut job_config) = config.jobs.into_iter().next().unwrap();
        job_config.cwd = temp.path().to_string_lossy().into_owned();

        let mut late_process = std::process::Command::new("sleep");
        late_process.arg("30");
        unsafe {
            late_process.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut late_process = late_process.spawn().unwrap();
        let late_identity = process_tree::identity(late_process.id()).unwrap();

        let active_pids = Arc::new(Mutex::new(HashMap::new()));
        let active_pids_for_job = active_pids.clone();
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownState::Running);
        let (snapshot_tx, snapshot_rx) = watch::channel(Arc::new(ProcessSnapshot::empty()));
        let (scanner_tx, scanner_rx) = channel();
        drop(scanner_rx);
        let job_scanner = ProcessScannerHandle {
            snapshots: snapshot_rx,
            requests: scanner_tx,
        };
        let base_env = Arc::new(env::vars().collect());
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();

        let result = runtime.block_on(async move {
            let job = tokio::spawn(start_proc(
                "late_descendant".to_string(),
                job_config,
                active_pids_for_job,
                shutdown_rx,
                job_scanner,
                base_env,
            ));
            timeout(Duration::from_secs(3), async {
                while !ready.exists() || lock_or_recover(&active_pids).is_empty() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("job did not start");
            let root = lock_or_recover(&active_pids)
                .values()
                .next()
                .unwrap()
                .info
                .identity;

            shutdown_tx.send(ShutdownState::Force).unwrap();
            sleep(Duration::from_millis(100)).await;
            snapshot_tx
                .send(Arc::new(ProcessSnapshot::with_descendant(
                    root,
                    late_identity,
                )))
                .unwrap();
            sleep(Duration::from_millis(100)).await;
            snapshot_tx
                .send(Arc::new(ProcessSnapshot::with_descendant(
                    root,
                    late_identity,
                )))
                .unwrap();
            timeout(Duration::from_secs(5), job)
                .await
                .expect("late descendant stalled shutdown")
                .unwrap()
        });

        let late_survived = process_tree::is_running_checked(late_identity).unwrap();
        if late_survived {
            let _ = late_process.kill();
        }
        let _ = late_process.wait();
        assert!(result.is_ok(), "late descendant made shutdown fatal");
        assert!(!late_survived, "late descendant survived shutdown");
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

    #[test]
    fn tracked_liveness_errors_are_not_treated_as_exit() {
        let process = ProcessIdentity {
            pid: 123,
            process_group: 123,
            start_time: 456,
        };
        let error = any_process_running_checked_with(&HashSet::from([process]), |_| {
            Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
        })
        .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn shutdown_escalates_only_after_consecutive_inspection_failures() {
        let mut failures = 0;

        assert!(!inspection_failure_requires_kill(
            &mut failures,
            true,
            false
        ));
        assert_eq!(failures, 1);
        assert!(!inspection_failure_requires_kill(
            &mut failures,
            false,
            false
        ));
        assert_eq!(failures, 0);
        assert!(!inspection_failure_requires_kill(
            &mut failures,
            true,
            false
        ));
        assert!(inspection_failure_requires_kill(&mut failures, true, false));
        assert!(!inspection_failure_requires_kill(&mut failures, true, true));
    }

    #[test]
    fn target_selection_records_inspection_errors_without_stopping() {
        let first = ProcessIdentity {
            pid: 123,
            process_group: 123,
            start_time: 456,
        };
        let second = ProcessIdentity {
            pid: 124,
            process_group: 123,
            start_time: 457,
        };
        let mut inspection_errors = SignalErrors::default();

        assert!(!process_is_running_best_effort_with(
            first,
            &mut inspection_errors,
            |_| Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        ));
        assert!(process_is_running_best_effort_with(
            second,
            &mut inspection_errors,
            |_| Ok(true),
        ));
        assert_eq!(
            inspection_errors.error.unwrap().kind(),
            std::io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn signal_errors_report_the_first_delivery_failure() {
        let mut errors = SignalErrors::default();
        errors.record_inspection(std::io::Error::other("inspection"));
        errors.record_delivery(std::io::Error::other("first delivery"));
        errors.record_delivery(std::io::Error::other("second delivery"));

        let error = errors.finish().unwrap_err();
        assert!(error.delivery_failed);
        assert_eq!(error.error.to_string(), "first delivery");
    }

    #[test]
    fn cleanup_signals_before_returning_an_inspection_error() {
        let unreadable = ProcessIdentity {
            pid: 123,
            process_group: 123,
            start_time: 456,
        };
        let running = ProcessIdentity {
            pid: 124,
            process_group: 123,
            start_time: 457,
        };
        let signaled = std::cell::RefCell::new(None);
        let runtime = Builder::new_current_thread().build().unwrap();

        let error = runtime
            .block_on(cleanup_known_processes_with(
                &HashSet::from([unreadable, running]),
                |process| {
                    if process == unreadable {
                        Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
                    } else {
                        Ok(true)
                    }
                },
                |targets| {
                    signaled.replace(Some(targets));
                    std::future::ready(Ok(()))
                },
            ))
            .unwrap_err();

        assert!(signaled.borrow().as_ref().unwrap().contains(&running));
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn zombie_root_still_owns_its_process_group() {
        unsafe {
            let mut pid_pipe = [0; 2];
            assert_eq!(libc::pipe(pid_pipe.as_mut_ptr()), 0);
            let mut alive_pipe = [0; 2];
            assert_eq!(libc::pipe(alive_pipe.as_mut_ptr()), 0);

            let root = libc::fork();
            assert!(root >= 0, "fork failed");
            if root == 0 {
                libc::setsid();
                let member = libc::fork();
                if member == 0 {
                    libc::close(pid_pipe[0]);
                    libc::close(pid_pipe[1]);
                    libc::close(alive_pipe[0]);
                    loop {
                        libc::pause();
                    }
                }
                if member < 0 {
                    libc::_exit(1);
                }
                libc::close(alive_pipe[0]);
                libc::close(alive_pipe[1]);
                libc::close(pid_pipe[0]);
                let root_pid = libc::getpid() as u32;
                libc::write(
                    pid_pipe[1],
                    std::ptr::addr_of!(root_pid).cast::<libc::c_void>(),
                    std::mem::size_of::<u32>(),
                );
                libc::close(pid_pipe[1]);
                libc::usleep(400_000);
                libc::_exit(0);
            }
            struct ProcessGroupGuard(libc::pid_t);
            impl Drop for ProcessGroupGuard {
                fn drop(&mut self) {
                    unsafe {
                        libc::kill(-self.0, libc::SIGKILL);
                        libc::kill(self.0, libc::SIGKILL);
                        libc::waitpid(self.0, std::ptr::null_mut(), 0);
                    }
                }
            }
            let _cleanup = ProcessGroupGuard(root);
            libc::close(pid_pipe[1]);
            libc::close(alive_pipe[1]);

            let mut raw_pid = [0; std::mem::size_of::<u32>()];
            assert_eq!(
                libc::read(
                    pid_pipe[0],
                    raw_pid.as_mut_ptr().cast::<libc::c_void>(),
                    raw_pid.len(),
                ),
                raw_pid.len() as isize
            );
            libc::close(pid_pipe[0]);
            let root_pid = u32::from_ne_bytes(raw_pid);
            let identity = process_tree::identity(root_pid).unwrap();
            assert_eq!(identity.process_group, root_pid);

            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                let stat = std::fs::read(format!("/proc/{root_pid}/stat")).unwrap();
                let close = stat.iter().rposition(|byte| *byte == b')').unwrap();
                if stat[close + 2] == b'Z' {
                    break;
                }
                assert!(Instant::now() < deadline, "root never became a zombie");
                std::thread::sleep(Duration::from_millis(20));
            }

            let runtime = Builder::new_current_thread().enable_all().build().unwrap();
            let result = runtime.block_on(signal_process_tree(
                "zombie root",
                None,
                PidInfo {
                    is_process_group: true,
                    identity,
                },
                &HashSet::new(),
                libc::SIGKILL,
            ));
            assert!(result.is_ok(), "signaling reported an error");

            libc::fcntl(alive_pipe[0], libc::F_SETFL, libc::O_NONBLOCK);
            let deadline = Instant::now() + Duration::from_secs(2);
            let group_member_died = loop {
                let mut byte = 0;
                let read = libc::read(
                    alive_pipe[0],
                    std::ptr::addr_of_mut!(byte).cast::<libc::c_void>(),
                    1,
                );
                if read == 0 {
                    break true;
                }
                if Instant::now() >= deadline {
                    break false;
                }
                std::thread::sleep(Duration::from_millis(20));
            };
            if !group_member_died {
                libc::kill(-(root_pid as libc::pid_t), libc::SIGKILL);
            }
            libc::close(alive_pipe[0]);

            assert!(
                group_member_died,
                "group member survived SIGKILL while its root was a zombie"
            );
        }
    }
}
