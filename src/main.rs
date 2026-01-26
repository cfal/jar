use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::io::Write;
use std::process::{ExitCode, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use rand::Rng;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::{Child, Command};
use tokio::runtime::Builder;
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::JoinHandle;
use tokio::time::sleep;

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
    /// Run job in its own process group (setsid). Allows killing all child processes together.
    #[serde(default, alias = "processGroup")]
    process_group: bool,
    /// Seconds to wait after SIGTERM before sending SIGKILL (default: 5)
    #[serde(default = "default_shutdown_timeout", alias = "shutdownTimeout")]
    shutdown_timeout: u64,
}

fn default_shutdown_timeout() -> u64 {
    5
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

/// Lock a mutex, recovering from poison if necessary.
/// For a process supervisor, it's better to continue with potentially stale data
/// than to crash entirely during shutdown.
fn lock_or_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn default_datetime_format() -> String {
    String::from("%Y-%m-%d %H:%M:%S %Z")
}

#[derive(Debug, Deserialize)]
struct Config {
    jobs: BTreeMap<String, JobConfig>,
}

/// Information tracked for each running process
#[derive(Clone, Copy)]
struct PidInfo {
    /// Whether this process is a process group leader (setsid)
    is_process_group: bool,
    /// Seconds to wait after SIGTERM before SIGKILL
    shutdown_timeout: u64,
}

#[derive(Clone)]
struct PrefixWriter<'a> {
    prefix: String,
    datetime_items: Vec<chrono::format::Item<'a>>,
    datetime_local: bool,
}

impl<'a> PrefixWriter<'a> {
    pub fn from(prefix: String, config: &'a DateTimeConfig) -> Self {
        let datetime_items = match config.enabled {
            true => chrono::format::StrftimeItems::new(&config.format).collect(),
            false => vec![],
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
            write!(f, "{}", self.prefix)
        } else {
            write!(
                f,
                "{} > {}",
                self.prefix,
                match self.datetime_local {
                    true =>
                        chrono::offset::Local::now().format_with_items(self.datetime_items.iter()),
                    false =>
                        chrono::offset::Utc::now().format_with_items(self.datetime_items.iter()),
                }
            )
        }
    }
}

async fn start_proc(
    prefix: String,
    opts: JobConfig,
    active_pids: Arc<Mutex<HashMap<u32, PidInfo>>>,
    shutdown: Arc<AtomicBool>,
    base_env: Arc<HashMap<String, String>>,
) -> bool {
    let status_prefix = PrefixWriter::from(format!("{} (s)", prefix), &opts.datetime);
    let mut initial = true;
    let mut had_failure = false;

    loop {
        // Check shutdown before starting a new process
        if shutdown.load(Ordering::Acquire) {
            break;
        }

        let delay = Duration::from_millis(if initial {
            initial = false;
            opts.delay
        } else {
            opts.restart_delay
        });

        if !delay.is_zero() {
            eprintln!("{} > Starting in {}s", status_prefix, delay.as_secs());
            sleep(delay).await;

            // Check shutdown again after sleeping
            if shutdown.load(Ordering::Acquire) {
                break;
            }
        }

        match start_proc_internal(&status_prefix, &prefix, &opts, &active_pids, &shutdown, &base_env).await {
            Ok(true) => break,
            Ok(false) => {
                had_failure = true;
            }
            Err(e) => {
                eprintln!("{} > Process error: {}", status_prefix, e);
                had_failure = true;
            }
        }
    }
    eprintln!("{} > Shutting down..", status_prefix);
    had_failure
}

async fn start_proc_internal(
    status_prefix: &PrefixWriter<'_>,
    prefix: &str,
    opts: &JobConfig,
    active_pids: &Arc<Mutex<HashMap<u32, PidInfo>>>,
    shutdown: &Arc<AtomicBool>,
    base_env: &Arc<HashMap<String, String>>,
) -> std::io::Result<bool> {
    // Check shutdown before spawning
    if shutdown.load(Ordering::Acquire) {
        return Ok(true);
    }

    let mut envs = base_env.as_ref().clone();
    if let Some(additional_env) = &opts.env {
        envs.extend(additional_env.clone());
    }

    // Validate working directory exists before spawning
    let cwd_path = std::path::Path::new(&opts.cwd);
    if !cwd_path.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Working directory does not exist: {}", opts.cwd),
        ));
    }

    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg(match opts.sudo_user {
            Some(ref username) => format!("exec sudo -u {} {}", username, opts.command),
            None => format!("exec {}", opts.command),
        })
        .current_dir(opts.cwd.clone())
        .envs(&envs)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    // Optionally create a new session/process group so we can kill all children together
    #[cfg(unix)]
    if opts.process_group {
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    let child = cmd.spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| std::io::Error::other("Could not get child pid"))?;

    // Check shutdown again before registering PID
    if shutdown.load(Ordering::Acquire) {
        // Process was spawned but we're shutting down - do inline graceful shutdown
        // to avoid orphaning this process (it's not tracked in the main shutdown handler)
        send_signal_libc(pid, libc::SIGTERM, opts.process_group);

        // Wait for process to exit or timeout, then SIGKILL if needed
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(opts.shutdown_timeout);
        while start.elapsed() < timeout && is_process_alive(pid) {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if is_process_alive(pid) {
            send_signal_libc(pid, libc::SIGKILL, opts.process_group);
        }
        // Wait on the child to reap it and avoid zombie
        drop(child);
        return Ok(true);
    }

    // Track PID with its shutdown configuration
    lock_or_recover(active_pids).insert(pid, PidInfo {
        is_process_group: opts.process_group,
        shutdown_timeout: opts.shutdown_timeout,
    });

    eprintln!("{} > Started (pid {})", status_prefix, pid);

    let handle_child_result = handle_child(status_prefix, prefix, &opts.datetime, child).await;

    // If PID was already removed from map, we're shutting down - don't restart
    if lock_or_recover(&active_pids).remove(&pid).is_none() {
        Ok(true)
    } else {
        handle_child_result?;
        Ok(false)
    }
}

async fn handle_child(
    status_prefix: &PrefixWriter<'_>,
    prefix: &str,
    datetime: &DateTimeConfig,
    mut child: Child,
) -> std::io::Result<()> {
    let stdout_prefix = format!("{} (1)", prefix);
    let stdout_handle = spawn_output_handler(
        child.stdout.take().ok_or_else(|| {
            std::io::Error::other("Could not get stdout")
        })?,
        stdout_prefix,
        datetime.clone(),
        std::io::stdout(),
    );
    let stderr_prefix = format!("{} (2)", prefix);
    let stderr_handle = spawn_output_handler(
        child.stderr.take().ok_or_else(|| {
            std::io::Error::other("Could not get stderr")
        })?,
        stderr_prefix,
        datetime.clone(),
        std::io::stderr(),
    );
    let status = child.wait().await?;
    // Ignore errors from output handlers - they may fail if stdout/stderr is closed
    let _ = stdout_handle.await;
    let _ = stderr_handle.await;

    // Handle both normal exit codes and signal termination
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(code) = status.code() {
            eprintln!("{} > Exited with code {}", status_prefix, code);
        } else if let Some(signal) = status.signal() {
            eprintln!("{} > Killed by signal {}", status_prefix, signal);
        } else {
            eprintln!("{} > Exited with unknown status", status_prefix);
        }
    }

    #[cfg(not(unix))]
    {
        match status.code() {
            Some(code) => eprintln!("{} > Exited with code {}", status_prefix, code),
            None => eprintln!("{} > Exited with unknown status", status_prefix),
        }
    }

    Ok(())
}

fn spawn_output_handler<R: 'static + AsyncRead + Send + Unpin, T: Write + Send + 'static>(
    reader: R,
    output_prefix: String,
    datetime: DateTimeConfig,
    mut writer: T,
) -> JoinHandle<std::io::Result<()>> {
    tokio::spawn(async move {
        let output_prefix = PrefixWriter::from(output_prefix, &datetime);
        let reader = BufReader::new(reader);
        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await? {
            // Gracefully handle write failures (e.g., stdout closed)
            if writeln!(writer, "{} > {}", output_prefix, line).is_err() {
                break;
            }
        }

        Ok(())
    })
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
            eprintln!("No such color, ignoring: {}", color);
            return tag.to_string();
        }
    };

    let reset = "\x1b[0m";
    format!("{}{}{}", code, tag, reset)
}

fn main() -> ExitCode {
    let args = env::args().collect::<Vec<_>>();
    if args.len() < 2 || args.iter().any(|arg| arg.as_str() == "--help") {
        eprintln!("Usage: {} [--wrap] config.json", args[0]);
        return ExitCode::from(1);
    }

    if args[1] == "--wrap" {
        if args.len() < 3 {
            eprintln!("No config file provided.");
            return ExitCode::from(1);
        }
        // Perf: collect args once outside the loop
        let wrapper_args: Vec<_> = args[2..].to_vec();
        loop {
            eprintln!("[Wrapper] Starting inner process..");
            match std::process::Command::new(&args[0])
                .args(&wrapper_args)
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .status()
            {
                Ok(exit_status) => match exit_status.code() {
                    Some(code) => eprintln!("[Wrapper] Exited with status code: {code}"),
                    None => eprintln!("[Wrapper] Process terminated by signal"),
                },
                Err(e) => {
                    eprintln!("[Wrapper] Failed to read exit status: {}", e);
                }
            }
            eprintln!("[Wrapper] Inner process exited, waiting 15 seconds before restarting..");
            std::thread::sleep(Duration::from_millis(15_000));
        }
    }

    let config_path = &args[1];

    // Get the config file's parent directory for #cwd# substitution
    // If the path has no parent (e.g., just "config.yaml"), use current directory
    let config_parent_path = std::path::Path::new(config_path)
        .parent()
        .and_then(|p| {
            let s = p.to_string_lossy();
            // parent() returns Some("") for paths without directory component
            if s.is_empty() { None } else { Some(s.into_owned()) }
        })
        .or_else(|| {
            env::current_dir()
                .ok()
                .map(|p| p.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| ".".to_string());

    let home_path = env::var("HOME").unwrap_or_else(|_| ".".to_string());

    let config_data = match std::fs::read_to_string(config_path) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Could not read config file: {}", e);
            return ExitCode::from(1);
        }
    };

    let mut config: Config = match serde_yaml::from_str(&config_data) {
        Ok(c) => c,
        Err(_) => {
            let config_data = config_data
                .split('\n')
                .filter(|s| !s.trim_start().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n");
            match serde_json::from_str(&config_data) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Could not parse config file: {}", e);
                    return ExitCode::from(1);
                }
            }
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
                    eprintln!(
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
            .replace("#home#", &home_path);

        job_config.command = job_config
            .command
            .replace("#cwd#", &config_parent_path)
            .replace("#home#", &home_path);
    }

    // Cache environment variables once at startup (perf improvement)
    let base_env: Arc<HashMap<String, String>> = Arc::new(env::vars().collect());

    // Shutdown flag to prevent race conditions when spawning new processes
    let shutdown = Arc::new(AtomicBool::new(false));

    let mut builder = Builder::new_current_thread();
    let runtime = match builder.enable_io().enable_time().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("Could not build tokio runtime: {}", e);
            return ExitCode::from(1);
        }
    };

    let active_pid_map = runtime.block_on(async move {
        // Register signal handlers FIRST - if any fail, we cannot operate safely
        // and should not start any jobs that we couldn't manage
        macro_rules! register_signal {
            ($kind:expr, $name:expr) => {
                match signal($kind) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("Failed to register {} handler: {}", $name, e);
                        // Return None to signal failure to caller
                        return None;
                    }
                }
            };
        }

        let mut sigterm = register_signal!(SignalKind::terminate(), "SIGTERM");
        let mut sighup = register_signal!(SignalKind::hangup(), "SIGHUP");
        let mut sigint = register_signal!(SignalKind::interrupt(), "SIGINT");
        let mut sigquit = register_signal!(SignalKind::quit(), "SIGQUIT");
        let mut sigusr1 = register_signal!(SignalKind::user_defined1(), "SIGUSR1");
        let mut sigusr2 = register_signal!(SignalKind::user_defined2(), "SIGUSR2");

        // Now safe to start jobs since we can handle signals
        let mut pid_map: HashMap<String, Arc<Mutex<HashMap<u32, PidInfo>>>> = HashMap::new();
        let mut job_handles: Vec<JoinHandle<bool>> = Vec::new();

        for (name, job_config) in config.jobs {
            if job_config.disabled {
                eprintln!("! Skipping disabled job: {}", name);
                continue;
            }

            if job_config.command.is_empty() {
                eprintln!("! Skipping job with missing command: {}", name);
                continue;
            }

            eprintln!("Initializing job \"{}\": {:?}", name, job_config);
            let prefix = create_prefix(&job_config.color, &name);

            let sudo_user = job_config.sudo_user.clone().unwrap_or_default();
            let pid_set = match pid_map.entry(sudo_user) {
                Entry::Occupied(o) => o.get().clone(),
                Entry::Vacant(v) => v.insert(Arc::new(Mutex::new(HashMap::new()))).clone(),
            };

            let handle = tokio::spawn(start_proc(
                prefix,
                job_config,
                pid_set,
                shutdown.clone(),
                base_env.clone(),
            ));
            job_handles.push(handle);
        }

        // Signal handling loop: forward USR1/USR2, exit on termination signals
        let signal_name = loop {
            let signal = tokio::select! {
                _ = sigterm.recv() => Some(("TERM", None)),
                _ = sighup.recv() => Some(("HUP", None)),
                _ = sigint.recv() => Some(("INT", None)),
                _ = sigquit.recv() => Some(("QUIT", None)),
                _ = sigusr1.recv() => Some(("USR1", Some("USR1"))),
                _ = sigusr2.recv() => Some(("USR2", Some("USR2"))),
            };

            match signal {
                Some((name, Some(forward_signal))) => {
                    // Forward USR1/USR2 to all active processes
                    eprintln!("Received {} signal, forwarding to all jobs..", name);
                    for (sudo_user, pid_set_mutex) in pid_map.iter() {
                        let pid_set = lock_or_recover(pid_set_mutex);
                        if pid_set.is_empty() {
                            continue;
                        }

                        if sudo_user.is_empty() {
                            // Use libc::kill directly for non-sudo cases
                            if let Some(sig) = signal_name_to_number(forward_signal) {
                                for (pid, info) in pid_set.iter() {
                                    send_signal_libc(*pid, sig, info.is_process_group);
                                }
                            }
                        } else {
                            // Must use sudo subprocess for different users
                            let mut sudo_cmd = std::process::Command::new("sudo");
                            sudo_cmd.args(["-u", sudo_user, "kill", &format!("-{}", forward_signal)]);
                            for pid in pid_set.keys() {
                                sudo_cmd.arg(pid.to_string());
                            }
                            let _ = sudo_cmd
                                .stdout(Stdio::null())
                                .stderr(Stdio::null())
                                .status();
                        }
                    }
                }
                Some((name, None)) => {
                    // Termination signal - break out of loop
                    break name;
                }
                None => continue,
            }
        };

        // Print an extra newline in case we have a ^C prefix.
        eprintln!(
            "\nReceived {} signal, killing active processes..",
            signal_name
        );

        // Set shutdown flag BEFORE clearing PIDs to prevent race condition
        shutdown.store(true, Ordering::Release);

        let mut cloned_pid_map = HashMap::new();
        for (sudo_user, pid_set_mutex) in pid_map.into_iter() {
            let mut pid_set = lock_or_recover(&pid_set_mutex);
            let cloned_pid_set = pid_set.clone();
            pid_set.clear();
            cloned_pid_map.insert(sudo_user, cloned_pid_set);
        }

        // Don't wait for job handles here - they're blocked on child processes
        // which won't exit until we send them signals. Drop them and let
        // shutdown_background handle cleanup.
        drop(job_handles);

        Some(cloned_pid_map)
    });

    // Shutdown runtime - this will abort the blocked tasks
    runtime.shutdown_background();

    // Handle signal registration failure (returned None)
    let Some(active_pid_map) = active_pid_map else {
        eprintln!("Failed to initialize signal handlers, exiting");
        return ExitCode::from(1);
    };

    // Collect all PIDs for graceful shutdown
    let all_pids: Vec<(String, u32, PidInfo)> = active_pid_map
        .into_iter()
        .flat_map(|(sudo_user, pid_map): (String, HashMap<u32, PidInfo>)| {
            pid_map
                .into_iter()
                .map(move |(pid, info)| (sudo_user.clone(), pid, info))
        })
        .collect();

    if all_pids.is_empty() {
        return ExitCode::SUCCESS;
    }

    // Send SIGTERM to all processes (using -pid for process groups)
    send_signal_to_pids(&all_pids, "TERM");

    // Graceful shutdown: wait for each process according to its timeout
    let max_timeout = all_pids
        .iter()
        .map(|(_, _, info)| info.shutdown_timeout)
        .max()
        .unwrap_or(5);

    let start = std::time::Instant::now();
    let check_interval = Duration::from_millis(100);

    // Track which PIDs have exceeded their timeout
    let mut killed_pids: std::collections::HashSet<u32> = std::collections::HashSet::new();

    loop {
        let elapsed_secs = start.elapsed().as_secs();

        // Check each process
        for (sudo_user, pid, info) in &all_pids {
            if killed_pids.contains(pid) {
                continue;
            }

            let alive = is_process_alive(*pid);
            if !alive {
                // Process exited on its own
                continue;
            }

            // If this process has exceeded its timeout, kill it
            if elapsed_secs >= info.shutdown_timeout {
                eprintln!(
                    "Process {} exceeded {}s shutdown timeout, sending SIGKILL..",
                    pid, info.shutdown_timeout
                );
                send_signal_to_pids(&[(sudo_user.clone(), *pid, *info)], "KILL");
                killed_pids.insert(*pid);
            }
        }

        // Check if all processes are done
        let any_alive = all_pids
            .iter()
            .any(|(_, pid, _)| !killed_pids.contains(pid) && is_process_alive(*pid));

        if !any_alive {
            eprintln!("All processes exited gracefully");
            break;
        }

        // Exit if we've exceeded the maximum timeout
        if start.elapsed() >= Duration::from_secs(max_timeout) {
            break;
        }

        std::thread::sleep(check_interval);
    }

    ExitCode::SUCCESS
}

/// Check if a process is still alive using libc::kill with signal 0
fn is_process_alive(pid: u32) -> bool {
    // Safely convert PID to pid_t - PIDs above i32::MAX are invalid
    let Ok(pid_signed) = i32::try_from(pid) else {
        return false; // Invalid PID cannot be alive
    };

    // kill(pid, 0) checks if process exists without sending a signal
    // Returns 0 if process exists and we have permission to signal it
    // Returns -1 with ESRCH if process doesn't exist
    // Returns -1 with EPERM if process exists but we can't signal it (still alive)
    let result = unsafe { libc::kill(pid_signed as libc::pid_t, 0) };
    if result == 0 {
        return true;
    }
    // If we got EPERM, process exists but we can't signal it - still alive
    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
    errno == libc::EPERM
}

/// Convert a signal name to its numeric value
fn signal_name_to_number(signal: &str) -> Option<libc::c_int> {
    match signal {
        "TERM" => Some(libc::SIGTERM),
        "KILL" => Some(libc::SIGKILL),
        "USR1" => Some(libc::SIGUSR1),
        "USR2" => Some(libc::SIGUSR2),
        "HUP" => Some(libc::SIGHUP),
        "INT" => Some(libc::SIGINT),
        "QUIT" => Some(libc::SIGQUIT),
        _ => None,
    }
}

/// Send a signal to a single PID using libc::kill
/// For process groups, pass is_process_group=true to send to the process group
fn send_signal_libc(pid: u32, signal: libc::c_int, is_process_group: bool) -> bool {
    let target_pid = if is_process_group {
        // Negative PID sends signal to the process group
        -(pid as libc::pid_t)
    } else {
        pid as libc::pid_t
    };

    let result = unsafe { libc::kill(target_pid, signal) };
    result == 0
}

/// Send a signal to a list of PIDs, using process groups where applicable
/// Uses libc::kill directly for non-sudo cases, falls back to subprocess for sudo
fn send_signal_to_pids(pids: &[(String, u32, PidInfo)], signal: &str) {
    let signal_num = signal_name_to_number(signal);

    // Group by sudo user for efficient batching
    let mut by_sudo_user: HashMap<&str, Vec<(u32, bool)>> = HashMap::new();
    for (sudo_user, pid, info) in pids {
        by_sudo_user
            .entry(sudo_user.as_str())
            .or_default()
            .push((*pid, info.is_process_group));
    }

    for (sudo_user, pid_list) in by_sudo_user {
        if sudo_user.is_empty() {
            // Use libc::kill directly for non-sudo cases
            if let Some(sig) = signal_num {
                for (pid, is_group) in pid_list {
                    send_signal_libc(pid, sig, is_group);
                }
            } else {
                // Unknown signal, fall back to subprocess
                let mut cmd = std::process::Command::new("kill");
                cmd.args([&format!("-{}", signal), "--"]);
                for (pid, is_group) in pid_list {
                    if is_group {
                        cmd.arg(format!("-{}", pid));
                    } else {
                        cmd.arg(pid.to_string());
                    }
                }
                let _ = cmd.stdout(Stdio::null()).stderr(Stdio::null()).status();
            }
        } else {
            // Must use sudo subprocess for different users
            let mut sudo_cmd = std::process::Command::new("sudo");
            sudo_cmd.args(["-u", sudo_user, "kill", &format!("-{}", signal), "--"]);
            for (pid, is_group) in pid_list {
                if is_group {
                    sudo_cmd.arg(format!("-{}", pid));
                } else {
                    sudo_cmd.arg(pid.to_string());
                }
            }
            let _ = sudo_cmd.stdout(Stdio::null()).stderr(Stdio::null()).status();
        }
    }
}
