use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::io::Write;
use std::process::{ExitCode, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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

fn default_datetime_format() -> String {
    String::from("%Y-%m-%d %H:%M:%S %Z")
}

#[derive(Debug, Deserialize)]
struct Config {
    jobs: BTreeMap<String, JobConfig>,
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
    active_pids: Arc<Mutex<HashSet<u32>>>,
    shutdown: Arc<AtomicBool>,
    base_env: Arc<HashMap<String, String>>,
) -> bool {
    let status_prefix = PrefixWriter::from(format!("{} (s)", prefix), &opts.datetime);
    let mut initial = true;
    let mut had_failure = false;

    loop {
        // Check shutdown before starting a new process
        if shutdown.load(Ordering::Relaxed) {
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
            if shutdown.load(Ordering::Relaxed) {
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
    active_pids: &Arc<Mutex<HashSet<u32>>>,
    shutdown: &Arc<AtomicBool>,
    base_env: &Arc<HashMap<String, String>>,
) -> std::io::Result<bool> {
    // Check shutdown before spawning
    if shutdown.load(Ordering::Relaxed) {
        return Ok(true);
    }

    let mut envs = base_env.as_ref().clone();
    if let Some(additional_env) = &opts.env {
        envs.extend(additional_env.clone());
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

    let child = cmd.spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| std::io::Error::other("Could not get child pid"))?;

    // Check shutdown again before registering PID
    if shutdown.load(Ordering::Relaxed) {
        // Process was spawned but we're shutting down - kill it immediately
        let _ = std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status();
        return Ok(true);
    }

    active_pids.lock().unwrap().insert(pid);

    eprintln!("{} > Started (pid {})", status_prefix, pid);

    let handle_child_result = handle_child(status_prefix, prefix, &opts.datetime, child).await;

    // If PID was already removed from set, we're shutting down - don't restart
    if !active_pids.lock().unwrap().remove(&pid) {
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
    let config_parent_path = std::path::Path::new(config_path)
        .parent()
        .expect("Could not read the parent path of the config file")
        .to_string_lossy()
        .into_owned();

    let home_path = env::var("HOME").unwrap_or_default();

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
                Ok(p) => p
                    .into_os_string()
                    .into_string()
                    .expect("Could not parse current directory"),
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
    let runtime = builder
        .enable_io()
        .enable_time()
        .build()
        .expect("Could not build tokio runtime");

    let (active_pid_map, any_job_failed) = runtime.block_on(async move {
        let mut pid_map: HashMap<String, Arc<Mutex<HashSet<u32>>>> = HashMap::new();
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
                Entry::Vacant(v) => v.insert(Arc::new(Mutex::new(HashSet::new()))).clone(),
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

        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        let mut sighup = signal(SignalKind::hangup()).unwrap();
        let mut sigint = signal(SignalKind::interrupt()).unwrap();
        let mut sigquit = signal(SignalKind::quit()).unwrap();
        let mut sigusr1 = signal(SignalKind::user_defined1()).unwrap();
        let mut sigusr2 = signal(SignalKind::user_defined2()).unwrap();

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
                        let pid_set = pid_set_mutex.lock().unwrap();
                        if pid_set.is_empty() {
                            continue;
                        }

                        let mut cmd = if sudo_user.is_empty() {
                            let mut kill_cmd = std::process::Command::new("kill");
                            kill_cmd.arg(format!("-{}", forward_signal));
                            kill_cmd
                        } else {
                            let mut sudo_cmd = std::process::Command::new("sudo");
                            sudo_cmd.args(["-u", sudo_user, "kill", &format!("-{}", forward_signal)]);
                            sudo_cmd
                        };

                        for pid in pid_set.iter() {
                            cmd.arg(pid.to_string());
                        }

                        let _ = cmd
                            .stdout(Stdio::null())
                            .stderr(Stdio::null())
                            .status();
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
        shutdown.store(true, Ordering::Relaxed);

        let mut cloned_pid_map = HashMap::new();
        for (sudo_user, pid_set_mutex) in pid_map.into_iter() {
            let mut pid_set = pid_set_mutex.lock().unwrap();
            let cloned_pid_set = pid_set.clone();
            pid_set.clear();
            cloned_pid_map.insert(sudo_user, cloned_pid_set);
        }

        // Collect results from all job handles to check for failures
        let mut any_failed = false;
        for handle in job_handles {
            if let Ok(had_failure) = handle.await
                && had_failure
            {
                any_failed = true;
            }
        }

        (cloned_pid_map, any_failed)
    });

    runtime.shutdown_background();

    for (sudo_user, pid_set) in active_pid_map.into_iter() {
        if pid_set.is_empty() {
            continue;
        }

        let mut cmd = if sudo_user.is_empty() {
            let mut kill_cmd = std::process::Command::new("kill");
            kill_cmd.arg("-TERM");
            kill_cmd
        } else {
            let mut sudo_cmd = std::process::Command::new("sudo");
            sudo_cmd.args(["-u", &sudo_user, "kill", "-TERM"]);
            sudo_cmd
        };

        for pid in pid_set.into_iter() {
            cmd.arg(pid.to_string());
        }

        // We can't kill the whole process group by specifying -<pid>, because that
        // will also kill our current process, and the wrapper process.
        // Assume that children processes also handle killing their own
        // children correctly.
        let _ = cmd
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status();
    }

    if any_job_failed {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
