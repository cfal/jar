use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::env;
use std::process::Stdio;
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
}

fn default_restart_delay() -> u64 {
    15_000
}

#[derive(Debug, Deserialize)]
struct Config {
    jobs: HashMap<String, JobConfig>,
}

async fn start_proc(prefix: String, opts: JobConfig, active_pids: Arc<Mutex<HashSet<u32>>>) {
    let mut initial = true;
    loop {
        let delay = Duration::from_millis(if initial {
            initial = false;
            opts.delay
        } else {
            opts.restart_delay
        });

        if !delay.is_zero() {
            eprintln!("{} > * > Starting in {}s", prefix, delay.as_secs());
            sleep(delay).await;
        }

        match start_proc_internal(&prefix, &opts, &active_pids).await {
            Ok(true) => break,
            Ok(false) => (),
            Err(e) => {
                eprintln!("{} > * > Process error: {}", prefix, e);
            }
        }
    }
    eprintln!("{} > * > Shutting down..", prefix);
}

async fn start_proc_internal(
    prefix: &str,
    opts: &JobConfig,
    active_pids: &Arc<Mutex<HashSet<u32>>>,
) -> std::io::Result<bool> {
    let mut envs = env::vars().collect::<HashMap<String, String>>();
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
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "Could not get child pid"))?;

    active_pids.lock().unwrap().insert(pid);

    eprintln!("{} > * > Started (pid {})", prefix, pid);

    let handle_child_result = handle_child(prefix, child).await;

    // This should never be true because we're single threaded, but if we were to
    // switch to a multi-threaded executor, a cleared set means that we are shutting down,
    // and we can use it as a signal not to restart.
    if !active_pids.lock().unwrap().remove(&pid) {
        Ok(true)
    } else {
        handle_child_result?;
        Ok(false)
    }
}

async fn handle_child(prefix: &str, mut child: Child) -> std::io::Result<()> {
    let stdout_handle = spawn_output_handler(
        child.stdout.take().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "Could not get stdout")
        })?,
        prefix.to_string(),
        "stdout",
    );
    let stderr_handle = spawn_output_handler(
        child.stderr.take().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "Could not get stderr")
        })?,
        prefix.to_string(),
        "stderr",
    );
    let status = child.wait().await?;
    stdout_handle.await??;
    stderr_handle.await??;

    eprintln!(
        "{} > * > Exited with code {}",
        prefix,
        status
            .code()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "No status code"))?,
    );

    Ok(())
}

fn spawn_output_handler<R: 'static + AsyncRead + Send + Unpin>(
    reader: R,
    prefix: String,
    msg_type: &'static str,
) -> JoinHandle<std::io::Result<()>> {
    tokio::spawn(async move {
        let reader = BufReader::new(reader);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            eprintln!("{} > {} > {}", prefix, msg_type, line);
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

fn main() -> std::io::Result<()> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() < 2 {
        eprintln!("Usage: {} [--wrap] config.json", args[0]);
        std::process::exit(1);
    }

    if args[1] == "--wrap" {
        if args.len() < 3 {
            eprintln!("No config file provided.");
            std::process::exit(1);
        }
        loop {
            let updated_args = args[2..]
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            eprintln!("[Wrapper] Starting inner process..");
            match std::process::Command::new(&args[0])
                .args(updated_args)
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

    let home_path = env::var("HOME").expect("Could not read $HOME");

    let config_data = std::fs::read_to_string(config_path)?;

    let mut config: Config = match serde_yaml::from_str(&config_data) {
        Ok(c) => c,
        Err(_) => {
            let config_data = config_data
                .split('\n')
                .filter(|s| !s.trim_start().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n");
            serde_json::from_str(&config_data)?
        }
    };

    let print_colors = [
        "red", "green", "yellow", "blue", "magenta", "cyan", "white", "gray",
    ];

    let mut rng = rand::thread_rng();
    let mut next_random_color_index = rng.gen_range(0..print_colors.len());

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

    let mut builder = Builder::new_current_thread();
    let runtime = builder
        .enable_io()
        .enable_time()
        .build()
        .expect("Could not build tokio runtime");

    let active_pid_map = runtime.block_on(async move {
        // another option is to pass a different variable to keep track
        // of the pid for each task - for example, HashMap<String, Arc<AtomicU32>>,
        // where the key would be the name of the job and everyone gets a different
        // Arc<AtomicU32>. Could possibly reduce contention if there are very frequent
        // restarts.
        let mut pid_map: HashMap<String, Arc<Mutex<HashSet<u32>>>> = HashMap::new();
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
            tokio::spawn(start_proc(prefix, job_config, pid_set));
        }

        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        let mut sighup = signal(SignalKind::hangup()).unwrap();
        let mut sigint = signal(SignalKind::interrupt()).unwrap();
        let mut sigquit = signal(SignalKind::quit()).unwrap();

        let signal_name = tokio::select! {
            _ = sigterm.recv() => "TERM",
            _ = sighup.recv() => "HUP",
            _ = sigint.recv() => "INT",
            _ = sigquit.recv() => "QUIT",
        };

        // Print an extra newline incase we have a ^C prefix.
        eprintln!(
            "\nReceived {} signal, killing active processes..",
            signal_name
        );

        let mut cloned_pid_map = HashMap::new();
        for (sudo_user, pid_set_mutex) in pid_map.into_iter() {
            let mut pid_set = pid_set_mutex.lock().unwrap();
            let cloned_pid_set = pid_set.clone();
            pid_set.clear();
            cloned_pid_map.insert(sudo_user, cloned_pid_set);
        }

        cloned_pid_map
    });

    runtime.shutdown_background();

    for (sudo_user, pid_set) in active_pid_map.into_iter() {
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

    Ok(())
}
