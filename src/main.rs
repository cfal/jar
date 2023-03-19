use async_recursion::async_recursion;
use rand::Rng;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio::time::sleep;

#[derive(Debug, Deserialize)]
struct JobConfig {
    #[serde(default)]
    disabled: bool,
    #[serde(default)]
    color: String,
    command: String,
    #[serde(default = "default_cwd")]
    cwd: String,
    #[serde(default)]
    delay: u64,
    env: Option<HashMap<String, String>>,
    #[serde(default = "default_restart_delay")]
    restart_delay: u64,
}

fn default_cwd() -> String {
    match env::current_dir() {
        Ok(p) => p
            .into_os_string()
            .into_string()
            .expect("Could not read current directory"),
        Err(e) => {
            eprintln!("Could not read current directory: {}", e);
            env::var("HOME").expect("Could not read current directory or home directory")
        }
    }
}

fn default_restart_delay() -> u64 {
    15_000
}

#[derive(Debug, Deserialize)]
struct Config {
    jobs: HashMap<String, JobConfig>,
}

#[async_recursion]
async fn start_proc(name: String, opts: Arc<JobConfig>, delay: Duration) {
    sleep(delay).await;

    let restart_delay = Duration::from_millis(opts.restart_delay);

    let mut envs = env::vars().collect::<HashMap<String, String>>();
    if let Some(additional_env) = &opts.env {
        envs.extend(additional_env.clone());
    }

    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg(format!("exec {}", opts.command))
        .current_dir(opts.cwd.clone())
        .envs(&envs)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            eprintln!(
                "{} > * > Failed to start, retrying in {:?}: {}",
                &name, restart_delay, e
            );
            start_proc(name, opts, restart_delay).await;
            return;
        }
    };

    eprintln!("{} > * > Started (pid {})", &name, child.id().unwrap());

    let stdout_handle = spawn_output_handler(
        child.stdout.take().unwrap(),
        opts.color.clone(),
        name.clone(),
        "stdout",
    );
    let stderr_handle = spawn_output_handler(
        child.stderr.take().unwrap(),
        opts.color.clone(),
        name.clone(),
        "stderr",
    );

    let status = child.wait().await.unwrap();
    stdout_handle.await.unwrap();
    stderr_handle.await.unwrap();

    eprintln!(
        "{} > * > Exited with code {}",
        &name,
        status.code().unwrap_or(-1)
    );

    eprintln!("{} > * > Restarting in {:?}", &name, restart_delay);

    start_proc(name, opts, restart_delay).await;
}

fn spawn_output_handler<R: 'static + AsyncRead + std::marker::Send + Unpin>(
    reader: R,
    color: String,
    tag: String,
    msg_type: &'static str,
) -> JoinHandle<std::io::Result<()>> {
    let prefix = color_func(&color, &tag);
    tokio::spawn(async move {
        let reader = BufReader::new(reader);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            eprintln!("{} > {} > {}", prefix, msg_type, line);
        }

        Ok(())
    })
}

fn color_func(color: &str, tag: &str) -> String {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config_path = match env::args().nth(1) {
        Some(path) => path,
        None => {
            eprintln!("Usage: job_runner config.json");
            std::process::exit(1);
        }
    };

    let config_parent_path = std::path::Path::new(&config_path)
        .parent()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let home_path = env::var("HOME")?;

    let config_data = std::fs::read_to_string(config_path)?;
    let mut config: Config = serde_json::from_str(&config_data)?;

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
        job_config.cwd = job_config
            .cwd
            .replace("#cwd#", &config_parent_path)
            .replace("#home#", &home_path);

        job_config.command = job_config
            .command
            .replace("#cwd#", &config_parent_path)
            .replace("#home#", &home_path);
    }

    let mut handles = Vec::new();
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
        let delay = Duration::from_millis(job_config.delay);
        let handle = tokio::spawn(start_proc(name, Arc::new(job_config), delay));
        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    Ok(())
}
