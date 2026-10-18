use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

mod support;
use support::SpawnGuarded;

#[derive(Clone, Copy, Debug)]
struct TestProcess {
    pid: u32,
    start_time: u64,
}

struct TestProcessCleanup(Vec<TestProcess>);

impl Drop for TestProcessCleanup {
    fn drop(&mut self) {
        for process in &self.0 {
            if is_process_alive(*process) {
                unsafe { libc::kill(process.pid as libc::pid_t, libc::SIGKILL) };
            }
        }
    }
}

fn binary_path() -> String {
    env!("CARGO_BIN_EXE_jr").to_string()
}

fn create_temp_config(content: &str) -> tempfile::NamedTempFile {
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::with_suffix(".yaml").unwrap();
    file.write_all(content.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn collect_output_for(
    reader: impl std::io::Read + Send + 'static,
    duration: Duration,
) -> Vec<String> {
    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let reader = BufReader::new(reader);
        for line in reader.lines().map_while(Result::ok) {
            let _ = tx.send(line);
        }
    });

    let mut lines = Vec::new();
    let deadline = std::time::Instant::now() + duration;

    while std::time::Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(line) => lines.push(line),
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    lines
}

fn send_signal(pid: u32, signal: libc::c_int) {
    unsafe { libc::kill(pid as libc::pid_t, signal) };
}

fn read_process(pid: u32) -> Option<(char, u64)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let mut fields = stat[stat.rfind(')')? + 1..].split_whitespace();
    let state = fields.next()?.chars().next()?;
    let start_time = fields.nth(18)?.parse().ok()?;
    Some((state, start_time))
}

fn is_process_alive(process: TestProcess) -> bool {
    read_process(process.pid)
        .is_some_and(|(state, start_time)| start_time == process.start_time && state != 'Z')
}

fn read_test_processes(path: &std::path::Path) -> Vec<TestProcess> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.trim().parse::<u32>().ok())
        .filter_map(|pid| read_process(pid).map(|(_, start_time)| TestProcess { pid, start_time }))
        .collect()
}

fn wait_until(duration: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + duration;
    while std::time::Instant::now() < deadline {
        if condition() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    condition()
}

#[test]
fn invalid_cli_arguments_report_specific_errors_without_starting_jobs() {
    for (args, diagnostic) in [
        (vec!["-t"], "-t requires a number argument"),
        (vec!["-t", "0"], "-t requires a positive integer"),
        (vec!["-t", "-1"], "-t requires a positive integer"),
        (vec!["-t", "not-a-number"], "-t requires a positive integer"),
        (
            vec!["-t", "18446744073709551616"],
            "-t requires a positive integer",
        ),
        (vec!["--unknown"], "Unknown option: --unknown"),
        (vec!["--wrap"], "No config file provided"),
        (vec!["-t", "2"], "No config file provided"),
    ] {
        let output = Command::new(binary_path()).args(&args).output().unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert_eq!(output.status.code(), Some(1), "{args:?}: {stderr}");
        assert!(stderr.contains(diagnostic), "{args:?}: {stderr}");
        assert!(!stderr.contains("Initializing job"));
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn deterministic_job_ordering() {
    let config = r#"
jobs:
  zebra:
    command: "echo zebra"
    delay: 100
  alpha:
    command: "echo alpha"
    delay: 100
  middle:
    command: "echo middle"
    delay: 100
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();
    let output = collect_output_for(stderr, Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    let init_lines: Vec<_> = output
        .iter()
        .filter(|line| line.contains("Initializing job"))
        .collect();

    assert!(
        init_lines.len() >= 3,
        "Expected at least 3 initialization lines, got: {:?}",
        init_lines
    );

    let first_job = init_lines.iter().position(|l| l.contains("\"alpha\""));
    let second_job = init_lines.iter().position(|l| l.contains("\"middle\""));
    let third_job = init_lines.iter().position(|l| l.contains("\"zebra\""));

    assert!(
        first_job < second_job && second_job < third_job,
        "Jobs not in alphabetical order. Lines: {:?}",
        init_lines
    );
}

#[test]
fn sigusr1_forwarding() {
    let config = r#"
jobs:
  signal_handler:
    command: "sh -c 'trap \"echo USR1_RECEIVED\" USR1; while true; do sleep 0.5; done'"
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();

    thread::sleep(Duration::from_secs(1));

    send_signal(child.id(), libc::SIGUSR1);

    let output = collect_output_for(stdout, Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    let usr1_received = output.iter().any(|line| line.contains("USR1_RECEIVED"));
    assert!(
        usr1_received,
        "SIGUSR1 was not forwarded to child process. Output: {:?}",
        output
    );
}

#[test]
fn sigusr2_forwarding() {
    let config = r#"
jobs:
  signal_handler:
    command: "sh -c 'trap \"echo USR2_RECEIVED\" USR2; while true; do sleep 0.5; done'"
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();

    thread::sleep(Duration::from_secs(1));

    send_signal(child.id(), libc::SIGUSR2);

    let output = collect_output_for(stdout, Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    let usr2_received = output.iter().any(|line| line.contains("USR2_RECEIVED"));
    assert!(
        usr2_received,
        "SIGUSR2 was not forwarded to child process. Output: {:?}",
        output
    );
}

#[test]
fn process_group_kills_children() {
    let temp = tempfile::tempdir().unwrap();
    let pid_file = temp.path().join("children.pids");
    let config = format!(
        "jobs:\n  parent_with_children:\n    command: |\n      sleep 300 &\n      echo $! >> {0}\n      sleep 300 &\n      echo $! >> {0}\n      wait\n    process_group: true\n",
        pid_file.display()
    );
    let config_file = create_temp_config(&config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    assert!(
        wait_until(Duration::from_secs(3), || read_test_processes(&pid_file)
            .len()
            == 2),
        "expected two recorded child processes"
    );
    let sleep_processes = read_test_processes(&pid_file);
    let _cleanup = TestProcessCleanup(sleep_processes.clone());

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    for process in sleep_processes {
        assert!(
            !is_process_alive(process),
            "Sleep process {} should have been killed with process group",
            process.pid
        );
    }
}

#[test]
fn process_group_signal_via_negative_pid() {
    let config = r#"
jobs:
  group_leader:
    command: "bash -c 'bash -c \"trap \\\"echo CHILD_GOT_USR1\\\" USR1; while true; do sleep 0.2; done\" & wait'"
    process_group: true
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();

    thread::sleep(Duration::from_secs(2));

    send_signal(child.id(), libc::SIGUSR1);

    let output = collect_output_for(stdout, Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    let child_received = output.iter().any(|line| line.contains("CHILD_GOT_USR1"));
    assert!(
        child_received,
        "Child process in group should receive signal. Output: {:?}",
        output
    );
}

#[test]
fn process_group_all_children_killed() {
    let temp = tempfile::tempdir().unwrap();
    let pid_file = temp.path().join("children.pids");
    let config = format!(
        "jobs:\n  spawner:\n    command: |\n      for i in 1 2 3 4 5; do\n        sleep 302 &\n        echo $! >> {0}\n      done\n      wait\n    process_group: true\n    shutdown_timeout: 3\n",
        pid_file.display()
    );
    let config_file = create_temp_config(&config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    assert!(
        wait_until(Duration::from_secs(3), || read_test_processes(&pid_file)
            .len()
            == 5),
        "expected five recorded child processes"
    );
    let sleep_processes = read_test_processes(&pid_file);
    let _cleanup = TestProcessCleanup(sleep_processes.clone());

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    let surviving: Vec<_> = sleep_processes
        .iter()
        .filter(|process| is_process_alive(**process))
        .collect();

    assert!(
        surviving.is_empty(),
        "recorded child processes remained alive: {surviving:?}"
    );
}

#[test]
fn process_group_enabled_by_default() {
    let temp = tempfile::tempdir().unwrap();
    let pid_file = temp.path().join("child.pid");
    let config = format!(
        "jobs:\n  parent:\n    command: |\n      sleep 301 &\n      echo $! > {}\n      wait\n",
        pid_file.display()
    );
    let config_file = create_temp_config(&config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    assert!(wait_until(Duration::from_secs(3), || {
        read_test_processes(&pid_file).len() == 1
    }));
    let sleep_processes = read_test_processes(&pid_file);
    let _cleanup = TestProcessCleanup(sleep_processes.clone());

    send_signal(child.id(), libc::SIGTERM);

    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    while child.try_wait().unwrap().is_none() && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }

    let surviving: Vec<_> = sleep_processes
        .iter()
        .filter(|process| is_process_alive(**process))
        .collect();

    assert!(
        child.try_wait().unwrap().is_some(),
        "job-runner did not shut down"
    );
    assert!(
        surviving.is_empty(),
        "default process-group shutdown left children alive: {surviving:?}"
    );
}

#[test]
fn graceful_shutdown_sigkill_fallback() {
    let config = r#"
jobs:
  stubborn:
    command: "sh -c 'trap \"\" TERM; while true; do sleep 0.5; done'"
    shutdown_timeout: 2
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();

    thread::sleep(Duration::from_secs(1));

    send_signal(child.id(), libc::SIGTERM);

    let output = collect_output_for(stderr, Duration::from_secs(5));

    let _ = child.wait();

    let sigkill_sent = output.iter().any(|line| {
        line.contains("SIGKILL") || line.contains("exceeded") || line.contains("timeout")
    });

    assert!(
        sigkill_sent,
        "Expected SIGKILL to be sent to stubborn process. Output: {:?}",
        output
    );
}

#[test]
fn graceful_shutdown_clean_exit() {
    let temp = tempfile::tempdir().unwrap();
    let ready = temp.path().join("ready");
    let graceful = temp.path().join("graceful");
    let config = format!(
        r#"
jobs:
  active:
    command: |
      trap 'touch {}; exit 0' TERM
      touch {}
      while :; do sleep 1; done
    restart: false
"#,
        graceful.display(),
        ready.display()
    );

    let config_file = create_temp_config(&config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();

    assert!(wait_until(Duration::from_secs(5), || ready.exists()));

    send_signal(child.id(), libc::SIGTERM);

    let output = collect_output_for(stderr, Duration::from_secs(3));

    assert!(child.wait().unwrap().success(), "{output:?}");
    assert!(graceful.exists(), "TERM trap did not run: {output:?}");
    assert!(
        output
            .iter()
            .any(|line| line.contains("Received TERM signal")),
        "{output:?}"
    );
    assert!(
        !output
            .iter()
            .any(|line| line.contains("Killed by signal 9")),
        "{output:?}"
    );
}

#[test]
fn signal_killed_process_reporting() {
    let config = r#"
jobs:
  exiter:
    command: "sh -c 'sleep 0.5; exit 42'"
    restart_delay: 100
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();

    thread::sleep(Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);

    let output = collect_output_for(stderr, Duration::from_secs(3));

    let _ = child.wait();

    let exit_reported = output
        .iter()
        .any(|line| line.contains("Exited with code 42"));

    assert!(
        exit_reported,
        "Expected 'Exited with code 42' to be reported. Output: {:?}",
        output
    );
}

#[test]
fn multiple_usr_signals() {
    let config = r#"
jobs:
  counter:
    command: |
      count=0
      trap 'count=$((count+1)); echo COUNT=$count' USR1
      while true; do sleep 0.2; done
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();

    thread::sleep(Duration::from_millis(500));

    for _ in 0..3 {
        send_signal(child.id(), libc::SIGUSR1);
        thread::sleep(Duration::from_millis(300));
    }

    let output = collect_output_for(stdout, Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    let count_lines: Vec<_> = output
        .iter()
        .filter(|line| line.contains("COUNT="))
        .collect();

    assert!(
        count_lines.len() >= 2,
        "Expected at least 2 signal receipts, got: {:?}",
        count_lines
    );
}

#[test]
fn disabled_jobs_skipped() {
    let config = r#"
jobs:
  enabled_job:
    command: "echo enabled"
  disabled_job:
    command: "echo disabled"
    disabled: true
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();
    let output = collect_output_for(stderr, Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    let skipped = output
        .iter()
        .any(|line| line.contains("Skipping disabled job"));

    assert!(
        skipped,
        "Expected disabled job to be skipped. Output: {:?}",
        output
    );
}

#[test]
fn configurable_shutdown_timeout() {
    let config = r#"
jobs:
  quick_kill:
    command: "sh -c 'trap \"\" TERM; while true; do sleep 0.1; done'"
    shutdown_timeout: 1
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    thread::sleep(Duration::from_secs(1));

    let start = std::time::Instant::now();

    send_signal(child.id(), libc::SIGTERM);

    let status = child.wait().expect("Failed to wait for child");
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(3),
        "Shutdown should respect the 1s timeout. Elapsed: {:?}",
        elapsed
    );

    assert!(
        elapsed >= Duration::from_millis(900),
        "Shutdown should have waited at least ~1s. Elapsed: {:?}",
        elapsed
    );

    assert!(status.success(), "Job-runner should exit successfully");
}

#[test]
fn wrapper_mode() {
    let config = r#"
jobs:
  quick:
    command: "sh -c 'echo wrapped; exit 0'"
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .args(["--wrap", config_file.path().to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner in wrapper mode");

    let stdout = child.stdout.take().unwrap();

    thread::sleep(Duration::from_secs(2));

    let output = collect_output_for(stdout, Duration::from_secs(1));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    let has_wrapped_output = output.iter().any(|line| line.contains("wrapped"));
    assert!(
        has_wrapped_output,
        "Wrapper mode should run inner process. Output: {:?}",
        output
    );
}

#[test]
fn json_config_with_comments() {
    use std::io::Write;

    let config = r#"{
  // This is a comment
  "jobs": {
    "test_job": {
      "command": "echo json_with_comments"
    }
  }
}
"#;

    let mut file = tempfile::NamedTempFile::with_suffix(".json").unwrap();
    file.write_all(config.as_bytes()).unwrap();
    file.flush().unwrap();

    let mut child = Command::new(binary_path())
        .arg(file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();
    let output = collect_output_for(stdout, Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let status = child.wait().expect("Failed to wait");

    let job_ran = output
        .iter()
        .any(|line| line.contains("json_with_comments"));
    assert!(
        job_ran && status.success(),
        "JSON with comments should be parsed correctly. Output: {:?}",
        output
    );
}

#[test]
fn config_file_not_found() {
    let output = Command::new(binary_path())
        .arg("/nonexistent/path/config.yaml")
        .output()
        .expect("Failed to run job-runner");

    assert!(
        !output.status.success(),
        "Should exit with error for missing config"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Could not read config file"),
        "Should report config read error. Stderr: {}",
        stderr
    );
}

#[test]
fn invalid_config_file() {
    use std::io::Write;

    let mut file = tempfile::NamedTempFile::with_suffix(".yaml").unwrap();
    file.write_all(b"this is not valid yaml: [[[").unwrap();
    file.flush().unwrap();

    let output = Command::new(binary_path())
        .arg(file.path())
        .output()
        .expect("Failed to run job-runner");

    assert!(
        !output.status.success(),
        "Should exit with error for invalid config"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Could not parse"),
        "Should report parse error. Stderr: {}",
        stderr
    );
}

#[test]
fn signal_killed_reports_signal_number() {
    let config = r#"
jobs:
  self_killer:
    command: "sleep 0.5; kill -KILL $$"
    restart_delay: 100
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();

    let output = collect_output_for(stderr, Duration::from_secs(3));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    let signal_reported = output.iter().any(|line| line.contains("Killed by signal"));

    assert!(
        signal_reported,
        "Should report 'Killed by signal'. Output: {:?}",
        output
    );
}

#[test]
fn help_flag() {
    assert_eq!(env!("CARGO_PKG_NAME"), "jar");
    assert_eq!(
        std::path::Path::new(&binary_path()).file_name().unwrap(),
        "jr"
    );
    let output = Command::new(binary_path())
        .arg("--help")
        .output()
        .expect("Failed to run job-runner");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&format!("Usage: {} [--wrap]", binary_path())),
        "Should show usage with --help. Stderr: {}",
        stderr
    );
}

#[test]
fn no_arguments_shows_usage() {
    let output = Command::new(binary_path())
        .output()
        .expect("Failed to run job-runner");

    assert!(!output.status.success(), "Should exit with error");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Usage:"),
        "Should show usage with no args. Stderr: {}",
        stderr
    );
}

#[test]
fn yaml_config_parsing() {
    let config = r#"
jobs:
  test_job:
    command: "echo hello"
    color: green
    delay: 100
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    thread::sleep(Duration::from_secs(1));
    send_signal(child.id(), libc::SIGTERM);

    let status = child.wait().expect("Failed to wait for child");
    let output = collect_output_for(child.stdout.take().unwrap(), Duration::from_secs(1));
    assert!(
        output.iter().any(|line| line.ends_with("hello")),
        "{output:?}"
    );

    assert!(
        status.success(),
        "Job runner should handle YAML config correctly"
    );
}

#[test]
fn env_variables_passed_to_job() {
    let config = r#"
jobs:
  env_test:
    command: "sh -c 'echo MY_VAR=$MY_VAR; echo OTHER_VAR=$OTHER_VAR'"
    env:
      MY_VAR: "hello_world"
      OTHER_VAR: "test_value"
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();
    let output = collect_output_for(stdout, Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    let has_my_var = output.iter().any(|l| l.contains("MY_VAR=hello_world"));
    let has_other_var = output.iter().any(|l| l.contains("OTHER_VAR=test_value"));

    assert!(
        has_my_var,
        "MY_VAR should be passed to job. Output: {:?}",
        output
    );
    assert!(
        has_other_var,
        "OTHER_VAR should be passed to job. Output: {:?}",
        output
    );
}

#[test]
fn initial_delay() {
    let config = r#"
jobs:
  delayed_job:
    command: "echo started"
    delay: 1500
"#;

    let config_file = create_temp_config(config);

    let start = std::time::Instant::now();

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();

    let (tx, rx) = mpsc::channel();
    let reader_thread = thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            if line.contains("started") {
                let _ = tx.send(std::time::Instant::now());
                break;
            }
        }
    });

    let started_time = rx.recv_timeout(Duration::from_secs(5));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();
    let _ = reader_thread.join();

    if let Ok(time) = started_time {
        let elapsed = time - start;
        assert!(
            elapsed >= Duration::from_millis(1400),
            "Job should have waited ~1.5s before starting. Elapsed: {:?}",
            elapsed
        );
    } else {
        panic!("Job never printed 'started'");
    }
}

#[test]
fn restart_delay_after_failure() {
    let config = r#"
jobs:
  failing_job:
    command: "sh -c 'echo attempt; exit 1'"
    restart_delay: 1000
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();

    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            if line.contains("attempt") {
                let _ = tx.send(std::time::Instant::now());
            }
        }
    });

    let first = rx.recv_timeout(Duration::from_secs(2));
    let second = rx.recv_timeout(Duration::from_secs(3));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    if let (Ok(t1), Ok(t2)) = (first, second) {
        let gap = t2 - t1;
        assert!(
            gap >= Duration::from_millis(900),
            "Restart delay should be ~1s. Gap: {:?}",
            gap
        );
    } else {
        panic!("Did not see two restart attempts");
    }
}

#[test]
fn cwd_placeholder_substitution() {
    use std::io::Write;

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("config.yaml");
    let marker_file = temp_dir.path().join("marker.txt");

    let config = "jobs:\n  cwd_test:\n    command: \"touch marker.txt\"\n    cwd: \"#cwd#\"\n";

    let mut file = std::fs::File::create(&config_path).expect("Failed to create config");
    file.write_all(config.as_bytes()).unwrap();

    let mut child = Command::new(binary_path())
        .arg(&config_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    thread::sleep(Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    assert!(
        marker_file.exists(),
        "#cwd# should expand to config file directory. Marker file should be at {:?}",
        marker_file
    );
}

#[test]
fn runner_cwd_placeholder_substitution() {
    use std::io::Write;

    let runner_dir = tempfile::tempdir().expect("Failed to create runner dir");
    let config_dir = tempfile::tempdir().expect("Failed to create config dir");
    let config_path = config_dir.path().join("config.yaml");
    let runner_marker = runner_dir.path().join("runner-marker.txt");
    let config_marker = config_dir.path().join("runner-marker.txt");

    let config = "jobs:\n  runner_cwd_test:\n    command: \"touch runner-marker.txt\"\n    cwd: \"#runner_cwd#\"\n";

    let mut file = std::fs::File::create(&config_path).expect("Failed to create config");
    file.write_all(config.as_bytes()).unwrap();

    let mut child = Command::new(binary_path())
        .arg(&config_path)
        .current_dir(runner_dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    thread::sleep(Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    assert!(
        runner_marker.exists(),
        "#runner_cwd# should expand to the directory where job-runner was started. Marker file should be at {:?}",
        runner_marker
    );
    assert!(
        !config_marker.exists(),
        "#runner_cwd# should not expand to the config file directory. Unexpected marker file at {:?}",
        config_marker
    );
}

#[test]
fn dollar_placeholder_substitution() {
    use std::io::Write;

    let runner_dir = tempfile::tempdir().expect("Failed to create runner dir");
    let config_dir = tempfile::tempdir().expect("Failed to create config dir");
    let config_path = config_dir.path().join("config.yaml");
    let config_marker = config_dir.path().join("dollar-cwd-marker.txt");
    let runner_marker = runner_dir.path().join("dollar-cwd-marker.txt");
    let expected_home = std::env::var("HOME").unwrap_or_default();
    let expected_runner_cwd = runner_dir.path().to_string_lossy().into_owned();

    let config = "jobs:\n  dollar_test:\n    command: \"sh -c 'touch dollar-cwd-marker.txt; echo HOME_DIR=$HOME; echo RUNNER_DIR=$RUNNER_CWD'\"\n    cwd: \"$CWD\"\n";

    let mut file = std::fs::File::create(&config_path).expect("Failed to create config");
    file.write_all(config.as_bytes()).unwrap();

    let mut child = Command::new(binary_path())
        .arg(&config_path)
        .current_dir(runner_dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();
    let output = collect_output_for(stdout, Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    assert!(
        config_marker.exists(),
        "$CWD should expand to config file directory. Marker file should be at {:?}",
        config_marker
    );
    assert!(
        !runner_marker.exists(),
        "$CWD should not expand to the runner startup directory. Unexpected marker file at {:?}",
        runner_marker
    );
    assert!(
        output
            .iter()
            .any(|l| l.contains(&format!("HOME_DIR={}", expected_home))),
        "$HOME should expand in command. Output: {:?}",
        output
    );
    assert!(
        output
            .iter()
            .any(|l| l.contains(&format!("RUNNER_DIR={}", expected_runner_cwd))),
        "$RUNNER_CWD should expand in command. Output: {:?}",
        output
    );
}

#[test]
fn dollar_cwd_prefix_substitution() {
    use std::io::Write;

    let home_dir = tempfile::tempdir().expect("Failed to create home dir");
    let runner_dir = tempfile::tempdir().expect("Failed to create runner dir");
    let config_dir = tempfile::tempdir().expect("Failed to create config dir");
    let config_path = config_dir.path().join("config.yaml");
    for root in [home_dir.path(), runner_dir.path(), config_dir.path()] {
        std::fs::create_dir(root.join("work")).unwrap();
    }

    let config = r#"jobs:
  home:
    command: "touch home-marker"
    cwd: "$HOME/work"
  config:
    command: "touch config-marker"
    cwd: "$CWD/work"
  runner:
    command: "touch runner-marker"
    cwd: "$RUNNER_CWD/work"
"#;
    let mut file = std::fs::File::create(&config_path).expect("Failed to create config");
    file.write_all(config.as_bytes()).unwrap();

    let mut child = Command::new(binary_path())
        .arg(&config_path)
        .current_dir(runner_dir.path())
        .env("HOME", home_dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    thread::sleep(Duration::from_secs(2));
    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    assert!(home_dir.path().join("work/home-marker").exists());
    assert!(config_dir.path().join("work/config-marker").exists());
    assert!(runner_dir.path().join("work/runner-marker").exists());
}

#[test]
fn home_placeholder_substitution() {
    let config =
        "jobs:\n  home_test:\n    command: \"sh -c 'echo HOME_DIR=$PWD'\"\n    cwd: \"#home#\"\n";

    let config_file = create_temp_config(config);
    let expected_home = std::env::var("HOME").unwrap_or_default();

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();
    let output = collect_output_for(stdout, Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    let home_output = output
        .iter()
        .find(|l| l.contains("HOME_DIR="))
        .map(|l| l.to_string())
        .unwrap_or_default();

    assert!(
        home_output.contains(&expected_home),
        "#home# should expand to $HOME ({}). Output: {:?}",
        expected_home,
        output
    );
}

#[test]
fn datetime_timestamps() {
    let config = r#"
jobs:
  timestamp_job:
    command: "echo hello"
    datetime:
      enabled: true
      format: "%Y-%m-%d"
      local: true
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();
    let output = collect_output_for(stdout, Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    let today = chrono::Local::now().format("%Y-%m-%d").to_string();

    let has_timestamp = output.iter().any(|l| l.contains(&today));

    assert!(
        has_timestamp,
        "Output should contain timestamp with date {}. Output: {:?}",
        today, output
    );
}

#[test]
fn empty_command_skipped() {
    let config = r#"
jobs:
  empty_cmd:
    command: ""
  valid_cmd:
    command: "echo valid"
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();
    let output = collect_output_for(stderr, Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    let skipped = output
        .iter()
        .any(|l| l.contains("Skipping job with missing command"));

    assert!(
        skipped,
        "Empty command job should be skipped. Output: {:?}",
        output
    );
}

#[test]
fn multiple_shutdown_timeouts() {
    let config = r#"
jobs:
  fast_kill:
    command: "sh -c 'trap \"\" TERM; while true; do sleep 0.1; done'"
    shutdown_timeout: 1
  slow_kill:
    command: "sh -c 'trap \"\" TERM; while true; do sleep 0.1; done'"
    shutdown_timeout: 3
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    thread::sleep(Duration::from_secs(1));

    let start = std::time::Instant::now();

    send_signal(child.id(), libc::SIGTERM);

    let _ = child.wait();
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_secs(2) && elapsed < Duration::from_secs(5),
        "Shutdown should respect individual timeouts. Elapsed: {:?}",
        elapsed
    );
}

#[test]
fn color_assignment() {
    let config = r#"
jobs:
  job_with_color:
    command: "echo colored"
    color: red
  job_without_color:
    command: "echo default"
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();
    let output = collect_output_for(stderr, Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    let has_red = output.iter().any(|l| l.contains("\x1b[31m"));

    assert!(
        has_red,
        "Job with color: red should have red ANSI code. Output: {:?}",
        output
    );
}

#[test]
fn invalid_color_ignored() {
    let config = r#"
jobs:
  bad_color:
    command: "echo test"
    color: not_a_color
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();
    let output = collect_output_for(stderr, Duration::from_secs(2));

    send_signal(child.id(), libc::SIGTERM);
    let _ = child.wait();

    let color_warning = output.iter().any(|l| l.contains("No such color, ignoring"));

    assert!(
        color_warning,
        "Invalid color should produce warning. Output: {:?}",
        output
    );
}

#[test]
fn camelcase_config_aliases() {
    let config = r#"
jobs:
  camel_test:
    command: "sh -c 'trap \"\" TERM; while true; do sleep 1; done'"
    processGroup: true
    shutdownTimeout: 1
    restartDelay: 500
    sudoUser: null
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_guarded()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();

    thread::sleep(Duration::from_secs(1));

    let start = std::time::Instant::now();
    send_signal(child.id(), libc::SIGTERM);

    let output = collect_output_for(stderr, Duration::from_secs(3));
    let _ = child.wait();
    let elapsed = start.elapsed();

    let initialized = output.iter().any(|l| l.contains("Started (pid"));

    assert!(
        elapsed < Duration::from_secs(3),
        "shutdownTimeout should be respected. Elapsed: {:?}",
        elapsed
    );

    assert!(
        initialized,
        "camelCase aliases should work. Output: {:?}",
        output
    );
}
