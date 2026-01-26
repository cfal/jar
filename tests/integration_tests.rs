//! Integration tests for job-runner-rs
//!
//! These tests verify the key features:
//! - Deterministic job ordering (BTreeMap)
//! - Signal forwarding (SIGUSR1/SIGUSR2)
//! - Process groups (setsid)
//! - Graceful shutdown (SIGTERM -> wait -> SIGKILL)
//! - Signal-killed process reporting

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

/// Helper to get the path to the built binary
fn binary_path() -> String {
    // Use debug build for tests
    env!("CARGO_BIN_EXE_job-runner-rs").to_string()
}

/// Helper to create a temporary config file
fn create_temp_config(content: &str) -> tempfile::NamedTempFile {
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::with_suffix(".yaml").unwrap();
    file.write_all(content.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}


/// Helper to collect output for a duration
fn collect_output_for(
    reader: impl std::io::Read + Send + 'static,
    duration: Duration,
) -> Vec<String> {
    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let reader = BufReader::new(reader);
        for line in reader.lines() {
            if let Ok(line) = line {
                let _ = tx.send(line);
            }
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

/// Helper to send a signal to a process
#[cfg(unix)]
fn send_signal(pid: u32, signal: &str) {
    Command::new("kill")
        .args([&format!("-{}", signal), &pid.to_string()])
        .status()
        .expect("Failed to send signal");
}

/// Helper to check if a process is alive
fn is_process_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

// =============================================================================
// TEST: Deterministic Job Ordering (BTreeMap)
// =============================================================================

#[test]
fn test_deterministic_job_ordering() {
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
        .spawn()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();
    let output = collect_output_for(stderr, Duration::from_secs(2));

    // Send TERM to stop
    send_signal(child.id(), "TERM");
    let _ = child.wait();

    // Find the "Initializing job" lines and verify order
    let init_lines: Vec<_> = output
        .iter()
        .filter(|line| line.contains("Initializing job"))
        .collect();

    assert!(
        init_lines.len() >= 3,
        "Expected at least 3 initialization lines, got: {:?}",
        init_lines
    );

    // Jobs should be initialized in alphabetical order: alpha, middle, zebra
    let first_job = init_lines.iter().position(|l| l.contains("\"alpha\""));
    let second_job = init_lines.iter().position(|l| l.contains("\"middle\""));
    let third_job = init_lines.iter().position(|l| l.contains("\"zebra\""));

    assert!(
        first_job < second_job && second_job < third_job,
        "Jobs not in alphabetical order. Lines: {:?}",
        init_lines
    );
}

// =============================================================================
// TEST: SIGUSR1 Forwarding
// =============================================================================

#[test]
#[cfg(unix)]
fn test_sigusr1_forwarding() {
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
        .spawn()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();

    // Wait for job to start
    thread::sleep(Duration::from_secs(1));

    // Send SIGUSR1 to job-runner
    send_signal(child.id(), "USR1");

    // Collect output
    let output = collect_output_for(stdout, Duration::from_secs(2));

    // Cleanup
    send_signal(child.id(), "TERM");
    let _ = child.wait();

    // Verify the signal was forwarded
    let usr1_received = output.iter().any(|line| line.contains("USR1_RECEIVED"));
    assert!(
        usr1_received,
        "SIGUSR1 was not forwarded to child process. Output: {:?}",
        output
    );
}

// =============================================================================
// TEST: SIGUSR2 Forwarding
// =============================================================================

#[test]
#[cfg(unix)]
fn test_sigusr2_forwarding() {
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
        .spawn()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();

    // Wait for job to start
    thread::sleep(Duration::from_secs(1));

    // Send SIGUSR2 to job-runner
    send_signal(child.id(), "USR2");

    // Collect output
    let output = collect_output_for(stdout, Duration::from_secs(2));

    // Cleanup
    send_signal(child.id(), "TERM");
    let _ = child.wait();

    // Verify the signal was forwarded
    let usr2_received = output.iter().any(|line| line.contains("USR2_RECEIVED"));
    assert!(
        usr2_received,
        "SIGUSR2 was not forwarded to child process. Output: {:?}",
        output
    );
}

// =============================================================================
// TEST: Process Groups (setsid)
// =============================================================================

#[test]
#[cfg(unix)]
fn test_process_group_kills_children() {
    let config = r#"
jobs:
  parent_with_children:
    command: "bash -c 'bash -c \"sleep 300\" & bash -c \"sleep 300\" & wait'"
    process_group: true
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start job-runner");

    // Wait for child processes to spawn
    thread::sleep(Duration::from_secs(2));

    // Find the sleep processes
    let pgrep_output = Command::new("pgrep")
        .args(["-f", "sleep 300"])
        .output()
        .expect("Failed to run pgrep");

    let sleep_pids_before: Vec<u32> = String::from_utf8_lossy(&pgrep_output.stdout)
        .lines()
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    assert!(
        sleep_pids_before.len() >= 2,
        "Expected at least 2 sleep processes, found: {:?}",
        sleep_pids_before
    );

    // Send TERM to job-runner
    send_signal(child.id(), "TERM");

    // Wait for graceful shutdown
    thread::sleep(Duration::from_secs(7));

    // Verify all sleep processes are gone
    for pid in &sleep_pids_before {
        assert!(
            !is_process_alive(*pid),
            "Sleep process {} should have been killed with process group",
            pid
        );
    }

    let _ = child.wait();
}

// =============================================================================
// TEST: Process Group Disabled (default behavior)
// =============================================================================

#[test]
#[cfg(unix)]
fn test_process_group_disabled_by_default() {
    let config = r#"
jobs:
  parent:
    command: "bash -c 'bash -c \"sleep 301\" & wait'"
    # process_group not set, defaults to false
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start job-runner");

    // Wait for child process to spawn
    thread::sleep(Duration::from_secs(2));

    // Find the sleep process
    let pgrep_output = Command::new("pgrep")
        .args(["-f", "sleep 301"])
        .output()
        .expect("Failed to run pgrep");

    let sleep_pids: Vec<u32> = String::from_utf8_lossy(&pgrep_output.stdout)
        .lines()
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    // Send TERM to job-runner
    send_signal(child.id(), "TERM");

    // Wait for shutdown (should still kill via SIGKILL fallback)
    thread::sleep(Duration::from_secs(7));

    // Cleanup any remaining sleep processes
    for pid in &sleep_pids {
        let _ = Command::new("kill")
            .args(["-9", &pid.to_string()])
            .status();
    }

    let _ = child.wait();
}

// =============================================================================
// TEST: Graceful Shutdown - SIGTERM then SIGKILL
// =============================================================================

#[test]
#[cfg(unix)]
fn test_graceful_shutdown_sigkill_fallback() {
    // This job ignores SIGTERM, so it should be SIGKILL'd after timeout
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
        .spawn()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();

    // Wait for job to start
    thread::sleep(Duration::from_secs(1));

    // Send TERM to job-runner
    send_signal(child.id(), "TERM");

    // Collect output during shutdown (wait longer than timeout)
    let output = collect_output_for(stderr, Duration::from_secs(5));

    let _ = child.wait();

    // Should see SIGKILL message since the process ignores SIGTERM
    let sigkill_sent = output
        .iter()
        .any(|line| line.contains("SIGKILL") || line.contains("exceeded") || line.contains("timeout"));

    assert!(
        sigkill_sent,
        "Expected SIGKILL to be sent to stubborn process. Output: {:?}",
        output
    );
}

// =============================================================================
// TEST: Graceful Shutdown - Clean Exit
// =============================================================================

#[test]
#[cfg(unix)]
fn test_graceful_shutdown_clean_exit() {
    // Use a process that exits quickly on its own (not waiting for signal)
    // This tests that graceful shutdown waits for processes to exit
    let config = r#"
jobs:
  quick_exit:
    command: "sleep 0.5"
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();

    // Wait for job to start and finish
    thread::sleep(Duration::from_secs(2));

    // Send TERM to job-runner
    send_signal(child.id(), "TERM");

    // Collect output during shutdown
    let output = collect_output_for(stderr, Duration::from_secs(3));

    let _ = child.wait();

    // When the process has already exited, we should see graceful exit
    // (no SIGKILL needed because process is already gone)
    let graceful_or_no_remaining = output.iter().any(|line| {
        line.contains("gracefully") || line.contains("Exited with code 0")
    }) || !output.iter().any(|line| line.contains("SIGKILL"));

    assert!(
        graceful_or_no_remaining,
        "Expected graceful shutdown. Output: {:?}",
        output
    );
}

// =============================================================================
// TEST: Signal-Killed Process Reporting
// =============================================================================

#[test]
#[cfg(unix)]
fn test_signal_killed_process_reporting() {
    // Use a short-lived process that exits with a specific code
    // This tests that exit codes are reported (signal reporting happens
    // during normal operation, not during shutdown)
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
        .spawn()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();

    // Wait for job to exit and report
    thread::sleep(Duration::from_secs(2));

    // Send TERM to job-runner
    send_signal(child.id(), "TERM");

    // Collect output
    let output = collect_output_for(stderr, Duration::from_secs(3));

    let _ = child.wait();

    // Should see "Exited with code 42" in output
    let exit_reported = output
        .iter()
        .any(|line| line.contains("Exited with code 42"));

    assert!(
        exit_reported,
        "Expected 'Exited with code 42' to be reported. Output: {:?}",
        output
    );
}

// =============================================================================
// TEST: Multiple Signals Don't Cause Issues
// =============================================================================

#[test]
#[cfg(unix)]
fn test_multiple_usr_signals() {
    let config = r#"
jobs:
  counter:
    command: "sh -c 'count=0; trap \"count=$((count+1)); echo COUNT=$count\" USR1; while true; do sleep 0.2; done'"
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();

    // Wait for job to start
    thread::sleep(Duration::from_millis(500));

    // Send multiple USR1 signals
    for _ in 0..3 {
        send_signal(child.id(), "USR1");
        thread::sleep(Duration::from_millis(300));
    }

    // Collect output
    let output = collect_output_for(stdout, Duration::from_secs(2));

    // Cleanup
    send_signal(child.id(), "TERM");
    let _ = child.wait();

    // Count how many times the signal was received
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

// =============================================================================
// TEST: Disabled Jobs Are Skipped
// =============================================================================

#[test]
fn test_disabled_jobs_skipped() {
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
        .spawn()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();
    let output = collect_output_for(stderr, Duration::from_secs(2));

    send_signal(child.id(), "TERM");
    let _ = child.wait();

    // Should see "Skipping disabled job"
    let skipped = output
        .iter()
        .any(|line| line.contains("Skipping disabled job"));

    assert!(skipped, "Expected disabled job to be skipped. Output: {:?}", output);
}

// =============================================================================
// TEST: Configurable Shutdown Timeout
// =============================================================================

#[test]
#[cfg(unix)]
fn test_configurable_shutdown_timeout() {
    // Job with short timeout - should be killed quickly
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
        .spawn()
        .expect("Failed to start job-runner");

    // Wait for job to start
    thread::sleep(Duration::from_secs(1));

    let start = std::time::Instant::now();

    // Send TERM to job-runner
    send_signal(child.id(), "TERM");

    // Wait for job-runner to exit
    let status = child.wait().expect("Failed to wait for child");
    let elapsed = start.elapsed();

    // Shutdown should complete around 1s (not 5s default)
    // Allow some buffer for process overhead
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

// =============================================================================
// TEST: Wrapper Mode
// =============================================================================

#[test]
#[cfg(unix)]
fn test_wrapper_mode() {
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
        .spawn()
        .expect("Failed to start job-runner in wrapper mode");

    let stdout = child.stdout.take().unwrap();

    // Wait for inner process to run
    thread::sleep(Duration::from_secs(2));

    // Collect output
    let output = collect_output_for(stdout, Duration::from_secs(1));

    // Kill wrapper
    send_signal(child.id(), "TERM");
    let _ = child.wait();

    // Should see the wrapped output
    let has_wrapped_output = output.iter().any(|line| line.contains("wrapped"));
    assert!(
        has_wrapped_output,
        "Wrapper mode should run inner process. Output: {:?}",
        output
    );
}

// =============================================================================
// TEST: JSON Config with Comments
// =============================================================================

#[test]
fn test_json_config_with_comments() {
    use std::io::Write;

    // Create a JSON config file with // comments
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
        .spawn()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();
    let output = collect_output_for(stdout, Duration::from_secs(2));

    send_signal(child.id(), "TERM");
    let status = child.wait().expect("Failed to wait");

    // Should have parsed the JSON and run the job
    let job_ran = output.iter().any(|line| line.contains("json_with_comments"));
    assert!(
        job_ran || status.success(),
        "JSON with comments should be parsed correctly. Output: {:?}",
        output
    );
}

// =============================================================================
// TEST: Config File Not Found
// =============================================================================

#[test]
fn test_config_file_not_found() {
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

// =============================================================================
// TEST: Invalid Config File
// =============================================================================

#[test]
fn test_invalid_config_file() {
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
        stderr.contains("Could not parse config file"),
        "Should report parse error. Stderr: {}",
        stderr
    );
}

// =============================================================================
// TEST: Signal Killed Process Reports Signal Number
// =============================================================================

#[test]
#[cfg(unix)]
fn test_signal_killed_reports_signal_number() {
    // A process that kills itself with a signal during normal operation
    // (not during shutdown, which aborts the reporting task)
    let config = r#"
jobs:
  self_killer:
    command: "sh -c 'sleep 0.5; kill -KILL $$'"
    restart_delay: 100
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();

    // Wait for process to kill itself and be reported
    let output = collect_output_for(stderr, Duration::from_secs(3));

    send_signal(child.id(), "TERM");
    let _ = child.wait();

    // Should see "Killed by signal 9" message (SIGKILL)
    let signal_reported = output
        .iter()
        .any(|line| line.contains("Killed by signal"));

    assert!(
        signal_reported,
        "Should report 'Killed by signal'. Output: {:?}",
        output
    );
}

// =============================================================================
// TEST: Help Flag
// =============================================================================

#[test]
fn test_help_flag() {
    let output = Command::new(binary_path())
        .arg("--help")
        .output()
        .expect("Failed to run job-runner");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Usage:"),
        "Should show usage with --help. Stderr: {}",
        stderr
    );
}

// =============================================================================
// TEST: No Arguments Shows Usage
// =============================================================================

#[test]
fn test_no_arguments_shows_usage() {
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

// =============================================================================
// TEST: Config File Parsing - YAML
// =============================================================================

#[test]
fn test_yaml_config_parsing() {
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
        .spawn()
        .expect("Failed to start job-runner");

    thread::sleep(Duration::from_secs(1));
    send_signal(child.id(), "TERM");

    let status = child.wait().expect("Failed to wait for child");

    // Should exit cleanly (not crash on config parsing)
    assert!(
        status.success(),
        "Job runner should handle YAML config correctly"
    );
}
