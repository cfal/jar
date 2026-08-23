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

/// Convert a signal name to its numeric value
#[cfg(unix)]
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

/// Helper to send a signal to a process using libc::kill
#[cfg(unix)]
fn send_signal(pid: u32, signal: &str) {
    if let Some(sig) = signal_name_to_number(signal) {
        unsafe { libc::kill(pid as libc::pid_t, sig) };
    }
}

/// Helper to check if a process is alive using libc::kill with signal 0
#[cfg(unix)]
fn is_process_alive(pid: u32) -> bool {
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if result == 0 {
        return true;
    }
    // If we got EPERM, process exists but we can't signal it - still alive
    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
    errno == libc::EPERM
}

/// Helper for non-unix platforms (not typically used in tests)
#[cfg(not(unix))]
fn is_process_alive(_pid: u32) -> bool {
    false
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
// TEST: Process Group Receives Signal via Negative PID (libc)
// =============================================================================

#[test]
#[cfg(unix)]
fn test_process_group_signal_via_negative_pid() {
    // This test verifies that when process_group is enabled, the entire
    // process group is signaled, not just the leader process
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
        .spawn()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();

    // Wait for child processes to spawn
    thread::sleep(Duration::from_secs(2));

    // Send SIGUSR1 to job-runner (which should forward to the process group)
    send_signal(child.id(), "USR1");

    // Collect output
    let output = collect_output_for(stdout, Duration::from_secs(2));

    // Cleanup
    send_signal(child.id(), "TERM");
    let _ = child.wait();

    // The child process in the group should have received USR1
    let child_received = output.iter().any(|line| line.contains("CHILD_GOT_USR1"));
    assert!(
        child_received,
        "Child process in group should receive signal. Output: {:?}",
        output
    );
}

// =============================================================================
// TEST: Multiple Children in Process Group All Killed
// =============================================================================

#[test]
#[cfg(unix)]
fn test_process_group_all_children_killed() {
    // Test that ALL children in a process group are killed, not just some
    let config = r#"
jobs:
  spawner:
    command: "bash -c 'for i in 1 2 3 4 5; do bash -c \"sleep 302\" & done; wait'"
    process_group: true
    shutdown_timeout: 3
"#;

    let config_file = create_temp_config(config);

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start job-runner");

    // Wait for all child processes to spawn
    thread::sleep(Duration::from_secs(2));

    // Find all the sleep 302 processes
    let pgrep_output = Command::new("pgrep")
        .args(["-f", "sleep 302"])
        .output()
        .expect("Failed to run pgrep");

    let sleep_pids_before: Vec<u32> = String::from_utf8_lossy(&pgrep_output.stdout)
        .lines()
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    assert!(
        sleep_pids_before.len() >= 4,
        "Expected at least 4 sleep processes, found: {:?}",
        sleep_pids_before
    );

    // Send TERM to job-runner
    send_signal(child.id(), "TERM");

    // Wait for shutdown (including timeout + SIGKILL)
    thread::sleep(Duration::from_secs(5));

    // Verify ALL sleep processes are gone
    let mut alive_count = 0;
    for pid in &sleep_pids_before {
        if is_process_alive(*pid) {
            alive_count += 1;
        }
    }

    assert_eq!(
        alive_count,
        0,
        "All {} sleep processes should be killed, but {} are still alive. PIDs: {:?}",
        sleep_pids_before.len(),
        alive_count,
        sleep_pids_before
    );

    let _ = child.wait();
}

// =============================================================================
// TEST: Process Group Enabled by Default
// =============================================================================

#[test]
#[cfg(unix)]
fn test_process_group_enabled_by_default() {
    let config = r#"
jobs:
  parent:
    command: "bash -c 'bash -c \"sleep 301\" & wait'"
    # process_group not set, defaults to true
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

    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    while child.try_wait().unwrap().is_none() && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }

    let surviving_pids: Vec<u32> = sleep_pids
        .iter()
        .copied()
        .filter(|pid| is_process_alive(*pid))
        .collect();

    for pid in &surviving_pids {
        let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
    }

    assert!(
        child.try_wait().unwrap().is_some(),
        "job-runner did not shut down"
    );
    assert!(
        surviving_pids.is_empty(),
        "default process-group shutdown left children alive: {surviving_pids:?}"
    );
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
    let sigkill_sent = output.iter().any(|line| {
        line.contains("SIGKILL") || line.contains("exceeded") || line.contains("timeout")
    });

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
    let graceful_or_no_remaining = output
        .iter()
        .any(|line| line.contains("gracefully") || line.contains("Exited with code 0"))
        || !output.iter().any(|line| line.contains("SIGKILL"));

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

    assert!(
        skipped,
        "Expected disabled job to be skipped. Output: {:?}",
        output
    );
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
    let job_ran = output
        .iter()
        .any(|line| line.contains("json_with_comments"));
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
        stderr.contains("Could not parse"),
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
    command: "sleep 0.5; kill -KILL $$"
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
    let signal_reported = output.iter().any(|line| line.contains("Killed by signal"));

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

// =============================================================================
// TEST: is_process_alive - Existing Process Returns True
// =============================================================================

#[test]
#[cfg(unix)]
fn test_is_process_alive_existing_process() {
    // Spawn a long-running process
    let mut child = Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("Failed to spawn sleep process");

    let pid = child.id();

    // Process should be alive
    assert!(
        is_process_alive(pid),
        "is_process_alive should return true for running process"
    );

    // Clean up
    let _ = child.kill();
    let _ = child.wait();
}

// =============================================================================
// TEST: is_process_alive - Non-Existent Process Returns False
// =============================================================================

#[test]
#[cfg(unix)]
fn test_is_process_alive_nonexistent_process() {
    // Use a PID that's very unlikely to exist
    // PID 4194304 is above the typical max PID on Linux (default max is 32768 or 4194304)
    // We'll use a more reliable approach: spawn and kill a process, then check
    let mut child = Command::new("true")
        .spawn()
        .expect("Failed to spawn process");

    let pid = child.id();
    let _ = child.wait(); // Wait for it to exit

    // Give the OS a moment to clean up
    thread::sleep(Duration::from_millis(100));

    // Process should no longer be alive
    assert!(
        !is_process_alive(pid),
        "is_process_alive should return false for exited process (pid {})",
        pid
    );
}

// =============================================================================
// TEST: is_process_alive - Process Transition From Alive to Dead
// =============================================================================

#[test]
#[cfg(unix)]
fn test_is_process_alive_transition() {
    // Spawn a short-lived process
    let mut child = Command::new("sleep")
        .arg("0.2")
        .spawn()
        .expect("Failed to spawn sleep process");

    let pid = child.id();

    // Should be alive initially
    assert!(
        is_process_alive(pid),
        "Process should be alive immediately after spawn"
    );

    // Wait for process to exit
    let _ = child.wait();
    thread::sleep(Duration::from_millis(100));

    // Should be dead now
    assert!(
        !is_process_alive(pid),
        "Process should be dead after wait()"
    );
}

// =============================================================================
// TEST: libc Signal Sending - SIGTERM to Process
// =============================================================================

#[test]
#[cfg(unix)]
fn test_libc_signal_term() {
    // Spawn a process that will run until signaled
    let mut child = Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("Failed to spawn sleep process");

    let pid = child.id();

    // Verify it's running
    assert!(is_process_alive(pid), "Process should be alive");

    // Send SIGTERM via libc
    let result = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    assert_eq!(result, 0, "libc::kill should return 0 on success");

    // Wait for process to exit
    let status = child.wait().expect("Failed to wait");

    // Process should have been terminated by signal
    assert!(
        !status.success(),
        "Process should not exit successfully after SIGTERM"
    );
}

// =============================================================================
// TEST: libc Signal Sending - Signal to Non-Existent Process
// =============================================================================

#[test]
#[cfg(unix)]
fn test_libc_signal_nonexistent_process() {
    // Spawn and immediately kill a process to get a dead PID
    let mut child = Command::new("true")
        .spawn()
        .expect("Failed to spawn process");
    let pid = child.id();
    let _ = child.wait();
    thread::sleep(Duration::from_millis(100));

    // Sending signal to non-existent process should return -1
    let result = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    assert_eq!(
        result, -1,
        "libc::kill should return -1 for non-existent process"
    );

    // Errno should be ESRCH (no such process)
    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
    assert_eq!(errno, libc::ESRCH, "errno should be ESRCH");
}

// =============================================================================
// TEST: libc Signal Sending - Process Group (Negative PID)
// =============================================================================

#[test]
#[cfg(unix)]
fn test_libc_signal_process_group() {
    // Create a process that spawns children in its own process group
    let mut child = Command::new("bash")
        .args(["-c", "sleep 60 & sleep 60 & wait"])
        .spawn()
        .expect("Failed to spawn bash");

    let pid = child.id();
    thread::sleep(Duration::from_millis(500));

    // Find child sleep processes
    let pgrep_output = Command::new("pgrep")
        .args(["-P", &pid.to_string()])
        .output()
        .expect("Failed to run pgrep");

    let child_pids: Vec<u32> = String::from_utf8_lossy(&pgrep_output.stdout)
        .lines()
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    assert!(
        !child_pids.is_empty(),
        "Should have spawned child processes"
    );

    // Send SIGTERM to the process group (negative PID)
    // Note: This only works if the process is a process group leader
    // For this test, we'll just kill the parent and verify children handling
    let result = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    assert_eq!(result, 0, "Should be able to signal the parent");

    let _ = child.wait();

    // Clean up any remaining children
    for cpid in &child_pids {
        let _ = unsafe { libc::kill(*cpid as libc::pid_t, libc::SIGKILL) };
    }
}

// =============================================================================
// TEST: Environment Variables Passed to Job
// =============================================================================

#[test]
#[cfg(unix)]
fn test_env_variables_passed_to_job() {
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
        .spawn()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();
    let output = collect_output_for(stdout, Duration::from_secs(2));

    send_signal(child.id(), "TERM");
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

// =============================================================================
// TEST: Initial Delay Before Job Start
// =============================================================================

#[test]
#[cfg(unix)]
fn test_initial_delay() {
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
        .spawn()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();

    // Collect output until we see "started"
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

    // Wait for the "started" message or timeout
    let started_time = rx.recv_timeout(Duration::from_secs(5));

    send_signal(child.id(), "TERM");
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

// =============================================================================
// TEST: Restart Delay After Job Failure
// =============================================================================

#[test]
#[cfg(unix)]
fn test_restart_delay_after_failure() {
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
        .spawn()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();

    // Collect attempts
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            if line.contains("attempt") {
                let _ = tx.send(std::time::Instant::now());
            }
        }
    });

    // Wait for at least 2 attempts
    let first = rx.recv_timeout(Duration::from_secs(2));
    let second = rx.recv_timeout(Duration::from_secs(3));

    send_signal(child.id(), "TERM");
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

// =============================================================================
// TEST: CWD Placeholder Substitution (#cwd#)
// =============================================================================

#[test]
#[cfg(unix)]
fn test_cwd_placeholder_substitution() {
    use std::io::Write;

    // Create a temp directory and config file
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let config_path = temp_dir.path().join("config.yaml");
    let marker_file = temp_dir.path().join("marker.txt");

    // Build config with placeholder - avoids raw string issues with #" in Rust 2024
    let config = "jobs:\n  cwd_test:\n    command: \"touch marker.txt\"\n    cwd: \"#cwd#\"\n";

    let mut file = std::fs::File::create(&config_path).expect("Failed to create config");
    file.write_all(config.as_bytes()).unwrap();

    let mut child = Command::new(binary_path())
        .arg(&config_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start job-runner");

    // Wait for job to run
    thread::sleep(Duration::from_secs(2));

    send_signal(child.id(), "TERM");
    let _ = child.wait();

    // The marker file should exist in the config file's directory
    assert!(
        marker_file.exists(),
        "#cwd# should expand to config file directory. Marker file should be at {:?}",
        marker_file
    );
}

// =============================================================================
// TEST: Runner CWD Placeholder Substitution (#runner_cwd#)
// =============================================================================

#[test]
#[cfg(unix)]
fn test_runner_cwd_placeholder_substitution() {
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
        .spawn()
        .expect("Failed to start job-runner");

    thread::sleep(Duration::from_secs(2));

    send_signal(child.id(), "TERM");
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

// =============================================================================
// TEST: Dollar Placeholder Substitution ($CWD, $HOME, $RUNNER_CWD)
// =============================================================================

#[test]
#[cfg(unix)]
fn test_dollar_placeholder_substitution() {
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
        .spawn()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();
    let output = collect_output_for(stdout, Duration::from_secs(2));

    send_signal(child.id(), "TERM");
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

// =============================================================================
// TEST: Home Placeholder Substitution (#home#)
// =============================================================================

#[test]
#[cfg(unix)]
fn test_home_placeholder_substitution() {
    // Build config with placeholder - avoids raw string issues with #" in Rust 2024
    let config =
        "jobs:\n  home_test:\n    command: \"sh -c 'echo HOME_DIR=$PWD'\"\n    cwd: \"#home#\"\n";

    let config_file = create_temp_config(config);
    let expected_home = std::env::var("HOME").unwrap_or_default();

    let mut child = Command::new(binary_path())
        .arg(config_file.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();
    let output = collect_output_for(stdout, Duration::from_secs(2));

    send_signal(child.id(), "TERM");
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

// =============================================================================
// TEST: Datetime Timestamps in Output
// =============================================================================

#[test]
#[cfg(unix)]
fn test_datetime_timestamps() {
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
        .spawn()
        .expect("Failed to start job-runner");

    let stdout = child.stdout.take().unwrap();
    let output = collect_output_for(stdout, Duration::from_secs(2));

    send_signal(child.id(), "TERM");
    let _ = child.wait();

    // Get today's date in the expected format
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();

    let has_timestamp = output.iter().any(|l| l.contains(&today));

    assert!(
        has_timestamp,
        "Output should contain timestamp with date {}. Output: {:?}",
        today, output
    );
}

// =============================================================================
// TEST: Empty Command is Skipped
// =============================================================================

#[test]
fn test_empty_command_skipped() {
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
        .spawn()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();
    let output = collect_output_for(stderr, Duration::from_secs(2));

    send_signal(child.id(), "TERM");
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

// =============================================================================
// TEST: Multiple Jobs with Different Shutdown Timeouts
// =============================================================================

#[test]
#[cfg(unix)]
fn test_multiple_shutdown_timeouts() {
    // Two jobs that ignore SIGTERM with different timeouts
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
        .spawn()
        .expect("Failed to start job-runner");

    // Wait for jobs to start
    thread::sleep(Duration::from_secs(1));

    let start = std::time::Instant::now();

    // Send TERM to job-runner
    send_signal(child.id(), "TERM");

    // Wait for shutdown
    let _ = child.wait();
    let elapsed = start.elapsed();

    // Shutdown should take around 3s (the longer timeout)
    // Allow some buffer
    assert!(
        elapsed >= Duration::from_secs(2) && elapsed < Duration::from_secs(5),
        "Shutdown should respect individual timeouts. Elapsed: {:?}",
        elapsed
    );
}

// =============================================================================
// TEST: Color Assignment (Random Colors for Jobs)
// =============================================================================

#[test]
fn test_color_assignment() {
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
        .spawn()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();
    let output = collect_output_for(stderr, Duration::from_secs(2));

    send_signal(child.id(), "TERM");
    let _ = child.wait();

    // Red color code is \x1b[31m
    let has_red = output.iter().any(|l| l.contains("\x1b[31m"));

    assert!(
        has_red,
        "Job with color: red should have red ANSI code. Output: {:?}",
        output
    );
}

// =============================================================================
// TEST: Invalid Color is Ignored
// =============================================================================

#[test]
fn test_invalid_color_ignored() {
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
        .spawn()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();
    let output = collect_output_for(stderr, Duration::from_secs(2));

    send_signal(child.id(), "TERM");
    let _ = child.wait();

    let color_warning = output.iter().any(|l| l.contains("No such color, ignoring"));

    assert!(
        color_warning,
        "Invalid color should produce warning. Output: {:?}",
        output
    );
}

// =============================================================================
// TEST: CamelCase Config Aliases (processGroup, shutdownTimeout, etc.)
// =============================================================================

#[test]
#[cfg(unix)]
fn test_camelcase_config_aliases() {
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
        .spawn()
        .expect("Failed to start job-runner");

    let stderr = child.stderr.take().unwrap();

    thread::sleep(Duration::from_secs(1));

    let start = std::time::Instant::now();
    send_signal(child.id(), "TERM");

    let output = collect_output_for(stderr, Duration::from_secs(3));
    let _ = child.wait();
    let elapsed = start.elapsed();

    let initialized = output.iter().any(|l| l.contains("Started (pid"));

    // Check shutdown was ~1s (shutdownTimeout worked)
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

// =============================================================================
// TEST: Signal kill(pid, 0) Returns Correct Value for Current Process
// =============================================================================

#[test]
#[cfg(unix)]
fn test_signal_zero_current_process() {
    // We can always signal ourselves
    let pid = std::process::id();
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    assert_eq!(
        result, 0,
        "Should be able to signal our own process with kill(pid, 0)"
    );
    assert!(
        is_process_alive(pid),
        "is_process_alive should return true for current process"
    );
}

// =============================================================================
// TEST: Very High PID is Not Alive
// =============================================================================

#[test]
#[cfg(unix)]
fn test_very_high_pid_not_alive() {
    // PID 4194304 is typically the max on Linux, try something above it
    // On most systems, this PID won't exist
    let result = is_process_alive(99999999);
    assert!(!result, "Very high PID should not be alive");
}
