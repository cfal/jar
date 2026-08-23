#![cfg(unix)]

use std::fs::{self, File};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(20);
static CONTAINER_SEQUENCE: AtomicUsize = AtomicUsize::new(0);
static RESOURCE_INTENSIVE_TEST: Mutex<()> = Mutex::new(());

fn serialize_resource_intensive_test() -> MutexGuard<'static, ()> {
    RESOURCE_INTENSIVE_TEST
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

struct Runner {
    child: Child,
    log_path: PathBuf,
}

impl Runner {
    fn start(config: &Path, log_path: PathBuf, extra_args: &[&str]) -> Self {
        let log = File::create(&log_path).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_job-runner-rs"));
        command
            .args(extra_args)
            .arg(config)
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        Self {
            child: command.spawn().unwrap(),
            log_path,
        }
    }

    fn start_with_unread_stdout(config: &Path, log_path: PathBuf) -> Self {
        let log = File::create(&log_path).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_job-runner-rs"));
        command.arg(config).stdout(Stdio::piped()).stderr(log);
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        Self {
            child: command.spawn().unwrap(),
            log_path,
        }
    }

    fn start_with_unread_stderr(config: &Path, log_path: PathBuf) -> Self {
        let log = File::create(&log_path).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_job-runner-rs"));
        command.arg(config).stdout(log).stderr(Stdio::piped());
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        Self {
            child: command.spawn().unwrap(),
            log_path,
        }
    }

    fn start_with_path(config: &Path, log_path: PathBuf, path: &str) -> Self {
        let log = File::create(&log_path).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_job-runner-rs"));
        command
            .arg(config)
            .env("PATH", path)
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        Self {
            child: command.spawn().unwrap(),
            log_path,
        }
    }

    fn signal(&self, signal: libc::c_int) {
        let result = unsafe { libc::kill(self.child.id() as libc::pid_t, signal) };
        assert_eq!(result, 0, "failed to signal runner: {}", self.log());
    }

    fn is_running(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }

    fn wait(&mut self, duration: Duration) -> ExitStatus {
        let deadline = Instant::now() + duration;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "runner did not exit within {duration:?}: {}",
                self.log()
            );
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn log(&self) -> String {
        fs::read_to_string(&self.log_path).unwrap_or_default()
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            unsafe {
                libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL);
                libc::kill(-(self.child.id() as libc::pid_t), libc::SIGKILL);
            }
            let _ = self.child.wait();
        }
    }
}

struct ProcessCleanup(Vec<PathBuf>);

impl Drop for ProcessCleanup {
    fn drop(&mut self) {
        for pid in read_pids(&self.0) {
            unsafe {
                libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
                libc::kill(pid as libc::pid_t, libc::SIGKILL);
            }
        }
    }
}

struct SudoProcessCleanup {
    username: String,
    pid_path: PathBuf,
}

impl Drop for SudoProcessCleanup {
    fn drop(&mut self) {
        for pid in read_pids(std::slice::from_ref(&self.pid_path)) {
            let process_group = process_group_id(pid)
                .map(|group| format!("-{group}"))
                .unwrap_or_else(|| pid.to_string());
            let pid = pid.to_string();
            let _ = Command::new("sudo")
                .args([
                    "-n",
                    "-u",
                    &self.username,
                    "--",
                    "kill",
                    "-KILL",
                    "--",
                    &process_group,
                    &pid,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn wait_until(duration: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        thread::sleep(POLL_INTERVAL);
    }
    condition()
}

fn read_pids(paths: &[PathBuf]) -> Vec<u32> {
    paths
        .iter()
        .filter_map(|path| fs::read_to_string(path).ok())
        .flat_map(|contents| {
            contents
                .lines()
                .filter_map(|line| line.trim().parse::<u32>().ok())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn process_is_running(pid: u32) -> bool {
    let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    let Some(close) = stat.rfind(')') else {
        return false;
    };
    stat[close + 1..]
        .split_whitespace()
        .next()
        .is_some_and(|state| state != "Z")
}

fn process_group_id(pid: u32) -> Option<u32> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat[stat.rfind(')')? + 1..]
        .split_whitespace()
        .nth(2)?
        .parse()
        .ok()
}

fn resident_set_kib(pid: u32) -> Option<u64> {
    fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

fn current_username() -> Option<String> {
    let output = Command::new("id").arg("-un").output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn sudo_available_for(username: &str) -> bool {
    Command::new("sudo")
        .args(["-n", "-u", username, "--", "true"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn install_failing_signal_sudo(root: &Path) -> String {
    let sudo_output = Command::new("which").arg("sudo").output().unwrap();
    assert!(sudo_output.status.success());
    let real_sudo = String::from_utf8_lossy(&sudo_output.stdout)
        .trim()
        .to_string();
    let fake_bin = root.join("bin");
    fs::create_dir(&fake_bin).unwrap();
    write_executable(
        &fake_bin.join("sudo"),
        &format!(
            "#!/bin/sh\nfor argument in \"$@\"; do\n  [ \"$argument\" = job-runner-signal ] && exit 77\ndone\nexec {} \"$@\"\n",
            real_sudo
        ),
    );
    format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    )
}

fn exercise_sudo_signal_route(username: &str) {
    let temp = tempfile::tempdir().unwrap();
    let pid_file = temp.path().join("job.pid");
    let ready = temp.path().join("ready");
    let received = temp.path().join("received");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    for path in [&pid_file, &ready, &received] {
        File::create(path).unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o666);
        fs::set_permissions(path, permissions).unwrap();
    }
    let mut permissions = fs::metadata(temp.path()).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(temp.path(), permissions).unwrap();
    fs::write(
        &config,
        format!(
            "jobs:\n  sudo_signal:\n    command: |\n      echo $$ > {}\n      trap 'echo $$ >> {}' USR1\n      echo ready > {}\n      while :; do sleep 1; done\n    sudo_user: {}\n    shutdown_timeout: 0\n",
            pid_file.display(),
            received.display(),
            ready.display(),
            username
        ),
    )
    .unwrap();
    let _cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: pid_file.clone(),
    };

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(5), || {
        fs::metadata(&ready).is_ok_and(|metadata| metadata.len() > 0)
            || fs::read_to_string(&pid_file).is_ok_and(|contents| !contents.trim().is_empty())
    }));
    let process_tree = Command::new("pstree")
        .args(["-ap", &runner.child.id().to_string()])
        .output()
        .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
        .unwrap_or_default();
    runner.signal(libc::SIGUSR1);
    assert!(wait_until(Duration::from_secs(2), || {
        fs::read_to_string(&received)
            .map(|contents| !contents.is_empty())
            .unwrap_or(false)
    }));
    thread::sleep(Duration::from_millis(200));
    let received_contents = fs::read_to_string(&received).unwrap();
    assert_eq!(
        received_contents.lines().count(),
        1,
        "sudo route delivered USR1 more than once; received={received_contents:?}; tree={process_tree}; log={}",
        runner.log(),
    );
    let pid = read_pids(std::slice::from_ref(&pid_file))[0];
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
    assert!(!process_is_running(pid));
}

struct ContainerCleanup(String);

impl Drop for ContainerCleanup {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.0])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn docker_available() -> bool {
    Command::new("docker")
        .args(["image", "inspect", "ubuntu:24.04"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn container_exists(name: &str) -> bool {
    Command::new("docker")
        .args(["inspect", name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn unique_container_name(label: &str) -> String {
    format!(
        "job-runner-test-{label}-{}-{}",
        std::process::id(),
        CONTAINER_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

#[test]
fn shell_compound_commands_are_not_truncated_by_exec() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    fs::write(
        &config,
        "jobs:\n  compound:\n    command: \"echo compound-first && echo compound-second\"\n    restart_delay: 10000\n",
    )
    .unwrap();

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(3), || runner
        .log()
        .contains("compound-second")));
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(3)).success());
}

#[test]
fn very_short_jobs_restart_reliably() {
    let temp = tempfile::tempdir().unwrap();
    let runs = temp.path().join("runs");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    fs::write(
        &config,
        format!(
            "jobs:\n  short:\n    command: \"echo run >> {}\"\n    restart_delay: 0\n",
            runs.display()
        ),
    )
    .unwrap();

    let started = Instant::now();
    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(4), || {
        fs::read_to_string(&runs)
            .map(|contents| contents.lines().count() >= 12)
            .unwrap_or(false)
    }));
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "zero restart delay bypassed the restart floor"
    );
    assert!(runner.is_running(), "short job stopped supervisor");
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(4)).success());
}

#[test]
fn job_group_signal_cannot_kill_the_supervisor() {
    let temp = tempfile::tempdir().unwrap();
    let runs = temp.path().join("runs");
    let script = temp.path().join("self-kill.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    write_executable(
        &script,
        &format!("#!/bin/sh\necho $$ >> {}\nkill -KILL 0\n", runs.display()),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  self_kill:\n    command: \"{}\"\n    restart_delay: 100\n",
            script.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![runs.clone()]);

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(3), || {
        fs::read_to_string(&runs)
            .map(|contents| contents.lines().count() >= 2)
            .unwrap_or(false)
    }));
    assert!(
        runner.is_running(),
        "job killed supervisor: {}",
        runner.log()
    );
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(3)).success());
}

#[test]
fn shutdown_kills_deep_and_new_session_descendants() {
    let temp = tempfile::tempdir().unwrap();
    let root_pid = temp.path().join("root.pid");
    let child_pid = temp.path().join("child.pid");
    let grandchild_pid = temp.path().join("grandchild.pid");
    let root_script = temp.path().join("root.sh");
    let child_script = temp.path().join("child.sh");
    let grandchild_script = temp.path().join("grandchild.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");

    write_executable(
        &grandchild_script,
        &format!(
            "#!/bin/bash\necho $$ > {}\ntrap '' TERM\nwhile :; do sleep 1; done\n",
            grandchild_pid.display()
        ),
    );
    write_executable(
        &child_script,
        &format!(
            "#!/bin/bash\necho $$ > {}\nsetsid {} &\ntrap '' TERM\nwait\n",
            child_pid.display(),
            grandchild_script.display()
        ),
    );
    write_executable(
        &root_script,
        &format!(
            "#!/bin/bash\necho $$ > {}\n{} &\ntrap 'exit 0' TERM\nwait\n",
            root_pid.display(),
            child_script.display()
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  tree:\n    command: \"{}\"\n    shutdown_timeout: 1\n",
            root_script.display()
        ),
    )
    .unwrap();
    let pid_files = vec![root_pid, child_pid, grandchild_pid];
    let _cleanup = ProcessCleanup(pid_files.clone());

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(3), || read_pids(&pid_files)
        .len()
        == 3));
    let pids = read_pids(&pid_files);
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
    assert!(
        pids.iter().all(|pid| !process_is_running(*pid)),
        "descendants survived: {:?}; {}",
        pids.iter()
            .copied()
            .filter(|pid| process_is_running(*pid))
            .collect::<Vec<_>>(),
        runner.log()
    );
}

#[test]
fn inherited_output_pipe_does_not_block_restart() {
    let temp = tempfile::tempdir().unwrap();
    let runs = temp.path().join("runs");
    let holders = temp.path().join("holders");
    let script = temp.path().join("pipe-holder.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    write_executable(
        &script,
        &format!(
            "#!/bin/bash\necho run >> {}\nsleep 30 &\necho $! >> {}\nsleep 0.2\nexit 0\n",
            runs.display(),
            holders.display()
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  pipe_holder:\n    command: \"{}\"\n    restart_delay: 100\n",
            script.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![holders.clone()]);

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(4), || {
        fs::read_to_string(&runs)
            .map(|contents| contents.lines().count() >= 2)
            .unwrap_or(false)
    }));
    let holder_pids = read_pids(&[holders]);
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(4)).success());
    assert!(
        wait_until(Duration::from_secs(1), || holder_pids
            .iter()
            .all(|pid| !process_is_running(*pid))),
        "pipe holders survived: {:?}; {}",
        holder_pids
            .iter()
            .copied()
            .filter(|pid| process_is_running(*pid))
            .collect::<Vec<_>>(),
        runner.log()
    );
}

#[test]
fn natural_leader_exit_kills_residual_process_group_before_restart() {
    let temp = tempfile::tempdir().unwrap();
    let children = temp.path().join("children");
    let script = temp.path().join("leave-child.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    write_executable(
        &script,
        &format!(
            "#!/bin/bash\nsleep 30 &\necho $! >> {}\nsleep 0.2\nexit 0\n",
            children.display()
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  residual:\n    command: \"{}\"\n    restart_delay: 100\n",
            script.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![children.clone()]);

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(4), || {
        read_pids(std::slice::from_ref(&children)).len() >= 2
    }));
    let child_pids = read_pids(&[children]);
    assert!(
        !process_is_running(child_pids[0]),
        "first run's child survived its leader and restart: {}",
        runner.log()
    );
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(4)).success());
    assert!(wait_until(Duration::from_secs(1), || child_pids
        .iter()
        .all(|pid| !process_is_running(*pid))));
}

#[test]
fn natural_leader_exit_kills_observed_new_session_descendants() {
    let temp = tempfile::tempdir().unwrap();
    let descendants = temp.path().join("descendants");
    let descendant_script = temp.path().join("descendant.sh");
    let leader_script = temp.path().join("leader.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    write_executable(
        &descendant_script,
        &format!(
            "#!/bin/bash\necho $$ >> {}\nwhile :; do sleep 1; done\n",
            descendants.display()
        ),
    );
    write_executable(
        &leader_script,
        &format!(
            "#!/bin/bash\nsetsid {} &\nwhile [ ! -s {} ]; do sleep 0.01; done\nsleep 0.2\nexit 0\n",
            descendant_script.display(),
            descendants.display()
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  escaped:\n    command: \"{}\"\n    restart_delay: 100\n",
            leader_script.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![descendants.clone()]);

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(5), || {
        read_pids(std::slice::from_ref(&descendants)).len() >= 2
    }));
    let descendant_pids = read_pids(std::slice::from_ref(&descendants));
    assert!(
        !process_is_running(descendant_pids[0]),
        "escaped descendant survived natural restart: {}",
        runner.log()
    );
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
    assert!(
        descendant_pids.iter().all(|pid| !process_is_running(*pid)),
        "escaped descendants remained after shutdown"
    );
}

#[test]
fn natural_exit_cleans_observed_process_after_its_group_changes() {
    let temp = tempfile::tempdir().unwrap();
    let before = temp.path().join("before.pid");
    let after = temp.path().join("after.pid");
    let switch = temp.path().join("switch");
    let escaped_script = temp.path().join("escaped.sh");
    let changing_script = temp.path().join("changing.sh");
    let leader_script = temp.path().join("leader.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    write_executable(
        &escaped_script,
        &format!(
            "#!/bin/bash\necho $$ > {}\nwhile :; do sleep 1; done\n",
            after.display()
        ),
    );
    write_executable(
        &changing_script,
        &format!(
            "#!/bin/bash\necho $$ > {}\nwhile [ ! -e {} ]; do sleep 0.01; done\nexec setsid {}\n",
            before.display(),
            switch.display(),
            escaped_script.display()
        ),
    );
    write_executable(
        &leader_script,
        &format!(
            "#!/bin/bash\n{} &\nwhile [ ! -s {} ]; do sleep 0.01; done\nexit 0\n",
            changing_script.display(),
            after.display()
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  changing_group:\n    command: \"{}\"\n    restart_delay: 10000\n",
            leader_script.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![before.clone(), after.clone()]);

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(3), || before.exists()));
    let pid = read_pids(std::slice::from_ref(&before))[0];
    let original_group = process_group_id(pid).unwrap();
    thread::sleep(Duration::from_millis(200));
    File::create(&switch).unwrap();
    assert!(wait_until(Duration::from_secs(3), || after.exists()));
    assert_eq!(read_pids(std::slice::from_ref(&after)), [pid]);
    assert_eq!(process_group_id(pid), Some(pid));
    assert_ne!(original_group, pid);
    assert!(
        wait_until(Duration::from_secs(3), || !process_is_running(pid)),
        "process survived group change and leader exit: {}",
        runner.log()
    );
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(3)).success());
}

#[test]
fn user_signal_reaches_observed_reparented_new_session_descendant() {
    let temp = tempfile::tempdir().unwrap();
    let before = temp.path().join("before.pid");
    let after = temp.path().join("after.pid");
    let switch = temp.path().join("switch");
    let ready = temp.path().join("ready");
    let bridge_done = temp.path().join("bridge-done");
    let received = temp.path().join("received");
    let escaped_script = temp.path().join("escaped.sh");
    let changing_script = temp.path().join("changing.sh");
    let bridge_script = temp.path().join("bridge.sh");
    let root_script = temp.path().join("root.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    write_executable(
        &escaped_script,
        &format!(
            "#!/bin/bash\necho $$ > {}\ntrap 'echo USR1 >> {}' USR1\ntouch {}\nwhile :; do sleep 1; done\n",
            after.display(),
            received.display(),
            ready.display()
        ),
    );
    write_executable(
        &changing_script,
        &format!(
            "#!/bin/bash\necho $$ > {}\nwhile [ ! -e {} ]; do sleep 0.01; done\nexec setsid {}\n",
            before.display(),
            switch.display(),
            escaped_script.display()
        ),
    );
    write_executable(
        &bridge_script,
        &format!(
            "#!/bin/bash\n{} &\nwhile [ ! -e {} ]; do sleep 0.01; done\n",
            changing_script.display(),
            ready.display()
        ),
    );
    write_executable(
        &root_script,
        &format!(
            "#!/bin/bash\ntrap ':' USR1\n{}\ntouch {}\nwhile :; do sleep 1; done\n",
            bridge_script.display(),
            bridge_done.display()
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  escaped_signal:\n    command: \"exec {}\"\n    shutdown_timeout: 1\n",
            root_script.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![before.clone(), after.clone()]);

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(3), || before.exists()));
    let pid = read_pids(std::slice::from_ref(&before))[0];
    thread::sleep(Duration::from_millis(200));
    File::create(&switch).unwrap();
    assert!(wait_until(Duration::from_secs(3), || {
        ready.exists() && bridge_done.exists()
    }));
    assert_eq!(read_pids(std::slice::from_ref(&after)), [pid]);
    thread::sleep(Duration::from_millis(200));

    runner.signal(libc::SIGUSR1);
    assert!(
        wait_until(Duration::from_secs(3), || received.exists()),
        "USR1 missed escaped descendant: {}",
        runner.log()
    );
    thread::sleep(Duration::from_millis(200));
    assert_eq!(fs::read_to_string(&received).unwrap().lines().count(), 1);
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
    assert!(!process_is_running(pid));
}

#[test]
fn blocked_runner_output_does_not_delay_shutdown() {
    let _resource_guard = serialize_resource_intensive_test();
    let temp = tempfile::tempdir().unwrap();
    let pid_file = temp.path().join("producer.pid");
    let script = temp.path().join("producer.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    write_executable(
        &script,
        &format!(
            "#!/bin/bash\necho $$ > {}\nexec dd if=/dev/zero bs=65536 status=none\n",
            pid_file.display()
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  producer:\n    command: \"{}\"\n",
            script.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![pid_file.clone()]);

    let mut runner = Runner::start_with_unread_stdout(&config, log);
    assert!(wait_until(Duration::from_secs(3), || pid_file.exists()));
    thread::sleep(Duration::from_secs(1));
    let rss_kib = resident_set_kib(runner.child.id()).expect("runner RSS unavailable");
    assert!(
        rss_kib < 64 * 1024,
        "blocked output consumed {rss_kib} KiB RSS"
    );
    runner.signal(libc::SIGTERM);
    assert!(
        runner.wait(Duration::from_secs(4)).success(),
        "blocked stdout stalled shutdown"
    );
}

#[test]
fn oversized_job_prefix_keeps_output_queue_memory_bounded() {
    let _resource_guard = serialize_resource_intensive_test();
    let temp = tempfile::tempdir().unwrap();
    let pid_file = temp.path().join("producer.pid");
    let script = temp.path().join("producer.sh");
    let config = temp.path().join("jobs.json");
    let log = temp.path().join("runner.log");
    write_executable(
        &script,
        &format!(
            "#!/bin/bash\necho $$ > {}\nexec dd if=/dev/zero bs=65536 status=none\n",
            pid_file.display()
        ),
    );
    let job_name = "wide".repeat(256 * 1024);
    fs::write(
        &config,
        format!(
            "{{\"jobs\":{{\"{job_name}\":{{\"command\":\"{}\",\"shutdown_timeout\":1}}}}}}",
            script.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![pid_file.clone()]);

    let mut runner = Runner::start_with_unread_stdout(&config, log);
    assert!(wait_until(Duration::from_secs(5), || pid_file.exists()));
    thread::sleep(Duration::from_secs(1));
    let rss_kib = resident_set_kib(runner.child.id()).expect("runner RSS unavailable");
    assert!(
        rss_kib < 64 * 1024,
        "oversized prefixes consumed {rss_kib} KiB RSS"
    );
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
}

#[test]
fn finite_tail_output_is_flushed_before_runner_exit() {
    let temp = tempfile::tempdir().unwrap();
    let ready = temp.path().join("ready");
    let script = temp.path().join("producer.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    write_executable(
        &script,
        &format!(
            "#!/bin/bash\npayload=$(printf '%02000d' 0 | tr 0 x)\nfor i in $(seq 1 512); do printf '%04d-%s\\n' \"$i\" \"$payload\"; done\necho FINITE_TAIL_MARKER\ntouch {}\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n",
            ready.display()
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  finite_output:\n    command: \"{}\"\n    shutdown_timeout: 1\n",
            script.display()
        ),
    )
    .unwrap();

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(5), || ready.exists()));
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
    assert!(
        runner.log().contains("FINITE_TAIL_MARKER"),
        "accepted tail output was lost"
    );
}

#[test]
fn exact_reader_chunk_boundary_does_not_add_blank_output() {
    let temp = tempfile::tempdir().unwrap();
    let ready = temp.path().join("ready");
    let script = temp.path().join("producer.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    write_executable(
        &script,
        &format!(
            "#!/bin/bash\nhead -c 65536 /dev/zero | tr '\\0' x\nprintf '\\nBOUNDARY_MARKER\\n'\ntouch {}\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n",
            ready.display()
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  boundary:\n    command: \"{}\"\n",
            script.display()
        ),
    )
    .unwrap();

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(4), || ready.exists()));
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(4)).success());
    let output = runner.log();
    assert!(output.contains("BOUNDARY_MARKER"));
    assert!(
        !output.lines().any(|line| line.ends_with("(1) > ")),
        "exact input boundary produced a synthetic blank frame"
    );
}

#[test]
fn blocked_runner_stderr_does_not_delay_shutdown() {
    let _resource_guard = serialize_resource_intensive_test();
    let temp = tempfile::tempdir().unwrap();
    let pid_file = temp.path().join("producer.pid");
    let script = temp.path().join("producer.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    write_executable(
        &script,
        &format!(
            "#!/bin/bash\necho $$ > {}\nexec dd if=/dev/zero bs=65536 status=none >&2\n",
            pid_file.display()
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  producer:\n    command: \"{}\"\n    shutdown_timeout: 1\n",
            script.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![pid_file.clone()]);

    let mut runner = Runner::start_with_unread_stderr(&config, log);
    assert!(wait_until(Duration::from_secs(3), || pid_file.exists()));
    thread::sleep(Duration::from_millis(300));
    runner.signal(libc::SIGTERM);
    assert!(
        runner.wait(Duration::from_secs(4)).success(),
        "blocked stderr stalled shutdown"
    );
}

#[test]
fn second_termination_signal_forces_shutdown() {
    let temp = tempfile::tempdir().unwrap();
    let pid_file = temp.path().join("stubborn.pid");
    let script = temp.path().join("stubborn.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    write_executable(
        &script,
        &format!(
            "#!/bin/bash\necho $$ > {}\ntrap '' TERM INT HUP QUIT\nwhile :; do sleep 1; done\n",
            pid_file.display()
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  stubborn:\n    command: \"{}\"\n    shutdown_timeout: 30\n",
            script.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![pid_file.clone()]);

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(3), || pid_file.exists()));
    let pid = read_pids(&[pid_file])[0];
    runner.signal(libc::SIGTERM);
    thread::sleep(Duration::from_millis(200));
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(4)).success());
    assert!(!process_is_running(pid));
}

#[test]
fn parallel_start_shutdown_does_not_orphan_jobs() {
    let _resource_guard = serialize_resource_intensive_test();
    let temp = tempfile::tempdir().unwrap();
    let script = temp.path().join("long-job.sh");
    write_executable(
        &script,
        "#!/bin/bash\necho $$ > \"$1\"\ntrap 'exit 0' TERM INT HUP QUIT\nwhile :; do sleep 1; done\n",
    );

    let mut cleanup = ProcessCleanup(Vec::new());
    let mut spawned = 0;
    for iteration in 0..12 {
        let config = temp.path().join(format!("jobs-{iteration}.yaml"));
        let log = temp.path().join(format!("runner-{iteration}.log"));
        let mut contents = String::from("jobs:\n");
        let mut iteration_pid_files = Vec::new();
        for job in 0..16 {
            let pid_file = temp.path().join(format!("pid-{iteration}-{job}"));
            contents.push_str(&format!(
                "  job_{job}:\n    command: \"{} {}\"\n    shutdown_timeout: 1\n",
                script.display(),
                pid_file.display()
            ));
            iteration_pid_files.push(pid_file);
        }
        fs::write(&config, contents).unwrap();
        cleanup.0.extend(iteration_pid_files.clone());

        let mut runner = Runner::start(&config, log, &["-t", "4"]);
        assert!(wait_until(Duration::from_secs(3), || runner
            .log()
            .contains("Initializing job")));
        thread::sleep(Duration::from_millis((iteration % 4) + 1));
        runner.signal(libc::SIGTERM);
        assert!(
            runner.wait(Duration::from_secs(5)).success(),
            "iteration {iteration} failed: {}",
            runner.log()
        );
        let pids = read_pids(&iteration_pid_files);
        spawned += pids.len();
        assert!(
            pids.iter().all(|pid| !process_is_running(*pid)),
            "iteration {iteration} left jobs alive: {}",
            runner.log()
        );
    }
    assert!(spawned > 0, "race test never started a job");
}

#[test]
fn wrapper_restarts_a_killed_inner_runner_and_stops_during_backoff() {
    let temp = tempfile::tempdir().unwrap();
    let runs = temp.path().join("runs");
    let script = temp.path().join("kill-runner.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("wrapper.log");
    write_executable(
        &script,
        &format!(
            "#!/bin/bash\necho $$ >> {}\npid=$PPID\nwhile [ \"$pid\" -gt 1 ]; do\n  if [ \"$(cat /proc/$pid/comm 2>/dev/null)\" = job-runner-rs ]; then\n    kill -KILL \"$pid\"\n    exit 0\n  fi\n  pid=$(awk '{{print $4}}' /proc/$pid/stat 2>/dev/null || echo 1)\ndone\nexit 1\n",
            runs.display()
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  kill_inner:\n    command: \"{}\"\n    restart_delay: 10000\n",
            script.display()
        ),
    )
    .unwrap();
    let mut wrapper = Runner::start(&config, log, &["--wrap"]);
    assert!(
        wait_until(Duration::from_secs(25), || {
            fs::read_to_string(&runs)
                .map(|contents| contents.lines().count() >= 2)
                .unwrap_or(false)
        }),
        "inner runner did not restart: {}",
        wrapper.log()
    );
    assert!(
        wrapper.is_running(),
        "wrapper died with inner: {}",
        wrapper.log()
    );

    let start = Instant::now();
    wrapper.signal(libc::SIGTERM);
    assert!(wrapper.wait(Duration::from_secs(4)).success());
    assert!(
        start.elapsed() < Duration::from_secs(4),
        "wrapper backoff was not interruptible"
    );
}

#[test]
fn wrapper_drops_user_signals_received_during_backoff() {
    let temp = tempfile::tempdir().unwrap();
    let runs = temp.path().join("runs");
    let stable = temp.path().join("stable");
    let stale_signal = temp.path().join("stale-signal");
    let job_pid = temp.path().join("job.pid");
    let script = temp.path().join("restart-once.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("wrapper.log");
    write_executable(
        &script,
        &format!(
            "#!/bin/bash\necho run >> {runs}\nif [ \"$(wc -l < {runs})\" -eq 1 ]; then\n  pid=$PPID\n  while [ \"$pid\" -gt 1 ]; do\n    if [ \"$(cat /proc/$pid/comm 2>/dev/null)\" = job-runner-rs ]; then\n      kill -KILL \"$pid\"\n      exit 0\n    fi\n    pid=$(awk '{{print $4}}' /proc/$pid/stat 2>/dev/null || echo 1)\n  done\n  exit 1\nfi\necho $$ > {job_pid}\ntrap 'touch {stale_signal}' USR1\ntouch {stable}\nwhile :; do sleep 1; done\n",
            runs = runs.display(),
            job_pid = job_pid.display(),
            stale_signal = stale_signal.display(),
            stable = stable.display()
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  restart_once:\n    command: \"{}\"\n    restart_delay: 10000\n",
            script.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![job_pid]);

    let mut wrapper = Runner::start(&config, log, &["--wrap"]);
    assert!(wait_until(Duration::from_secs(5), || {
        wrapper
            .log()
            .contains("waiting 15 seconds before restarting")
    }));
    wrapper.signal(libc::SIGUSR1);
    assert!(
        wait_until(Duration::from_secs(20), || stable.exists()),
        "inner runner did not survive restart: {}",
        wrapper.log()
    );
    thread::sleep(Duration::from_millis(300));
    assert!(wrapper.is_running());
    assert!(
        !stale_signal.exists(),
        "backoff signal reached next generation"
    );
    wrapper.signal(libc::SIGTERM);
    assert!(wrapper.wait(Duration::from_secs(5)).success());
}

#[test]
fn wrapper_forwards_user_signals_to_inner_jobs() {
    let temp = tempfile::tempdir().unwrap();
    let pid_file = temp.path().join("job.pid");
    let ready = temp.path().join("ready");
    let received = temp.path().join("received");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("wrapper.log");
    fs::write(
        &config,
        format!(
            "jobs:\n  signal_job:\n    command: |\n      echo $$ > {}\n      trap 'touch {}' USR1\n      touch {}\n      while :; do sleep 1; done\n",
            pid_file.display(),
            received.display(),
            ready.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![pid_file]);

    let mut wrapper = Runner::start(&config, log, &["--wrap"]);
    assert!(wait_until(Duration::from_secs(5), || ready.exists()));
    wrapper.signal(libc::SIGUSR1);
    assert!(
        wait_until(Duration::from_secs(3), || received.exists()),
        "wrapper did not forward USR1: {}",
        wrapper.log()
    );
    wrapper.signal(libc::SIGTERM);
    assert!(wrapper.wait(Duration::from_secs(5)).success());
}

#[test]
fn same_user_sudo_delivers_each_signal_once() {
    let Some(username) = current_username() else {
        eprintln!("skipped: current username unavailable");
        return;
    };
    if !sudo_available_for(&username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    for _ in 0..20 {
        exercise_sudo_signal_route(&username);
    }
}

#[test]
fn cross_uid_sudo_signals_and_stops_job() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    for _ in 0..3 {
        exercise_sudo_signal_route(username);
    }
}

#[test]
fn sudo_signals_nested_and_escaped_workloads_once() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }

    let temp = tempfile::tempdir().unwrap();
    let pids = temp.path().join("jobs.pid");
    let received = temp.path().join("received");
    let ready = temp.path().join("ready");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    for path in [&pids, &received, &ready] {
        File::create(path).unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o666);
        fs::set_permissions(path, permissions).unwrap();
    }
    let mut permissions = fs::metadata(temp.path()).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(temp.path(), permissions).unwrap();
    fs::write(
        &config,
        format!(
            "jobs:\n  sudo_tree:\n    command: |\n      echo $$ >> {pids}\n      trap 'echo root >> {received}' USR1\n      bash -c 'echo $$ >> {pids}; trap \"echo nested >> {received}\" USR1; while :; do sleep 1; done' &\n      setsid bash -c 'echo $$ >> {pids}; trap \"echo escaped >> {received}\" USR1; while :; do sleep 1; done' &\n      while [ \"$(wc -l < {pids})\" -lt 3 ]; do sleep 0.01; done\n      echo ready > {ready}\n      while :; do sleep 1; done\n    sudo_user: {username}\n    shutdown_timeout: 1\n",
            pids = pids.display(),
            received = received.display(),
            ready = ready.display()
        ),
    )
    .unwrap();
    let _cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: pids.clone(),
    };

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(5), || {
        fs::metadata(&ready).is_ok_and(|metadata| metadata.len() > 0)
    }));
    let process_ids = read_pids(std::slice::from_ref(&pids));
    assert_eq!(process_ids.len(), 3);
    thread::sleep(Duration::from_millis(200));
    runner.signal(libc::SIGUSR1);
    assert!(wait_until(Duration::from_secs(3), || {
        fs::read_to_string(&received)
            .map(|contents| contents.lines().count() >= 3)
            .unwrap_or(false)
    }));
    thread::sleep(Duration::from_millis(200));
    let deliveries = fs::read_to_string(&received).unwrap();
    for label in ["root", "nested", "escaped"] {
        assert_eq!(
            deliveries.lines().filter(|line| *line == label).count(),
            1,
            "{label} received USR1 more than once: {deliveries:?}"
        );
    }
    assert!(runner.is_running(), "USR1 restarted the sudo job");
    assert!(process_is_running(process_ids[0]));
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
    assert!(wait_until(Duration::from_secs(2), || process_ids
        .iter()
        .all(|pid| !process_is_running(*pid))));
}

#[test]
fn sudo_signal_helper_failure_fails_the_supervisor() {
    let Some(username) = current_username() else {
        eprintln!("skipped: current username unavailable");
        return;
    };
    if !sudo_available_for(&username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let path = install_failing_signal_sudo(temp.path());
    let pid_file = temp.path().join("escaped.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    File::create(&pid_file).unwrap();
    let mut permissions = fs::metadata(&pid_file).unwrap().permissions();
    permissions.set_mode(0o666);
    fs::set_permissions(&pid_file, permissions).unwrap();
    let mut permissions = fs::metadata(temp.path()).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(temp.path(), permissions).unwrap();
    fs::write(
        &config,
        format!(
            "jobs:\n  failed_signal:\n    command: |\n      setsid sh -c 'echo $$ > {}; while :; do sleep 1; done' &\n      while [ ! -s {} ]; do sleep 0.01; done\n      sleep 0.2\n      exit 0\n    sudo_user: {}\n    restart_delay: 10000\n",
            pid_file.display(),
            pid_file.display(),
            username
        ),
    )
    .unwrap();
    let _cleanup = SudoProcessCleanup {
        username,
        pid_path: pid_file.clone(),
    };
    let mut runner = Runner::start_with_path(&config, log, &path);
    assert!(wait_until(Duration::from_secs(4), || {
        !read_pids(std::slice::from_ref(&pid_file)).is_empty()
    }));
    let status = runner.wait(Duration::from_secs(8));
    assert!(!status.success(), "signal helper failure returned success");
    let output = runner.log();
    assert!(output.contains("sudo signal helper exited"), "{output}");
    assert!(
        !output.contains("Starting in 10.000s"),
        "job restarted: {output}"
    );
}

#[test]
fn sudo_user_signal_failure_initiates_invariant_shutdown() {
    let Some(username) = current_username() else {
        eprintln!("skipped: current username unavailable");
        return;
    };
    if !sudo_available_for(&username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }

    let temp = tempfile::tempdir().unwrap();
    let path = install_failing_signal_sudo(temp.path());
    let pid_file = temp.path().join("job.pid");
    let ready = temp.path().join("ready");
    let received = temp.path().join("received");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    for file in [&pid_file, &ready, &received] {
        File::create(file).unwrap();
        let mut permissions = fs::metadata(file).unwrap().permissions();
        permissions.set_mode(0o666);
        fs::set_permissions(file, permissions).unwrap();
    }
    let mut permissions = fs::metadata(temp.path()).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(temp.path(), permissions).unwrap();
    fs::write(
        &config,
        format!(
            "jobs:\n  failed_usr:\n    command: |\n      echo $$ > {}\n      trap 'echo USR1 > {}' USR1\n      echo ready > {}\n      while :; do sleep 1; done\n    sudo_user: {}\n    shutdown_timeout: 0\n",
            pid_file.display(),
            received.display(),
            ready.display(),
            username
        ),
    )
    .unwrap();
    let _cleanup = SudoProcessCleanup {
        username,
        pid_path: pid_file.clone(),
    };

    let mut runner = Runner::start_with_path(&config, log, &path);
    assert!(wait_until(Duration::from_secs(4), || {
        fs::metadata(&ready).is_ok_and(|metadata| metadata.len() > 0)
    }));
    runner.signal(libc::SIGUSR1);
    let status = runner.wait(Duration::from_secs(8));
    assert!(!status.success(), "failed USR1 forwarding returned success");
    assert_eq!(fs::metadata(&received).unwrap().len(), 0);
    let output = runner.log();
    assert!(output.contains("Signal forwarding failed"), "{output}");
}

#[test]
fn runner_refuses_to_run_as_container_pid_one() {
    if !docker_available() {
        eprintln!("skipped: Docker or ubuntu:24.04 image unavailable");
        return;
    }

    let binary = format!("{}:/runner:ro", env!("CARGO_BIN_EXE_job-runner-rs"));
    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--entrypoint",
            "/runner",
            "-v",
            &binary,
            "ubuntu:24.04",
            "--help",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Refusing to run as PID 1"), "{stderr}");
}

#[test]
fn short_docker_jobs_restart_without_leaving_containers() {
    if !docker_available() {
        eprintln!("skipped: Docker or ubuntu:24.04 image unavailable");
        return;
    }

    let temp = tempfile::tempdir().unwrap();
    let name = unique_container_name("short");
    let _container = ContainerCleanup(name.clone());
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    fs::write(
        &config,
        format!(
            "jobs:\n  docker_short:\n    command: \"docker run --rm --name {name} ubuntu:24.04 echo docker-short\"\n    restart_delay: 100\n"
        ),
    )
    .unwrap();

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(8), || runner
        .log()
        .matches("docker-short")
        .count()
        >= 2));
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
    assert!(!container_exists(&name), "short-lived container remained");
}

#[test]
fn attached_docker_job_proxies_term_to_container_pid_one() {
    if !docker_available() {
        eprintln!("skipped: Docker or ubuntu:24.04 image unavailable");
        return;
    }

    let temp = tempfile::tempdir().unwrap();
    let name = unique_container_name("proxy");
    let _container = ContainerCleanup(name.clone());
    let script = temp.path().join("container.sh");
    let ready = temp.path().join("ready");
    let terminated = temp.path().join("terminated");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    write_executable(
        &script,
        "#!/bin/bash\ntrap 'touch /output/terminated; exit 0' TERM\ntouch /output/ready\nwhile :; do sleep 1; done\n",
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  docker_proxy:\n    command: \"docker run --rm --name {name} -v {}:/entry.sh:ro -v {}:/output ubuntu:24.04 bash /entry.sh\"\n    shutdown_timeout: 4\n",
            script.display(),
            temp.path().display()
        ),
    )
    .unwrap();

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(8), || ready.exists()));
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(7)).success());
    assert!(terminated.exists(), "container PID 1 did not receive TERM");
    assert!(!container_exists(&name), "graceful container remained");
}

#[test]
fn long_docker_job_can_use_a_shutdown_hook() {
    if !docker_available() {
        eprintln!("skipped: Docker or ubuntu:24.04 image unavailable");
        return;
    }

    let temp = tempfile::tempdir().unwrap();
    let name = unique_container_name("long");
    let _container = ContainerCleanup(name.clone());
    let script = temp.path().join("docker-job.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    write_executable(
        &script,
        &format!(
            "#!/bin/bash\ncleanup() {{ docker rm -f {name} >/dev/null 2>&1 || true; }}\ntrap 'cleanup; exit 0' TERM INT HUP QUIT\ndocker run --rm --name {name} ubuntu:24.04 sleep 300 &\nwait $!\ncleanup\n"
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  docker_long:\n    command: \"{}\"\n    shutdown_timeout: 4\n",
            script.display()
        ),
    )
    .unwrap();

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(8), || container_exists(
        &name
    )));
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(7)).success());
    assert!(
        wait_until(Duration::from_secs(2), || !container_exists(&name)),
        "Docker-aware shutdown hook did not remove container: {}",
        runner.log()
    );
}
