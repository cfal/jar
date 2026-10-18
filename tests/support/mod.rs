use std::collections::HashMap;
use std::fs;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub start_time: u64,
}

fn stat(pid: u32) -> Option<(ProcessIdentity, u32, char)> {
    let bytes = fs::read(format!("/proc/{pid}/stat")).ok()?;
    let close = bytes.iter().rposition(|byte| *byte == b')')?;
    let tail = std::str::from_utf8(&bytes[close + 1..]).ok()?;
    let mut fields = tail.split_whitespace();
    let state = fields.next()?.chars().next()?;
    let parent = fields.next()?.parse().ok()?;
    let start_time = fields.nth(17)?.parse().ok()?;
    Some((ProcessIdentity { pid, start_time }, parent, state))
}

impl ProcessIdentity {
    pub fn read(pid: u32) -> Option<Self> {
        stat(pid).map(|(identity, _, _)| identity)
    }

    pub fn running(self) -> bool {
        stat(self.pid)
            .is_some_and(|(identity, _, state)| identity == self && !matches!(state, 'Z' | 'X'))
    }

    pub fn signal(self, signal: libc::c_int, sudo_user: Option<&str>) {
        if Self::read(self.pid) != Some(self) {
            return;
        }
        if unsafe { libc::kill(self.pid as libc::pid_t, signal) } == 0 {
            return;
        }
        if io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
            && let Some(user) = sudo_user
        {
            let _ = Command::new("sudo")
                .args([
                    "-n",
                    "-u",
                    user,
                    "--",
                    "kill",
                    &format!("-{signal}"),
                    "--",
                    &self.pid.to_string(),
                ])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

fn descendants(known: &mut Vec<ProcessIdentity>) {
    let Ok(entries) = fs::read_dir("/proc") else {
        return;
    };
    let snapshot: Vec<_> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().to_str()?.parse().ok())
        .filter_map(stat)
        .collect();
    loop {
        let mut added = false;
        for (identity, parent, _) in &snapshot {
            if !known.contains(identity)
                && snapshot
                    .iter()
                    .any(|(root, _, _)| root.pid == *parent && known.contains(root))
            {
                known.push(*identity);
                added = true;
            }
        }
        if !added {
            break;
        }
    }
}

pub struct GuardedChild {
    child: Child,
    root: Option<ProcessIdentity>,
    known: Vec<ProcessIdentity>,
}

pub trait SpawnGuarded {
    fn spawn_guarded(&mut self) -> io::Result<GuardedChild>;
}

impl SpawnGuarded for Command {
    fn spawn_guarded(&mut self) -> io::Result<GuardedChild> {
        let child = self.spawn()?;
        let root = ProcessIdentity::read(child.id());
        Ok(GuardedChild {
            child,
            root,
            known: root.into_iter().collect(),
        })
    }
}

impl Deref for GuardedChild {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.child
    }
}

impl DerefMut for GuardedChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.child
    }
}

impl GuardedChild {
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        descendants(&mut self.known);
        self.child.try_wait()
    }

    pub fn wait(&mut self) -> io::Result<ExitStatus> {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = self.try_wait()? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "test child did not exit",
                ));
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for GuardedChild {
    fn drop(&mut self) {
        let exited = self.try_wait().ok().flatten().is_some();
        let Some(root) = self.root else {
            if !exited {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
            return;
        };
        if !exited {
            root.signal(libc::SIGTERM, None);
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while !exited && Instant::now() < deadline {
            if self.child.try_wait().ok().flatten().is_some() {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        // Keep identities captured before shutdown, even after adoption or setsid.
        descendants(&mut self.known);
        for process in self.known.iter().rev() {
            process.signal(libc::SIGKILL, Some("root"));
        }
        let _ = self.child.wait();
        let deadline = Instant::now() + Duration::from_secs(2);
        while self.known.iter().any(|identity| identity.running()) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        for process in self.known.iter().filter(|identity| identity.running()) {
            eprintln!("test cleanup could not stop {process:?}");
        }
    }
}

type RecordedIdentities = HashMap<PathBuf, Vec<ProcessIdentity>>;
static RECORDED: OnceLock<Mutex<RecordedIdentities>> = OnceLock::new();

pub fn observe_pid_file(path: &Path) -> Vec<u32> {
    let pids: Vec<u32> = fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.trim().parse().ok())
        .collect();
    let mut recorded = RECORDED
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let identities = recorded.entry(path.to_path_buf()).or_default();
    for pid in &pids {
        if !identities.iter().any(|identity| identity.pid == *pid)
            && let Some(identity) = ProcessIdentity::read(*pid)
        {
            identities.push(identity);
        }
    }
    pids
}

pub fn cleanup_pid_file(path: &Path, sudo_user: Option<&str>) {
    let known = RECORDED
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(path)
        .unwrap_or_default();
    for pid in fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.trim().parse::<u32>().ok())
    {
        if !known.iter().any(|identity| identity.pid == pid)
            && ProcessIdentity::read(pid).is_some_and(|identity| identity.running())
        {
            eprintln!(
                "refusing test cleanup of unobserved PID {pid} from {}",
                path.display()
            );
        }
    }
    for identity in known {
        let mut tree = vec![identity];
        descendants(&mut tree);
        for process in tree.into_iter().rev() {
            process.signal(libc::SIGKILL, sudo_user);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EmergencyCleanup(Vec<ProcessIdentity>);
    impl Drop for EmergencyCleanup {
        fn drop(&mut self) {
            for identity in self.0.iter().rev() {
                identity.signal(libc::SIGKILL, None);
            }
        }
    }

    #[test]
    fn stale_or_unobserved_pid_records_cannot_kill_a_replacement() {
        let mut child = Command::new("sleep").arg("30").spawn_guarded().unwrap();
        let identity = ProcessIdentity::read(child.id()).unwrap();
        let stale = ProcessIdentity {
            start_time: identity.start_time + 1,
            ..identity
        };
        stale.signal(libc::SIGKILL, None);
        assert!(child.try_wait().unwrap().is_none());
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("pid");
        fs::write(&path, identity.pid.to_string()).unwrap();
        cleanup_pid_file(&path, None);
        assert!(child.try_wait().unwrap().is_none());
        observe_pid_file(&path);
        RECORDED
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .insert(path.clone(), vec![stale]);
        cleanup_pid_file(&path, None);
        assert!(child.try_wait().unwrap().is_none());
        identity.signal(libc::SIGTERM, None);
        assert!(!child.wait().unwrap().success());
    }

    #[test]
    fn unwinding_reaps_the_runner_and_stops_separate_job_sessions() {
        for wrap in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let config = temp.path().join("jobs.json");
            let pid_path = temp.path().join("pid");
            fs::write(
                &config,
                serde_json::json!({"jobs": {"job": {
                    "command": format!("echo $$ > {}; exec sleep 30", pid_path.display()),
                    "shutdown_timeout": 0
                }}})
                .to_string(),
            )
            .unwrap();
            let mut emergency = EmergencyCleanup(Vec::new());
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut command = Command::new(env!("CARGO_BIN_EXE_jr"));
                if wrap {
                    command.arg("--wrap");
                }
                let child = command
                    .arg(&config)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn_guarded()
                    .unwrap();
                emergency.0.push(ProcessIdentity::read(child.id()).unwrap());
                let deadline = Instant::now() + Duration::from_secs(5);
                while observe_pid_file(&pid_path).is_empty() {
                    assert!(Instant::now() < deadline, "job did not start");
                    thread::sleep(Duration::from_millis(20));
                }
                descendants(&mut emergency.0);
                assert!(emergency.0.len() >= if wrap { 4 } else { 3 });
                panic!("intentional fixture failure");
            }));
            assert!(result.is_err());
            assert!(
                emergency.0.iter().all(|identity| !identity.running()),
                "{:?}",
                emergency.0
            );
            assert_eq!(
                ProcessIdentity::read(emergency.0[0].pid),
                None,
                "runner was not reaped"
            );
            cleanup_pid_file(&pid_path, None);
        }
    }
}
