use std::collections::HashMap;
use std::fs;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

const IDENTITY_CHECKED_KILL: &str = r#"
pid=$1 expected=$2 signal=$3
IFS= read -r stat < "/proc/$pid/stat" || exit 0
set -f
set -- ${stat##*) }
[ "$#" -ge 20 ] || exit 1
shift 19
[ "$1" = "$expected" ] || exit 0
kill "-$signal" "$pid"
"#;

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
                    "sh",
                    "-c",
                    IDENTITY_CHECKED_KILL,
                    "--",
                    &self.pid.to_string(),
                    &self.start_time.to_string(),
                    &signal.to_string(),
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

static GUARDED_ROOTS: Mutex<Vec<ProcessIdentity>> = Mutex::new(Vec::new());

pub trait SpawnGuarded {
    fn spawn_guarded(&mut self) -> io::Result<GuardedChild>;
}

impl SpawnGuarded for Command {
    fn spawn_guarded(&mut self) -> io::Result<GuardedChild> {
        let child = self.spawn()?;
        let root = ProcessIdentity::read(child.id());
        if let Some(root) = root {
            GUARDED_ROOTS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(root);
        }
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
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if self.child.try_wait().ok().flatten().is_some() {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
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
        GUARDED_ROOTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|identity| *identity != root);
    }
}

type RecordedIdentities = HashMap<PathBuf, Vec<ProcessIdentity>>;
static RECORDED: OnceLock<Mutex<RecordedIdentities>> = OnceLock::new();

fn read_pid_file(path: &Path) -> Vec<u32> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.trim().parse().ok())
        .collect()
}

pub fn observe_pid_file(path: &Path) -> Vec<u32> {
    let pids = read_pid_file(path);
    if pids.is_empty() {
        return pids;
    }
    // A PID file may describe a finished generation. Authorize only live fixture ownership.
    let mut owned = GUARDED_ROOTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    descendants(&mut owned);
    let mut recorded = RECORDED
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let identities = recorded.entry(path.to_path_buf()).or_default();
    for pid in &pids {
        if let Some(identity) = owned
            .iter()
            .copied()
            .find(|identity| identity.pid == *pid && identity.running())
            && !identities.contains(&identity)
        {
            identities.push(identity);
        }
    }
    pids
}

pub fn cleanup_pid_file(path: &Path, sudo_user: Option<&str>) -> usize {
    let known = RECORDED
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(path)
        .unwrap_or_default();
    let mut unobserved = 0;
    for pid in read_pid_file(path) {
        if let Some(identity) = ProcessIdentity::read(pid)
            && identity.running()
            && !known.contains(&identity)
        {
            unobserved += 1;
            eprintln!(
                "refusing test cleanup of unobserved PID {pid} from {}",
                path.display()
            );
        }
    }
    for identity in known {
        cleanup_process_tree(identity, sudo_user);
    }
    unobserved
}

pub fn cleanup_process_tree(identity: ProcessIdentity, sudo_user: Option<&str>) {
    let mut tree = vec![identity];
    descendants(&mut tree);
    for process in tree.into_iter().rev() {
        process.signal(libc::SIGKILL, sudo_user);
    }
}
