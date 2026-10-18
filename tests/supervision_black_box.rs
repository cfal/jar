#![cfg(unix)]

use std::fs::{self, File};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

#[allow(dead_code)]
mod support;
use support::{GuardedChild, SpawnGuarded};

const POLL_INTERVAL: Duration = Duration::from_millis(20);
const TEST_RECONCILE_INTERVAL_ENV: &str = "JOB_RUNNER_TEST_PROCESS_RECONCILE_INTERVAL_MS";
static CONTAINER_SEQUENCE: AtomicUsize = AtomicUsize::new(0);
static RESOURCE_INTENSIVE_TEST: Mutex<()> = Mutex::new(());

fn serialize_resource_intensive_test() -> MutexGuard<'static, ()> {
    RESOURCE_INTENSIVE_TEST
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

struct Runner {
    child: GuardedChild,
    log_path: PathBuf,
}

impl Runner {
    fn spawn_in_session(mut command: Command, log_path: PathBuf) -> Self {
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        Self {
            child: command.spawn_guarded().unwrap(),
            log_path,
        }
    }

    fn start(config: &Path, log_path: PathBuf, extra_args: &[&str]) -> Self {
        Self::start_with_env(config, log_path, extra_args, &[])
    }

    fn start_with_env(
        config: &Path,
        log_path: PathBuf,
        extra_args: &[&str],
        environment: &[(&str, &str)],
    ) -> Self {
        let log = File::create(&log_path).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_jr"));
        command
            .args(extra_args)
            .arg(config)
            .envs(environment.iter().copied())
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        Self::spawn_in_session(command, log_path)
    }

    fn start_with_unread_stdout(config: &Path, log_path: PathBuf) -> Self {
        let log = File::create(&log_path).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_jr"));
        command.arg(config).stdout(Stdio::piped()).stderr(log);
        Self::spawn_in_session(command, log_path)
    }

    fn start_with_unread_stderr(config: &Path, log_path: PathBuf, extra_args: &[&str]) -> Self {
        let log = File::create(&log_path).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_jr"));
        command
            .args(extra_args)
            .arg(config)
            .stdout(log)
            .stderr(Stdio::piped());
        Self::spawn_in_session(command, log_path)
    }

    fn start_with_path(config: &Path, log_path: PathBuf, path: &str) -> Self {
        Self::start_with_path_and_env(config, log_path, path, &[])
    }

    fn start_with_path_and_env(
        config: &Path,
        log_path: PathBuf,
        path: &str,
        environment: &[(&str, &str)],
    ) -> Self {
        let log = File::create(&log_path).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_jr"));
        command
            .arg(config)
            .env("PATH", path)
            .envs(environment.iter().copied())
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        Self::spawn_in_session(command, log_path)
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

struct ForkedProcess(libc::pid_t);

impl Drop for ForkedProcess {
    fn drop(&mut self) {
        unsafe {
            libc::kill(self.0, libc::SIGKILL);
            libc::waitpid(self.0, std::ptr::null_mut(), 0);
        }
    }
}

struct ProcessCleanup(Vec<PathBuf>);

impl Drop for ProcessCleanup {
    fn drop(&mut self) {
        for path in &self.0 {
            support::cleanup_pid_file(path, None);
        }
    }
}

struct ChurnGuard {
    keep_running: Arc<AtomicBool>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl ChurnGuard {
    fn start(worker_count: usize, fork_count: Arc<AtomicUsize>) -> Self {
        let keep_running = Arc::new(AtomicBool::new(true));
        let handles = (0..worker_count)
            .map(|_| {
                let keep_running = keep_running.clone();
                let fork_count = fork_count.clone();
                thread::spawn(move || {
                    while keep_running.load(Ordering::Relaxed) {
                        let pid = fork_with_retry();
                        if pid == 0 {
                            unsafe { libc::_exit(0) };
                        }
                        let mut status = 0;
                        loop {
                            let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
                            if waited == pid {
                                break;
                            }
                            assert_eq!(
                                std::io::Error::last_os_error().raw_os_error(),
                                Some(libc::EINTR),
                                "waitpid failed"
                            );
                        }
                        fork_count.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        Self {
            keep_running,
            handles,
        }
    }

    fn stop(mut self) {
        self.keep_running.store(false, Ordering::Relaxed);
        for handle in self.handles.drain(..) {
            handle.join().unwrap();
        }
    }
}

impl Drop for ChurnGuard {
    fn drop(&mut self) {
        self.keep_running.store(false, Ordering::Relaxed);
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

struct SudoProcessCleanup {
    username: String,
    pid_path: PathBuf,
}

impl Drop for SudoProcessCleanup {
    fn drop(&mut self) {
        support::cleanup_pid_file(&self.pid_path, Some(&self.username));
    }
}

struct IdentityScopedSudoProcessCleanup {
    username: String,
    identity_path: PathBuf,
}

impl Drop for IdentityScopedSudoProcessCleanup {
    fn drop(&mut self) {
        let Some((pid, start_time)) = read_recorded_process_identity(&self.identity_path) else {
            return;
        };
        support::cleanup_process_tree(
            support::ProcessIdentity { pid, start_time },
            Some(&self.username),
        );
    }
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn build_identity_fault_library(root: &Path) -> PathBuf {
    let source = root.join("identity-fault.c");
    let library = root.join("identity-fault.so");
    fs::write(
        &source,
        r#"#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

__attribute__((constructor)) static void initialize(void) {
    if (!getenv("JOB_RUNNER_IDENTITY_FAULT_ROOT")) {
        char pid[32];
        snprintf(pid, sizeof(pid), "%d", getpid());
        setenv("JOB_RUNNER_IDENTITY_FAULT_ROOT", pid, 1);
    }
}

int open64(const char *path, int flags, ...) {
    static int (*original)(const char *, int, ...) = 0;
    static int failures = 0;
    if (!original) original = dlsym(RTLD_NEXT, "open64");
    mode_t mode = 0;
    if (flags & O_CREAT) {
        va_list arguments;
        va_start(arguments, flags);
        mode = va_arg(arguments, int);
        va_end(arguments);
    }
    int target = 0;
    char tail = 0;
    const char *root = getenv("JOB_RUNNER_IDENTITY_FAULT_ROOT");
    const char *ready = getenv("JOB_RUNNER_IDENTITY_FAULT_READY");
    if (failures < 3 && root && atoi(root) == getpid()
        && syscall(SYS_gettid) == getpid()
        && sscanf(path, "/proc/%d/stat%c", &target, &tail) == 1
        && target != getpid()) {
        if (failures == 0) {
            for (int attempt = 0; ready && access(ready, F_OK) != 0 && attempt < 500; attempt++) {
                usleep(10000);
            }
        }
        failures++;
        errno = EIO;
        return -1;
    }
    return original(path, flags, mode);
}
"#,
    )
    .unwrap();
    let status = Command::new("cc")
        .args(["-shared", "-fPIC"])
        .arg(&source)
        .arg("-o")
        .arg(&library)
        .arg("-ldl")
        .status()
        .unwrap();
    assert!(status.success(), "could not build identity fault library");
    library
}

fn build_proc_capture_fault_library(root: &Path) -> PathBuf {
    let source = root.join("proc-capture-fault.c");
    let library = root.join("proc-capture-fault.so");
    fs::write(
        &source,
        r#"#define _GNU_SOURCE
#include <dirent.h>
#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

__attribute__((constructor)) static void initialize(void) {
    if (!getenv("JOB_RUNNER_PROC_FAULT_ROOT")) {
        char pid[32];
        snprintf(pid, sizeof(pid), "%d", getpid());
        setenv("JOB_RUNNER_PROC_FAULT_ROOT", pid, 1);
    }
}

DIR *opendir(const char *path) {
    static DIR *(*original)(const char *) = 0;
    if (!original) original = dlsym(RTLD_NEXT, "opendir");
    const char *root = getenv("JOB_RUNNER_PROC_FAULT_ROOT");
    const char *marker = getenv("JOB_RUNNER_PROC_FAULT_MARKER");
    if (root && atoi(root) == getpid() && marker && !strcmp(path, "/proc")
        && access(marker, F_OK) == 0) {
        errno = EIO;
        return 0;
    }
    return original(path);
}
"#,
    )
    .unwrap();
    let status = Command::new("cc")
        .args(["-shared", "-fPIC"])
        .arg(&source)
        .arg("-o")
        .arg(&library)
        .arg("-ldl")
        .status()
        .unwrap();
    assert!(
        status.success(),
        "could not build proc capture fault library"
    );
    library
}

fn build_shim_startup_probe_library(root: &Path) -> PathBuf {
    let source = root.join("shim-startup-probe.c");
    let library = root.join("shim-startup-probe.so");
    fs::write(
        &source,
        r#"#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

static int readiness_fd = -1;

ssize_t write(int descriptor, const void *buffer, size_t length) {
    static ssize_t (*original)(int, const void *, size_t) = 0;
    if (!original) original = dlsym(RTLD_NEXT, "write");
    const char *marker = getenv("JOB_RUNNER_STOP_READY_WRITE_FOR");
    if (descriptor == readiness_fd && marker) {
        for (int attempt = 0; access(marker, F_OK) != 0 && attempt < 500; attempt++) {
            usleep(10000);
        }
        raise(SIGSTOP);
    }
    return original(descriptor, buffer, length);
}

__attribute__((constructor)) static void initialize(void) {
    const char *marker = getenv("JOB_RUNNER_SHIM_STARTUP_PID");
    if (!marker) return;
    int command_line = open("/proc/self/cmdline", O_RDONLY);
    if (command_line < 0) return;
    char contents[8192];
    ssize_t length = read(command_line, contents, sizeof(contents));
    close(command_line);
    if (length <= 0) return;
    int is_shim = 0;
    const char *previous = 0;
    for (ssize_t offset = 0; offset < length;) {
        const char *argument = contents + offset;
        size_t size = strnlen(argument, length - offset);
        if (!strcmp(argument, "--job-shim")) is_shim = 1;
        if (previous && !strcmp(previous, "--ready-fd")) readiness_fd = atoi(argument);
        previous = argument;
        offset += size + 1;
    }
    if (!is_shim) return;
    int output = open(marker, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (output >= 0) {
        dprintf(output, "%d\n", getpid());
        close(output);
    }
    const char *holder_marker = getenv("JOB_RUNNER_COMPLETION_HOLDER_PID");
    if (holder_marker) {
        pid_t first = fork();
        if (first == 0) {
            pid_t holder = fork();
            if (holder > 0) _exit(0);
            if (holder < 0) _exit(2);
            setsid();
            int holder_output = open(holder_marker, O_WRONLY | O_CREAT | O_TRUNC, 0666);
            if (holder_output >= 0) {
                dprintf(holder_output, "%d\n", getpid());
                close(holder_output);
            }
            sleep(30);
            _exit(0);
        }
        if (first > 0) waitpid(first, 0, 0);
    }
    if (getenv("JOB_RUNNER_STOP_SHIM_STARTUP")) raise(SIGSTOP);
}
"#,
    )
    .unwrap();
    let status = Command::new("cc")
        .args(["-shared", "-fPIC"])
        .arg(&source)
        .arg("-o")
        .arg(&library)
        .arg("-ldl")
        .status()
        .unwrap();
    assert!(
        status.success(),
        "could not build shim startup probe library"
    );
    library
}

fn build_helper_root_fault_library(root: &Path, helper_pid: &Path, helper_kind: &str) -> PathBuf {
    let source = root.join("signal-helper-root-fault.c");
    let library = root.join("signal-helper-root-fault.so");
    fs::write(
        &source,
        format!(
            r#"#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static int is_target_helper(int pid) {{
    char path[64];
    char command_line[8192];
    snprintf(path, sizeof(path), "/proc/%d/cmdline", pid);
    int descriptor = open(path, O_RDONLY);
    if (descriptor < 0) return 0;
    ssize_t length = read(descriptor, command_line, sizeof(command_line));
    close(descriptor);
    int is_shim = 0;
    int has_ready_fd = 0;
    int matches_kind = 0;
    for (ssize_t offset = 0; offset < length;) {{
        if (!strcmp(command_line + offset, "--job-shim")) is_shim = 1;
        if (!strcmp(command_line + offset, "--ready-fd")) has_ready_fd = 1;
        if (strstr(command_line + offset, "{helper_kind}")) matches_kind = 1;
        offset += strlen(command_line + offset) + 1;
    }}
    return is_shim && !has_ready_fd && matches_kind;
}}

__attribute__((constructor)) static void initialize(void) {{
    if (!is_target_helper(getpid())) return;
    int output = open("{}", O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (output >= 0) {{
        dprintf(output, "%d\n", getpid());
        close(output);
    }}
    raise(SIGSTOP);
}}

int kill(pid_t pid, int signal) {{
    static int (*original)(pid_t, int);
    if (!original) original = dlsym(RTLD_NEXT, "kill");
    if (pid > 0 && signal == SIGKILL && is_target_helper(pid)) {{
        errno = EPERM;
        return -1;
    }}
    return original(pid, signal);
}}
"#,
            helper_pid.display()
        ),
    )
    .unwrap();
    let status = Command::new("cc")
        .args(["-shared", "-fPIC"])
        .arg(&source)
        .arg("-o")
        .arg(&library)
        .arg("-ldl")
        .status()
        .unwrap();
    assert!(
        status.success(),
        "could not build signal-helper fault library"
    );
    library
}

fn build_partial_helper_capture_fault_library(root: &Path) -> PathBuf {
    let source = root.join("partial-helper-capture-fault.c");
    let library = root.join("partial-helper-capture-fault.so");
    fs::write(
        &source,
        r#"#define _GNU_SOURCE
#include <dirent.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static __thread DIR *active_scan;
static int is_owner;
static int faults_supervisor;
static int faults_own_root;

static int is_target_helper(int pid);

__attribute__((constructor)) static void initialize(void) {
    const char *root = getenv("JOB_RUNNER_PARTIAL_CAPTURE_ROOT");
    if (!root) {
        char pid[32];
        snprintf(pid, sizeof(pid), "%d", getpid());
        setenv("JOB_RUNNER_PARTIAL_CAPTURE_ROOT", pid, 1);
        is_owner = 1;
    } else {
        is_owner = atoi(root) == getpid();
    }
    faults_supervisor = is_owner && !getenv("JOB_RUNNER_PARTIAL_CAPTURE_OWN_ROOT");
    faults_own_root = getenv("JOB_RUNNER_PARTIAL_CAPTURE_OWN_ROOT")
        && is_target_helper(getpid());
}

DIR *opendir(const char *path) {
    static DIR *(*original)(const char *);
    if (!original) original = dlsym(RTLD_NEXT, "opendir");
    DIR *directory = original(path);
    if ((faults_supervisor || faults_own_root) && !strcmp(path, "/proc")) {
        active_scan = directory;
    }
    return directory;
}

int closedir(DIR *directory) {
    static int (*original)(DIR *);
    if (!original) original = dlsym(RTLD_NEXT, "closedir");
    if (directory == active_scan) active_scan = 0;
    return original(directory);
}

static int is_target_helper(int pid) {
    char path[64];
    char command_line[8192];
    snprintf(path, sizeof(path), "/proc/%d/cmdline", pid);
    int descriptor = open(path, O_RDONLY);
    if (descriptor < 0) return 0;
    ssize_t length = read(descriptor, command_line, sizeof(command_line));
    close(descriptor);
    if (length <= 0) return 0;
    const char *helper_kind = getenv("JOB_RUNNER_PARTIAL_CAPTURE_HELPER");
    int is_shim = 0;
    int has_ready_fd = 0;
    int matches_kind = 0;
    for (ssize_t offset = 0; offset < length;) {
        const char *argument = command_line + offset;
        size_t argument_length = strnlen(argument, length - offset);
        if (!strcmp(argument, "--job-shim")) is_shim = 1;
        if (!strcmp(argument, "--ready-fd")) has_ready_fd = 1;
        if (helper_kind && strstr(argument, helper_kind)) matches_kind = 1;
        offset += argument_length + 1;
    }
    int include_ready = getenv("JOB_RUNNER_PARTIAL_CAPTURE_INCLUDE_READY") != 0;
    return is_shim && (include_ready || !has_ready_fd) && matches_kind;
}

int open64(const char *path, int flags, ...) {
    static int (*original)(const char *, int, ...);
    if (!original) original = dlsym(RTLD_NEXT, "open64");
    mode_t mode = 0;
    if (flags & O_CREAT) {
        va_list arguments;
        va_start(arguments, flags);
        mode = va_arg(arguments, int);
        va_end(arguments);
    }
    int pid;
    char tail;
    if (active_scan && sscanf(path, "/proc/%d/stat%c", &pid, &tail) == 1
        && (faults_own_root ? pid == getpid() : is_target_helper(pid))) {
        const char *marker = getenv("JOB_RUNNER_PARTIAL_CAPTURE_FAULT_MARKER");
        if (marker) {
            int output = open(marker, O_WRONLY | O_CREAT | O_APPEND, 0666);
            if (output >= 0) {
                dprintf(output, "%d\n", getpid());
                close(output);
            }
        }
        errno = EIO;
        return -1;
    }
    return original(path, flags, mode);
}
"#,
    )
    .unwrap();
    let status = Command::new("cc")
        .args(["-shared", "-fPIC"])
        .arg(&source)
        .arg("-o")
        .arg(&library)
        .arg("-ldl")
        .status()
        .unwrap();
    assert!(
        status.success(),
        "could not build partial helper capture fault library"
    );
    library
}

fn build_signal_probe_named_sudo(root: &Path) -> PathBuf {
    let source = root.join("signal-probe.c");
    let executable = root.join("sudo");
    fs::write(
        &source,
        r#"#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

static unsigned long long process_start_time(void) {
    char stat[4096];
    int input = open("/proc/self/stat", O_RDONLY);
    if (input < 0) return 0;
    ssize_t length = read(input, stat, sizeof(stat) - 1);
    close(input);
    if (length <= 0) return 0;
    stat[length] = 0;
    char *field = strrchr(stat, ')');
    if (!field) return 0;
    field += 2;
    for (int index = 0; index < 19; index++) {
        field = strchr(field, ' ');
        if (!field) return 0;
        field++;
    }
    return strtoull(field, 0, 10);
}

int main(int argc, char **argv) {
    if (argc != 2) return 2;
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGUSR1);
    if (sigprocmask(SIG_BLOCK, &signals, 0) != 0) return 3;
    pid_t child = fork();
    if (child < 0) return 4;
    if (child == 0) {
        execlp("sleep", "sleep", "30", NULL);
        _exit(6);
    }
    int output = open(argv[1], O_WRONLY | O_CREAT | O_APPEND, 0666);
    if (output < 0) return 7;
    unsigned long long start_time = process_start_time();
    if (!start_time) return 9;
    dprintf(output, "%d %llu\nready\n", getpid(), start_time);
    int received = 0;
    if (sigwait(&signals, &received) != 0) return 8;
    dprintf(output, "received %d\n", received);
    kill(child, SIGKILL);
    waitpid(child, NULL, 0);
    close(output);
    return 0;
}
"#,
    )
    .unwrap();
    let status = Command::new("cc")
        .arg(&source)
        .arg("-o")
        .arg(&executable)
        .status()
        .unwrap();
    assert!(status.success(), "could not build signal probe");
    executable
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

// Callers must take the child path directly to libc syscalls because other test threads may hold locks.
fn fork_with_retry() -> libc::pid_t {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let pid = unsafe { libc::fork() };
        if pid >= 0 {
            return pid;
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EAGAIN) || Instant::now() >= deadline {
            panic!("fork failed: {error}");
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn read_pids(paths: &[PathBuf]) -> Vec<u32> {
    paths
        .iter()
        .flat_map(|path| support::observe_pid_file(path))
        .collect()
}

fn read_recorded_process_identity(path: &Path) -> Option<(u32, u64)> {
    let contents = fs::read_to_string(path).ok()?;
    let mut fields = contents.lines().next()?.split_whitespace();
    Some((fields.next()?.parse().ok()?, fields.next()?.parse().ok()?))
}

fn first_started_pid(log: &str) -> Option<u32> {
    log.split_once("Started (pid ")?
        .1
        .split_once(')')?
        .0
        .parse()
        .ok()
}

fn process_state(pid: u32) -> Option<char> {
    let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return None;
    };
    let close = stat.rfind(')')?;
    stat[close + 1..]
        .split_whitespace()
        .next()
        .and_then(|state| state.chars().next())
}

fn process_is_running(pid: u32) -> bool {
    process_state(pid).is_some_and(|state| state != 'Z')
}

fn process_group_id(pid: u32) -> Option<u32> {
    process_group_and_start_time(pid).map(|(process_group, _)| process_group)
}

fn process_group_and_start_time(pid: u32) -> Option<(u32, u64)> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let mut fields = stat[stat.rfind(')')? + 1..].split_whitespace();
    let process_group = fields.nth(2)?.parse().ok()?;
    let start_time = fields.nth(16)?.parse().ok()?;
    Some((process_group, start_time))
}

fn process_group_for_identity(pid: u32, start_time: u64) -> Option<u32> {
    let (process_group, current_start_time) = process_group_and_start_time(pid)?;
    (current_start_time == start_time).then_some(process_group)
}

fn parent_process_id(pid: u32) -> Option<u32> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat[stat.rfind(')')? + 1..]
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

fn zombie_child_count(parent_pid: u32) -> usize {
    let Ok(entries) = fs::read_dir("/proc") else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
        .filter_map(|pid| fs::read(format!("/proc/{pid}/stat")).ok())
        .filter_map(|stat| {
            let close = stat.iter().rposition(|byte| *byte == b')')?;
            let mut fields = stat[close + 1..]
                .split(u8::is_ascii_whitespace)
                .filter(|field| !field.is_empty());
            let state = *fields.next()?.first()?;
            let parent = std::str::from_utf8(fields.next()?)
                .ok()?
                .parse::<u32>()
                .ok()?;
            Some((state, parent))
        })
        .filter(|(state, parent)| *state == b'Z' && *parent == parent_pid)
        .count()
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

fn process_read_syscalls(pid: u32) -> Option<u64> {
    fs::read_to_string(format!("/proc/{pid}/io"))
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("syscr:"))?
        .trim()
        .parse()
        .ok()
}

fn process_cpu_ticks(pid: u32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let mut fields = stat[stat.rfind(')')? + 1..].split_whitespace();
    let user = fields.nth(11)?.parse::<u64>().ok()?;
    let system = fields.next()?.parse::<u64>().ok()?;
    Some(user + system)
}

fn process_voluntary_context_switches(pid: u32) -> Option<u64> {
    fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("voluntary_ctxt_switches:"))?
        .trim()
        .parse()
        .ok()
}

fn cpu_time_for_ticks(ticks: u64) -> Duration {
    let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    assert!(
        ticks_per_second > 0,
        "could not read the system clock tick rate"
    );
    Duration::from_secs_f64(ticks as f64 / ticks_per_second as f64)
}

fn current_username() -> Option<String> {
    let username = Command::new("id")
        .arg("-un")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|username| !username.is_empty());
    require_test_prerequisite(username.is_some(), "current username");
    username
}

fn require_test_prerequisite(available: bool, description: &str) -> bool {
    assert!(
        available || std::env::var_os("JR_REQUIRE_TEST_PREREQUISITES").is_none(),
        "required test prerequisite unavailable: {description}"
    );
    available
}

fn sudo_available_for(username: &str) -> bool {
    let available = Command::new("sudo")
        .args(["-n", "-u", username, "--", "true"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    require_test_prerequisite(available, &format!("noninteractive sudo for {username}"))
}

fn real_sudo_path() -> String {
    let sudo_output = Command::new("which").arg("sudo").output().unwrap();
    assert!(sudo_output.status.success());
    String::from_utf8_lossy(&sudo_output.stdout)
        .trim()
        .to_string()
}

fn install_fake_sudo(root: &Path, script: &str) -> String {
    let fake_bin = root.join("bin");
    fs::create_dir(&fake_bin).unwrap();
    write_executable(&fake_bin.join("sudo"), script);
    format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    )
}

fn install_failing_signal_sudo(root: &Path) -> String {
    let real_sudo = real_sudo_path();
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nfor argument in \"$@\"; do\n  case \"$argument\" in\n    job-runner-signal|job-runner-reconcile) exit 77 ;;\n  esac\ndone\nexec {} \"$@\"\n",
            real_sudo
        ),
    )
}

fn install_unready_cleanup_leaking_sudo(root: &Path, escapee_pid: &Path) -> String {
    let real_sudo = real_sudo_path();
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nfor argument in \"$@\"; do\n  if [ \"$argument\" = job-runner-signal ]; then\n    {real_sudo} \"$@\"\n    setsid sh -c 'echo $$ > {escapee_pid}; exec sleep 30' &\n    while [ ! -s {escapee_pid} ]; do sleep 0.01; done\n    kill -KILL \"$PPID\"\n    kill -KILL \"$$\"\n  fi\ndone\nexec {real_sudo} \"$@\"\n",
            escapee_pid = escapee_pid.display(),
        ),
    )
}

fn install_counting_signal_sudo(root: &Path, invocations: &Path) -> String {
    let real_sudo = real_sudo_path();
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nfor argument in \"$@\"; do\n  if [ \"$argument\" = job-runner-signal ]; then\n    echo invocation >> {}\n    break\n  fi\ndone\nexec {} \"$@\"\n",
            invocations.display(),
            real_sudo
        ),
    )
}

fn install_hanging_reconcile_sudo(
    root: &Path,
    helper_pid: &Path,
    helper_descendant_pid: &Path,
) -> String {
    let real_sudo = real_sudo_path();
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nfor argument in \"$@\"; do\n  if [ \"$argument\" = job-runner-reconcile ]; then\n    echo $$ > {helper_pid}\n    setsid sh -c 'echo $$ > {helper_descendant_pid}; exec sleep 30' &\n    wait\n    exit 0\n  fi\ndone\nexec {real_sudo} \"$@\"\n",
            helper_pid = helper_pid.display(),
            helper_descendant_pid = helper_descendant_pid.display(),
        ),
    )
}

fn install_cross_uid_helper_escape_sudo(
    root: &Path,
    privileged_descendant: &Path,
    reconcile_escapee: &Path,
    invocations: &Path,
) -> String {
    let real_sudo = real_sudo_path();
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\necho \"$*\" >> {invocations}\nsignal=0\nusr1=0\nreconcile=0\nfor argument in \"$@\"; do\n  [ \"$argument\" = job-runner-signal ] && signal=1\n  [ \"$argument\" = -USR1 ] && usr1=1\n  [ \"$argument\" = job-runner-reconcile ] && reconcile=1\ndone\nif [ \"$signal\" -eq 1 ] && [ \"$usr1\" -eq 1 ]; then\n  exec {real_sudo} -n -u root -- sh -c 'setsid sleep 30 & echo $! > {privileged_descendant}; wait'\nfi\nif [ \"$reconcile\" -eq 1 ]; then\n  setsid sh -c 'echo $$ > {reconcile_escapee}; exec sleep 30' &\n  while [ ! -s {reconcile_escapee} ]; do sleep 0.01; done\nfi\nexec {real_sudo} \"$@\"\n",
            privileged_descendant = privileged_descendant.display(),
            reconcile_escapee = reconcile_escapee.display(),
            invocations = invocations.display(),
        ),
    )
}

fn install_expiring_privileged_helper_sudo(root: &Path, privileged_pid: &Path) -> String {
    let real_sudo = real_sudo_path();
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nsignal=0\nusr1=0\nfor argument in \"$@\"; do\n  [ \"$argument\" = job-runner-signal ] && signal=1\n  [ \"$argument\" = -USR1 ] && usr1=1\ndone\nif [ \"$signal\" -eq 1 ] && [ \"$usr1\" -eq 1 ]; then\n  exec {real_sudo} -n -u root -- sh -c 'setsid sleep 1.8 & echo $! > {privileged_pid}; wait'\nfi\nexec {real_sudo} \"$@\"\n",
            privileged_pid = privileged_pid.display(),
        ),
    )
}

fn install_partial_capture_signal_sudo(
    root: &Path,
    escapee_pid: &Path,
    wait_for_escapee_exit: bool,
) -> String {
    let real_sudo = real_sudo_path();
    let helper_exit_command = if wait_for_escapee_exit {
        "wait".to_string()
    } else {
        format!(
            "while [ ! -s {} ]; do sleep 0.01; done",
            escapee_pid.display()
        )
    };
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nhelper=0\nusr1=0\nfor argument in \"$@\"; do\n  [ \"$argument\" = job-runner-signal ] && helper=1\n  [ \"$argument\" = -USR1 ] && usr1=1\ndone\nif [ \"$helper\" -eq 1 ] && [ \"$usr1\" -eq 1 ]; then\n  setsid sh -c 'echo $$ > {escapee_pid}; exec sleep 30' &\n  {helper_exit_command}\n  exit 0\nfi\nexec {real_sudo} \"$@\"\n",
            escapee_pid = escapee_pid.display(),
        ),
    )
}

fn install_partial_capture_reconcile_sudo(
    root: &Path,
    privileged_pid: &Path,
    escapee_pid: &Path,
) -> String {
    let real_sudo = real_sudo_path();
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nsignal=0\nusr1=0\nreconcile=0\nfor argument in \"$@\"; do\n  [ \"$argument\" = job-runner-signal ] && signal=1\n  [ \"$argument\" = -USR1 ] && usr1=1\n  [ \"$argument\" = job-runner-reconcile ] && reconcile=1\ndone\nif [ \"$signal\" -eq 1 ] && [ \"$usr1\" -eq 1 ]; then\n  exec {real_sudo} -n -u root -- sh -c 'setsid sleep 1.8 & echo $! > {privileged_pid}; wait'\nfi\nif [ \"$reconcile\" -eq 1 ]; then\n  setsid sh -c 'echo $$ > {escapee_pid}; exec sleep 30' &\n  wait\n  exit 0\nfi\nexec {real_sudo} \"$@\"\n",
            privileged_pid = privileged_pid.display(),
            escapee_pid = escapee_pid.display(),
        ),
    )
}

fn install_killing_reconcile_sudo(root: &Path) -> String {
    let real_sudo = real_sudo_path();
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nfor argument in \"$@\"; do\n  if [ \"$argument\" = job-runner-reconcile ]; then\n    {real_sudo} \"$@\"\n    kill -KILL \"$PPID\"\n    kill -KILL \"$$\"\n  fi\ndone\nexec {real_sudo} \"$@\"\n",
        ),
    )
}

fn install_leaking_cross_uid_reconcile_sudo(
    root: &Path,
    privileged_descendant: &Path,
    reconcile_escapee: &Path,
    reconcile_marker: &Path,
) -> String {
    let real_sudo = real_sudo_path();
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nsignal=0\nusr1=0\nreconcile=0\nfor argument in \"$@\"; do\n  [ \"$argument\" = job-runner-signal ] && signal=1\n  [ \"$argument\" = -USR1 ] && usr1=1\n  [ \"$argument\" = job-runner-reconcile ] && reconcile=1\ndone\nif [ \"$signal\" -eq 1 ] && [ \"$usr1\" -eq 1 ]; then\n  exec {real_sudo} -n -u root -- sh -c 'setsid sleep 30 & echo $! > {privileged_descendant}; wait'\nfi\nif [ \"$reconcile\" -eq 1 ]; then\n  touch {reconcile_marker}\n  {real_sudo} \"$@\"\n  exec {real_sudo} -n -u root -- sh -c 'setsid sleep 30 & echo $! >> {reconcile_escapee}; wait'\nfi\nexec {real_sudo} \"$@\"\n",
            privileged_descendant = privileged_descendant.display(),
            reconcile_escapee = reconcile_escapee.display(),
            reconcile_marker = reconcile_marker.display(),
        ),
    )
}

fn install_hanging_term_signal_sudo(root: &Path, marker: Option<&Path>) -> String {
    let real_sudo = real_sudo_path();
    let mark_started = marker
        .map(|path| format!("echo $$ > {}", path.display()))
        .unwrap_or_else(|| ":".to_string());
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nhelper=0\nterm=0\nfor argument in \"$@\"; do\n  [ \"$argument\" = job-runner-signal ] && helper=1\n  [ \"$argument\" = -TERM ] && term=1\ndone\nif [ \"$helper\" -eq 1 ] && [ \"$term\" -eq 1 ]; then {mark_started}; exec sleep 30; fi\nexec {real_sudo} \"$@\"\n",
        ),
    )
}

fn install_hanging_usr1_signal_sudo(root: &Path) -> String {
    let real_sudo = real_sudo_path();
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nhelper=0\nusr1=0\nfor argument in \"$@\"; do\n  [ \"$argument\" = job-runner-signal ] && helper=1\n  [ \"$argument\" = -USR1 ] && usr1=1\ndone\nif [ \"$helper\" -eq 1 ] && [ \"$usr1\" -eq 1 ]; then exec sleep 30; fi\nexec {real_sudo} \"$@\"\n",
        ),
    )
}

fn install_counting_hanging_usr1_signal_sudo(root: &Path, invocations: &Path) -> String {
    let real_sudo = real_sudo_path();
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nhelper=0\nusr1=0\nfor argument in \"$@\"; do\n  [ \"$argument\" = job-runner-signal ] && helper=1\n  [ \"$argument\" = -USR1 ] && usr1=1\ndone\nif [ \"$helper\" -eq 1 ] && [ \"$usr1\" -eq 1 ]; then\n  echo $$ >> {invocations}\n  exec sleep 30\nfi\nexec {real_sudo} \"$@\"\n",
            invocations = invocations.display(),
        ),
    )
}

fn install_forking_usr1_signal_sudo(root: &Path, helper_pid: &Path) -> String {
    let real_sudo = real_sudo_path();
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nhelper=0\nusr1=0\nfor argument in \"$@\"; do\n  [ \"$argument\" = job-runner-signal ] && helper=1\n  [ \"$argument\" = -USR1 ] && usr1=1\ndone\nif [ \"$helper\" -eq 1 ] && [ \"$usr1\" -eq 1 ]; then\n  sleep 30 &\n  echo $! > {helper_pid}\n  wait\n  exit 0\nfi\nexec {real_sudo} \"$@\"\n",
            helper_pid = helper_pid.display(),
        ),
    )
}

fn install_exiting_forking_usr1_signal_sudo(root: &Path, helper_pid: &Path) -> String {
    let real_sudo = real_sudo_path();
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nhelper=0\nusr1=0\nfor argument in \"$@\"; do\n  [ \"$argument\" = job-runner-signal ] && helper=1\n  [ \"$argument\" = -USR1 ] && usr1=1\ndone\nif [ \"$helper\" -eq 1 ] && [ \"$usr1\" -eq 1 ]; then\n  setsid sleep 30 &\n  echo $! > {helper_pid}\n  kill -KILL $$\nfi\nexec {real_sudo} \"$@\"\n",
            helper_pid = helper_pid.display(),
        ),
    )
}

fn install_slow_signal_sudo(root: &Path, invocations: &Path) -> String {
    let real_sudo = real_sudo_path();
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nhelper=0\nsignal=\nfor argument in \"$@\"; do\n  [ \"$argument\" = job-runner-signal ] && helper=1\n  case \"$argument\" in -USR1|-USR2) signal=$argument ;; esac\ndone\nif [ \"$helper\" -eq 1 ] && [ -n \"$signal\" ]; then\n  echo \"$signal\" >> {invocations}\n  sleep 0.15\n  exit 0\nfi\nexec {real_sudo} \"$@\"\n",
            invocations = invocations.display(),
        ),
    )
}

fn install_fail_first_kill_sudo(root: &Path, invocations: &Path) -> String {
    let real_sudo = real_sudo_path();
    install_fake_sudo(
        root,
        &format!(
            "#!/bin/sh\nsignal=0\nkill=0\nfor argument in \"$@\"; do\n  [ \"$argument\" = job-runner-signal ] && signal=1\n  [ \"$argument\" = -KILL ] && kill=1\ndone\nif [ \"$signal\" -eq 1 ] && [ \"$kill\" -eq 1 ]; then\n  echo invocation >> {invocations}\n  [ \"$(wc -l < {invocations})\" -eq 1 ] && exit 77\nfi\nexec {real_sudo} \"$@\"\n",
            invocations = invocations.display(),
            real_sudo = real_sudo,
        ),
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
    }));
    let pid = read_pids(std::slice::from_ref(&pid_file))[0];
    let expected_uid = Command::new("id").args(["-u", username]).output().unwrap();
    assert!(expected_uid.status.success());
    let status = fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    let effective_uid = status
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap();
    assert_eq!(
        effective_uid,
        String::from_utf8_lossy(&expected_uid.stdout).trim()
    );
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
    let available = Command::new("docker")
        .args(["image", "inspect", "ubuntu:24.04"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    require_test_prerequisite(available, "Docker with the ubuntu:24.04 image")
}

fn container_exists(name: &str) -> bool {
    let output = Command::new("docker")
        .args(["container", "ls", "--all", "--format", "{{.Names}}"])
        .output()
        .expect("could not query Docker containers");
    assert!(
        output.status.success(),
        "Docker container query failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .any(|line| line == name)
}

fn unique_container_name(label: &str) -> String {
    format!(
        "job-runner-test-{label}-{}-{}",
        std::process::id(),
        CONTAINER_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

#[test]
fn non_restarting_jobs_finish_once_and_aggregate_failures() {
    for wrap in [false, true] {
        for command in ["exit 0", "exit 7", "kill -TERM $$"] {
            let temp = tempfile::tempdir().unwrap();
            let config = temp.path().join("jobs.yaml");
            let runs = temp.path().join("runs");
            let done = temp.path().join("done");
            fs::write(
                &config,
                format!(
                    "jobs:\n  first:\n    command: 'echo run >> {}; {}'\n    restart: false\n    restartDelay: 0\n  last:\n    command: 'touch {}'\n    delay: 200\n    restart: false\n  disabled:\n    command: 'exit 1'\n    disabled: true\n  empty:\n    command: ''\n",
                    runs.display(), command, done.display()
                ),
            )
            .unwrap();
            let args = if wrap { vec!["--wrap"] } else { vec![] };
            let mut runner = Runner::start(&config, temp.path().join("runner.log"), &args);
            let status = runner.wait(Duration::from_secs(5));
            assert_eq!(status.success(), command == "exit 0", "{}", runner.log());
            assert!(done.exists(), "another job was stopped: {}", runner.log());
            assert_eq!(fs::read_to_string(&runs).unwrap(), "run\n");
            assert!(!runner.log().contains("before restarting"));
        }
    }
}

#[test]
fn non_restarting_startup_failure_does_not_retry_or_stop_other_jobs() {
    for wrap in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("jobs.yaml");
        let done = temp.path().join("done");
        fs::write(
            &config,
            format!(
                "jobs:\n  invalid:\n    command: 'echo no'\n    cwd: '{}/missing'\n    restart: false\n  last:\n    command: 'touch {}'\n    delay: 200\n    restart: false\n",
                temp.path().display(), done.display()
            ),
        )
        .unwrap();
        let args = if wrap { vec!["--wrap"] } else { vec![] };
        let mut runner = Runner::start(&config, temp.path().join("runner.log"), &args);
        assert!(!runner.wait(Duration::from_secs(5)).success());
        assert!(done.exists());
        assert_eq!(
            runner
                .log()
                .matches("Working directory does not exist")
                .count(),
            1
        );
    }
}

#[test]
fn non_restarting_jobs_can_run_alongside_restarting_jobs() {
    for wrap in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("jobs.yaml");
        let once = temp.path().join("once");
        let repeated = temp.path().join("repeated");
        fs::write(
            &config,
            format!(
                "jobs:\n  once:\n    command: 'echo run >> {}; exit 1'\n    restart: false\n  repeated:\n    command: 'echo run >> {}'\n    restartDelay: 100\n",
                once.display(), repeated.display()
            ),
        )
        .unwrap();
        let args = if wrap { vec!["--wrap"] } else { vec![] };
        let mut runner = Runner::start(&config, temp.path().join("runner.log"), &args);
        assert!(
            wait_until(Duration::from_secs(5), || {
                fs::read_to_string(&repeated)
                    .unwrap_or_default()
                    .lines()
                    .count()
                    >= 3
            }),
            "{}",
            runner.log()
        );
        assert!(runner.is_running());
        assert_eq!(fs::read_to_string(&once).unwrap(), "run\n");
        runner.signal(libc::SIGTERM);
        let status = runner.wait(Duration::from_secs(5));
        // A wrapper's explicit shutdown keeps its existing successful exit contract.
        assert_eq!(status.success(), wrap, "{}", runner.log());
    }
}

#[test]
fn non_restarting_job_cleans_descendants_and_drains_output() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("jobs.yaml");
    let descendant = temp.path().join("descendant.pid");
    fs::write(
        &config,
        format!(
            "jobs:\n  once:\n    command: 'sleep 30 & echo $! > {}; printf final-output'\n    restart: false\n    shutdown_timeout: 0\n",
            descendant.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![descendant.clone()]);
    let mut runner = Runner::start(&config, temp.path().join("runner.log"), &[]);
    assert!(
        runner.wait(Duration::from_secs(5)).success(),
        "{}",
        runner.log()
    );
    assert!(runner.log().contains("final-output"));
    let pids = read_pids(&[descendant]);
    assert!(!pids.is_empty());
    assert!(pids.into_iter().all(|pid| !process_is_running(pid)));
}

#[test]
fn non_restarting_invalid_launch_inputs_fail_once_without_stopping_other_jobs() {
    for (field, value, diagnostic) in [
        ("sudo_user", serde_json::json!(""), "Invalid sudo_user"),
        (
            "sudo_user",
            serde_json::json!("root --preserve-env"),
            "Invalid sudo_user",
        ),
        (
            "env",
            serde_json::json!({"": "value"}),
            "Invalid environment variable name",
        ),
        (
            "env",
            serde_json::json!({"A=B": "value"}),
            "Invalid environment variable name",
        ),
        (
            "env",
            serde_json::json!({"A\0B": "value"}),
            "Invalid environment variable name",
        ),
        ("env", serde_json::json!({"KEY": "a\0b"}), "nul byte found"),
        ("command", serde_json::json!("echo bad\0"), "nul byte found"),
    ] {
        for wrap in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let config = temp.path().join("jobs.json");
            let forbidden = temp.path().join("forbidden");
            let done = temp.path().join("done");
            let mut invalid = serde_json::json!({
                "command": format!("touch {}", forbidden.display()),
                "restart": false,
                "restartDelay": 0
            });
            invalid[field] = value.clone();
            fs::write(
                &config,
                serde_json::json!({"jobs": {
                    "invalid": invalid,
                    "other": {"command": format!("touch {}", done.display()), "delay": 200, "restart": false}
                }})
                .to_string(),
            )
            .unwrap();
            let args = if wrap { vec!["--wrap"] } else { vec![] };
            let mut runner = Runner::start(&config, temp.path().join("runner.log"), &args);
            assert_eq!(
                runner.wait(Duration::from_secs(5)).code(),
                Some(1),
                "{}",
                runner.log()
            );
            assert_eq!(
                runner.log().matches(diagnostic).count(),
                1,
                "{}",
                runner.log()
            );
            assert!(done.exists(), "{}", runner.log());
            assert!(!forbidden.exists(), "{}", runner.log());
            assert!(!runner.log().contains("before restarting"));
            assert!(
                !runner.log().contains("shutting down active processes"),
                "{}",
                runner.log()
            );
        }
    }
}

#[test]
fn shim_command_start_failure_is_job_local() {
    for wrap in [false, true] {
        for missing_sudo in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let config = temp.path().join("jobs.json");
            let done = temp.path().join("done");
            let bin = temp.path().join("bin");
            fs::create_dir(&bin).unwrap();
            std::os::unix::fs::symlink("/bin/sh", bin.join("sh")).unwrap();
            let mut bad = serde_json::json!({"command": ":", "restart": false});
            if missing_sudo {
                bad["sudo_user"] = serde_json::json!("root");
            } else {
                bad["env"] = serde_json::json!({"PATH": "/nonexistent"});
            }
            fs::write(&config, serde_json::json!({"jobs": {
                "bad": bad,
                "other": {"command": format!("printf done > {}", done.display()), "delay": 300, "restart": false}
            }}).to_string()).unwrap();
            let args = if wrap { vec!["--wrap"] } else { vec![] };
            let env = if missing_sudo {
                vec![("PATH", bin.to_str().unwrap())]
            } else {
                vec![]
            };
            let mut runner = Runner::start_with_env(&config, temp.path().join("log"), &args, &env);
            assert_eq!(
                runner.wait(Duration::from_secs(5)).code(),
                Some(1),
                "{}",
                runner.log()
            );
            assert!(done.exists(), "{}", runner.log());
            assert_eq!(
                runner
                    .log()
                    .matches("Job shim could not start command")
                    .count(),
                1,
                "{}",
                runner.log()
            );
            assert!(
                !runner.log().contains("Fatal process supervision error"),
                "{}",
                runner.log()
            );
            assert!(!runner.log().contains("before restarting"));
        }
    }
}

#[test]
fn shim_command_start_failure_retries_only_the_failed_job() {
    for wrap in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("jobs.json");
        let done = temp.path().join("done");
        fs::write(&config, serde_json::json!({"jobs": {
            "bad": {"command": ":", "env": {"PATH": "/nonexistent"}, "restart_delay": 100},
            "other": {"command": format!("printf done >> {}", done.display()), "delay": 300, "restart": false}
        }}).to_string()).unwrap();
        let args = if wrap { vec!["--wrap"] } else { vec![] };
        let mut runner = Runner::start(&config, temp.path().join("log"), &args);
        assert!(
            wait_until(Duration::from_secs(5), || done.exists()
                && runner
                    .log()
                    .matches("Job shim could not start command")
                    .count()
                    >= 2),
            "{}",
            runner.log()
        );
        assert!(runner.is_running());
        runner.signal(libc::SIGTERM);
        assert!(
            runner.wait(Duration::from_secs(5)).success(),
            "{}",
            runner.log()
        );
        assert_eq!(fs::read_to_string(done).unwrap(), "done");
        assert!(
            !runner.log().contains("Fatal process supervision error"),
            "{}",
            runner.log()
        );
    }
}

#[test]
fn non_restarting_initial_delay_is_cancelled_by_shutdown() {
    for wrap in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("jobs.yaml");
        let started = temp.path().join("started");
        fs::write(&config, format!(
            "jobs:\n  delayed:\n    command: 'touch {}'\n    delay: 60000\n    restart: false\n",
            started.display()
        )).unwrap();
        let args = if wrap { vec!["--wrap"] } else { vec![] };
        let mut runner = Runner::start(&config, temp.path().join("runner.log"), &args);
        assert!(
            wait_until(Duration::from_secs(5), || runner
                .log()
                .contains("Starting in 60.000s")),
            "{}",
            runner.log()
        );
        runner.signal(libc::SIGTERM);
        assert!(
            runner.wait(Duration::from_secs(5)).success(),
            "{}",
            runner.log()
        );
        assert!(!started.exists());
        assert!(!runner.log().contains("Started (pid"));
    }
}

fn build_paused_wrapper_inner_library(root: &Path) -> PathBuf {
    let source = root.join("pause-inner.c");
    let library = root.join("pause-inner.so");
    fs::write(
        &source,
        r#"#define _GNU_SOURCE
#include <dirent.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

static int term_ack = -1;
static _Atomic int reaped_signal_child;

static void acknowledge_term(int signal) {
    (void) signal;
    write(term_ack, "term", 4);
}

__attribute__((constructor)) static void initialize(void) {
    if (!getenv("JR_TEST_WRAPPER_INITIALIZED")) {
        setenv("JR_TEST_WRAPPER_INITIALIZED", "1", 1);
        return;
    }
    struct rlimit limit = {0, 0};
    setrlimit(RLIMIT_CORE, &limit);
    const char *ack = getenv("JR_TEST_TERM_ACK");
    if (ack) {
        term_ack = open(ack, O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (term_ack < 0) _exit(99);
        signal(SIGTERM, acknowledge_term);
    }
    FILE *ready = fopen(getenv("JR_TEST_INNER_READY"), "w");
    if (!ready) _exit(99);
    fprintf(ready, "%d\n", getpid());
    fclose(ready);
    for (;;) pause();
}

pid_t waitpid(pid_t pid, int *status, int options) {
    static pid_t (*original)(pid_t, int *, int);
    if (!original) original = dlsym(RTLD_NEXT, "waitpid");
    pid_t result = original(pid, status, options);
    if (result > 0 && status && WIFSIGNALED(*status))
        atomic_store(&reaped_signal_child, 1);
    return result;
}

DIR *opendir(const char *path) {
    static DIR *(*original)(const char *);
    if (!original) original = dlsym(RTLD_NEXT, "opendir");
    const char *ready = getenv("JR_TEST_POST_REAP_READY");
    const char *release = getenv("JR_TEST_POST_REAP_RELEASE");
    if (ready && release && !strcmp(path, "/proc")
        && atomic_exchange(&reaped_signal_child, 0)) {
        int fd = open(ready, O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (fd < 0) _exit(99);
        close(fd);
        for (int i = 0; i < 500 && access(release, F_OK); i++) usleep(10000);
    }
    return original(path);
}
"#,
    )
    .unwrap();
    assert!(
        Command::new("cc")
            .args(["-shared", "-fPIC"])
            .arg(&source)
            .arg("-o")
            .arg(&library)
            .arg("-ldl")
            .status()
            .unwrap()
            .success()
    );
    library
}

#[test]
fn one_generation_wrapper_shutdown_before_inner_signal_handlers_is_successful() {
    let temp = tempfile::tempdir().unwrap();
    let library = build_paused_wrapper_inner_library(temp.path());
    for signal in [libc::SIGTERM, libc::SIGINT, libc::SIGHUP, libc::SIGQUIT] {
        for requested_shutdown in [true, false] {
            let config = temp.path().join("jobs.json");
            let ready = temp
                .path()
                .join(format!("inner-{signal}-{requested_shutdown}"));
            let started = temp.path().join("started");
            fs::write(
                &config,
                serde_json::json!({"jobs": {"job": {
                    "command": format!("touch {}", started.display()), "restart": false
                }}})
                .to_string(),
            )
            .unwrap();
            let mut runner = Runner::start_with_env(
                &config,
                temp.path().join("log"),
                &["--wrap"],
                &[
                    ("LD_PRELOAD", library.to_str().unwrap()),
                    ("JR_TEST_INNER_READY", ready.to_str().unwrap()),
                ],
            );
            assert!(
                wait_until(Duration::from_secs(5), || !read_pids(std::slice::from_ref(
                    &ready
                ))
                .is_empty()),
                "{}",
                runner.log()
            );
            let inner = support::ProcessIdentity::read(read_pids(&[ready])[0]).unwrap();
            assert_eq!(parent_process_id(inner.pid), Some(runner.child.id()));
            if requested_shutdown {
                runner.signal(signal);
            } else {
                inner.signal(signal, None);
            }
            assert_eq!(
                runner.wait(Duration::from_secs(5)).code(),
                Some(if requested_shutdown { 0 } else { 128 + signal }),
                "{}",
                runner.log()
            );
            assert!(!inner.running());
            assert!(!started.exists());
            assert!(runner.log().contains("Process terminated by signal"));
            assert_eq!(runner.log().matches("Starting inner process").count(), 1);
        }
    }
}

#[test]
fn one_generation_wrapper_preserves_unrequested_signal_during_shutdown() {
    let temp = tempfile::tempdir().unwrap();
    let library = build_paused_wrapper_inner_library(temp.path());
    for signal in [libc::SIGQUIT, libc::SIGKILL] {
        let config = temp.path().join("jobs.json");
        let ready = temp.path().join(format!("inner-{signal}"));
        let ack = temp.path().join(format!("term-ack-{signal}"));
        fs::write(
            &config,
            r#"{"jobs":{"job":{"command":"exit 99","restart":false}}}"#,
        )
        .unwrap();
        let mut runner = Runner::start_with_env(
            &config,
            temp.path().join("log"),
            &["--wrap"],
            &[
                ("LD_PRELOAD", library.to_str().unwrap()),
                ("JR_TEST_INNER_READY", ready.to_str().unwrap()),
                ("JR_TEST_TERM_ACK", ack.to_str().unwrap()),
            ],
        );
        assert!(
            wait_until(Duration::from_secs(5), || !read_pids(std::slice::from_ref(
                &ready
            ))
            .is_empty()),
            "{}",
            runner.log()
        );
        let inner = support::ProcessIdentity::read(read_pids(&[ready])[0]).unwrap();
        runner.signal(libc::SIGTERM);
        assert!(
            wait_until(Duration::from_secs(5), || fs::metadata(&ack)
                .is_ok_and(|metadata| metadata.len() > 0)),
            "{}",
            runner.log()
        );
        inner.signal(signal, None);
        assert_eq!(
            runner.wait(Duration::from_secs(5)).code(),
            Some(128 + signal),
            "{}",
            runner.log()
        );
        assert!(!inner.running());
        assert_eq!(runner.log().matches("Starting inner process").count(), 1);
    }
}

#[test]
fn one_generation_wrapper_late_shutdown_preserves_reaped_signal_failure() {
    let temp = tempfile::tempdir().unwrap();
    let library = build_paused_wrapper_inner_library(temp.path());
    let config = temp.path().join("jobs.json");
    let ready = temp.path().join("inner");
    let reaped = temp.path().join("reaped");
    let release = temp.path().join("release");
    fs::write(
        &config,
        r#"{"jobs":{"job":{"command":"exit 99","restart":false}}}"#,
    )
    .unwrap();
    let mut runner = Runner::start_with_env(
        &config,
        temp.path().join("log"),
        &["--wrap"],
        &[
            ("LD_PRELOAD", library.to_str().unwrap()),
            ("JR_TEST_INNER_READY", ready.to_str().unwrap()),
            ("JR_TEST_POST_REAP_READY", reaped.to_str().unwrap()),
            ("JR_TEST_POST_REAP_RELEASE", release.to_str().unwrap()),
        ],
    );
    assert!(
        wait_until(Duration::from_secs(5), || !read_pids(std::slice::from_ref(
            &ready
        ))
        .is_empty()),
        "{}",
        runner.log()
    );
    let inner = support::ProcessIdentity::read(read_pids(&[ready])[0]).unwrap();
    inner.signal(libc::SIGTERM, None);
    assert!(
        wait_until(Duration::from_secs(5), || reaped.exists()),
        "{}",
        runner.log()
    );
    assert_eq!(support::ProcessIdentity::read(inner.pid), None);
    runner.signal(libc::SIGTERM);
    assert!(
        wait_until(Duration::from_secs(5), || runner
            .log()
            .contains("Received termination signal")),
        "{}",
        runner.log()
    );
    fs::write(&release, "release").unwrap();
    assert_eq!(
        runner.wait(Duration::from_secs(5)).code(),
        Some(143),
        "{}",
        runner.log()
    );
    assert_eq!(runner.log().matches("Starting inner process").count(), 1);
}

#[test]
fn one_generation_wrapper_preserves_job_failures_during_shutdown() {
    for wrap in [false, true] {
        for failure in [0, 3] {
            let temp = tempfile::tempdir().unwrap();
            let config = temp.path().join("jobs.json");
            let pid_path = temp.path().join("active.pid");
            fs::write(&config, serde_json::json!({"jobs": {
                "finished": {"command": format!("exit {failure}"), "restart": false, "prefix": "finished: "},
                "active": {"command": format!("echo $$ > {}; exec sleep 30", pid_path.display()), "restart": false}
            }}).to_string()).unwrap();
            let _cleanup = ProcessCleanup(vec![pid_path.clone()]);
            let args = if wrap { vec!["--wrap"] } else { vec![] };
            let mut runner = Runner::start(&config, temp.path().join("log"), &args);
            assert!(
                wait_until(Duration::from_secs(5), || {
                    !read_pids(std::slice::from_ref(&pid_path)).is_empty()
                        && runner
                            .log()
                            .contains(&format!("finished: Exited with code {failure}"))
                }),
                "{}",
                runner.log()
            );
            let pids = read_pids(std::slice::from_ref(&pid_path));
            runner.signal(libc::SIGTERM);
            assert_eq!(
                runner.wait(Duration::from_secs(5)).code(),
                Some(i32::from(failure != 0)),
                "{}",
                runner.log()
            );
            assert!(pids.into_iter().all(|pid| !process_is_running(pid)));
            if wrap {
                assert_eq!(runner.log().matches("Starting inner process").count(), 1);
            }
        }
    }
}

#[test]
fn one_generation_wrapper_cleans_jobs_after_inner_sigkill_without_restarting() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("jobs.yaml");
    let shim = temp.path().join("shim.pid");
    let job = temp.path().join("job.pid");
    fs::write(&config, format!(
        "jobs:\n  once:\n    command: 'echo $PPID > {}; echo $$ > {}; exec sleep 30'\n    restart: false\n",
        shim.display(), job.display()
    )).unwrap();
    let paths = [shim, job];
    let _cleanup = ProcessCleanup(paths.to_vec());
    let mut runner = Runner::start(&config, temp.path().join("runner.log"), &["--wrap"]);
    assert!(
        wait_until(Duration::from_secs(5), || read_pids(&paths).len() == 2),
        "{}",
        runner.log()
    );
    let pids = read_pids(&paths);
    let inner = parent_process_id(pids[0]).unwrap();
    assert_eq!(parent_process_id(inner), Some(runner.child.id()));
    assert_eq!(
        unsafe { libc::kill(inner as libc::pid_t, libc::SIGKILL) },
        0
    );
    assert_eq!(
        runner.wait(Duration::from_secs(8)).code(),
        Some(137),
        "{}",
        runner.log()
    );
    assert!(
        pids.iter().all(|pid| !process_is_running(*pid)),
        "{}",
        runner.log()
    );
    assert_eq!(
        runner
            .log()
            .matches("[Wrapper] Starting inner process..")
            .count(),
        1
    );
    assert!(!runner.log().contains("before restarting"));
}

#[test]
fn invalid_configuration_is_rejected_before_any_job_starts() {
    for yaml in [false, true] {
        for wrap in [false, true] {
            for invalid in [
                serde_json::json!({"typo": true, "jobs": {}}),
                serde_json::json!({"jobs": {"bad": {"command": ":", "restatr": false}}}),
                serde_json::json!({"jobs": {"bad": {"command": ":", "datetime": {"enabeld": true}}}}),
                serde_json::json!({"jobs": {"bad": {"command": ":", "restart_delay": 1, "restartDelay": 2}}}),
                serde_json::json!({"jobs": {"bad": {"command": {"shell": ":"}}}}),
                serde_json::json!({"jobs": {"bad": {"command": ":", "restart": "false"}}}),
            ] {
                let temp = tempfile::tempdir().unwrap();
                let config = temp
                    .path()
                    .join(if yaml { "jobs.yaml" } else { "jobs.json" });
                let marker = temp.path().join("forbidden");
                let mut invalid = invalid;
                invalid["jobs"]["aaa_valid"] = serde_json::json!({"command": format!("touch {}", marker.display()), "restart": false});
                let text = if yaml {
                    serde_yaml::to_string(&invalid).unwrap()
                } else {
                    invalid.to_string()
                };
                fs::write(&config, text).unwrap();
                let args = if wrap { vec!["--wrap"] } else { vec![] };
                let mut runner = Runner::start(&config, temp.path().join("log"), &args);
                assert_eq!(
                    runner.wait(Duration::from_secs(5)).code(),
                    Some(1),
                    "{}",
                    runner.log()
                );
                assert!(runner.log().contains("Could not parse"), "{}", runner.log());
                assert!(!runner.log().contains("Initializing job"));
                assert!(!runner.log().contains("Starting inner process"));
                assert!(!marker.exists());
            }
        }
    }
}

#[test]
fn ambiguous_or_invalid_cli_inputs_never_start_a_valid_config() {
    for wrap in [false, true] {
        for invalid_args in [vec!["extra.yaml"], vec!["--unknown"], vec!["-t", "0"]] {
            let temp = tempfile::tempdir().unwrap();
            let marker = temp.path().join("forbidden");
            let config = temp.path().join("jobs.json");
            fs::write(
                &config,
                serde_json::json!({"jobs": {"job": {
                    "command": format!("touch {}", marker.display()), "restart": false
                }}})
                .to_string(),
            )
            .unwrap();
            fs::copy(&config, temp.path().join("extra.yaml")).unwrap();
            let log_path = temp.path().join("log");
            let log = File::create(&log_path).unwrap();
            let mut command = Command::new(env!("CARGO_BIN_EXE_jr"));
            command
                .current_dir(temp.path())
                .stdout(log.try_clone().unwrap())
                .stderr(log);
            if wrap {
                command.arg("--wrap");
            }
            command.arg(config).args(&invalid_args);
            let mut runner = Runner::spawn_in_session(command, log_path);
            assert_eq!(
                runner.wait(Duration::from_secs(5)).code(),
                Some(1),
                "{}",
                runner.log()
            );
            let expected = match invalid_args[0] {
                "extra.yaml" => "Only one config file",
                "--unknown" => "Unknown option",
                _ => "positive integer",
            };
            assert!(runner.log().contains(expected), "{}", runner.log());
            assert!(!marker.exists());
            assert!(!runner.log().contains("Starting inner process"));
        }
    }
}

#[test]
fn configurations_without_active_jobs_exit_successfully() {
    for config_text in [
        "jobs: {}\n",
        "jobs:\n  disabled:\n    command: exit 1\n    disabled: true\n    prefix: '{unknown}'\n    datetime: {enabled: true, format: '%Q'}\n  empty:\n    command: ''\n    prefix: '}'\n    shutdownTimeout: 18446744073709551615\n",
    ] {
        for wrap in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let config = temp.path().join("jobs.yaml");
            fs::write(&config, config_text).unwrap();
            let args = if wrap { vec!["--wrap"] } else { vec![] };
            let mut runner = Runner::start(&config, temp.path().join("runner.log"), &args);
            assert!(
                runner.wait(Duration::from_secs(5)).success(),
                "{}",
                runner.log()
            );
            assert!(!runner.log().contains("Initializing job"));
            assert!(!runner.log().contains("before restarting"));
            if config_text.contains("disabled:") {
                assert!(runner.log().contains("Skipping disabled job: disabled"));
                assert!(
                    runner
                        .log()
                        .contains("Skipping job with missing command: empty")
                );
            }
        }
    }
}

#[test]
fn wrapper_reads_commented_json_and_extensionless_configs_with_thread_options() {
    for extension in ["json", "conf", ""] {
        for threads_after_config in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let config = temp.path().join(if extension.is_empty() {
                "jobs".to_string()
            } else {
                format!("jobs.{extension}")
            });
            let runs = temp.path().join("runs");
            fs::write(&config, format!("// whole-line comment\n{}\n", serde_json::json!({"jobs": {"once": {
                "command": format!("echo run >> {}; printf '%s' 'https://example.test/#fragment'", runs.display()),
                "restart": false,
                "prefix": "{stream}: "
            }}}))).unwrap();
            let log_path = temp.path().join("runner.log");
            let log = File::create(&log_path).unwrap();
            let mut command = Command::new(env!("CARGO_BIN_EXE_jr"));
            command
                .arg("--wrap")
                .stdout(log.try_clone().unwrap())
                .stderr(log);
            if threads_after_config {
                command.arg(&config).args(["-t", "2"]);
            } else {
                command.args(["-t", "2"]).arg(&config);
            }
            let mut runner = Runner::spawn_in_session(command, log_path);
            assert!(
                runner.wait(Duration::from_secs(5)).success(),
                "{}",
                runner.log()
            );
            assert_eq!(fs::read_to_string(runs).unwrap(), "run\n");
            assert!(
                runner
                    .log()
                    .lines()
                    .any(|line| line == "1: https://example.test/#fragment")
            );
            assert!(!runner.log().contains("before restarting"));
        }
    }
}

#[test]
fn output_reader_preserves_complete_payloads_at_buffer_and_frame_boundaries() {
    for length in [8191, 8192, 8193, 65535, 65536, 65537, 131072] {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("jobs.json");
        let payload_path = temp.path().join("payload");
        let stdout_path = temp.path().join("stdout");
        let stderr_path = temp.path().join("stderr");
        let payload: Vec<_> = (0..length).map(|i| [b'a', 0, 255][i % 3]).collect();
        let mut input = payload.clone();
        input.extend_from_slice(b"\n\ntail");
        fs::write(&payload_path, input).unwrap();
        fs::write(
            &config,
            serde_json::json!({"jobs": {"binary": {
                "command": format!("cat {0}; cat {0} >&2", payload_path.display()),
                "prefix": "{stream}>", "restart": false
            }}})
            .to_string(),
        )
        .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_jr"));
        command
            .arg(config)
            .stdout(File::create(&stdout_path).unwrap())
            .stderr(File::create(&stderr_path).unwrap());
        let mut runner = Runner::spawn_in_session(command, stderr_path.clone());
        assert!(runner.wait(Duration::from_secs(5)).success());
        for (stream, path) in [(b'1', stdout_path), (b'2', stderr_path)] {
            let mut expected = Vec::new();
            for reader_chunk in payload.chunks(65536) {
                for frame in reader_chunk.chunks(65533) {
                    expected.extend_from_slice(&[stream, b'>']);
                    expected.extend_from_slice(frame);
                    expected.push(b'\n');
                }
            }
            expected.extend_from_slice(&[stream, b'>', b'\n', stream, b'>']);
            expected.extend_from_slice(b"tail\n");
            let actual = fs::read(path).unwrap();
            let mut job_bytes = Vec::new();
            for line in actual.split_inclusive(|byte| *byte == b'\n') {
                if line.starts_with(&[stream, b'>']) {
                    job_bytes.extend_from_slice(line);
                } else {
                    assert!(
                        stream == b'2'
                            && (line.starts_with(b"Initializing job ")
                                || line.starts_with(b"s>Started (pid ")
                                || line == b"s>Exited with code 0\n"),
                        "unexpected output: {line:?}"
                    );
                }
            }
            assert_eq!(job_bytes, expected, "length={length}, stream={stream}");
        }
    }
}

#[test]
fn closed_output_sinks_do_not_prevent_completion_or_descendant_cleanup() {
    for stderr in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("jobs.json");
        let pid_path = temp.path().join("child.pid");
        fs::write(&config, serde_json::json!({"jobs": {"writer": {
            "command": format!("sleep 30 & echo $! > {}; printf out; printf err >&2", pid_path.display()),
            "restart": false, "shutdown_timeout": 0
        }}}).to_string()).unwrap();
        let _cleanup = ProcessCleanup(vec![pid_path.clone()]);
        let mut runner = if stderr {
            Runner::start_with_unread_stderr(&config, temp.path().join("log"), &[])
        } else {
            Runner::start_with_unread_stdout(&config, temp.path().join("log"))
        };
        if stderr {
            drop(runner.child.stderr.take());
        } else {
            drop(runner.child.stdout.take());
        }
        assert!(
            runner.wait(Duration::from_secs(5)).success(),
            "{}",
            runner.log()
        );
        let pids = read_pids(&[pid_path]);
        assert_eq!(pids.len(), 1);
        assert!(pids.into_iter().all(|pid| !process_is_running(pid)));
    }
}

#[test]
fn output_readers_preserve_binary_bytes_blank_lines_and_unterminated_tails() {
    let temp = tempfile::tempdir().unwrap();
    let payload = temp.path().join("payload");
    let stdout_path = temp.path().join("stdout");
    let stderr_path = temp.path().join("stderr");
    let config = temp.path().join("jobs.json");
    fs::write(&payload, b"\n\0\xff\r\ntail").unwrap();
    fs::write(
        &config,
        serde_json::json!({"jobs": {"binary": {
            "command": format!("cat {0}; cat {0} >&2", payload.display()),
            "prefix": "{stream}>", "restart": false
        }}})
        .to_string(),
    )
    .unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_jr"));
    command
        .arg(config)
        .stdout(File::create(&stdout_path).unwrap())
        .stderr(File::create(&stderr_path).unwrap());
    let mut runner = Runner::spawn_in_session(command, stderr_path.clone());
    assert!(runner.wait(Duration::from_secs(5)).success());
    assert_eq!(fs::read(stdout_path).unwrap(), b"1>\n1>\0\xff\r\n1>tail\n");
    let stderr = fs::read(stderr_path).unwrap();
    let job_stderr: Vec<u8> = stderr
        .split_inclusive(|byte| *byte == b'\n')
        .filter(|line| line.starts_with(b"2>"))
        .flatten()
        .copied()
        .collect();
    assert_eq!(job_stderr, b"2>\n2>\0\xff\r\n2>tail\n");
}

#[test]
fn prefix_templates_apply_to_stdout_stderr_and_job_status() {
    for (template, expected) in [
        (
            "{color}<{job}:{stream}>{reset} {datetime}: ",
            "\x1b[31m<worker:STREAM>\x1b[0m STAMP: ",
        ),
        ("", ""),
        ("{job}", "worker"),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("jobs.json");
        fs::write(
            &config,
            serde_json::json!({"jobs": {"worker": {
                "command": "printf 'out\\n'; printf err >&2",
                "restart": false,
                "color": "red",
                "prefix": template,
                "datetime": {"enabled": true, "format": "STAMP"}
            }}})
            .to_string(),
        )
        .unwrap();
        let mut runner = Runner::start(&config, temp.path().join("runner.log"), &[]);
        assert!(
            runner.wait(Duration::from_secs(5)).success(),
            "{}",
            runner.log()
        );
        let output = runner.log();
        for (stream, message) in [("1", "out"), ("2", "err"), ("s", "Exited with code 0")] {
            let line = format!("{}{message}", expected.replace("STREAM", stream));
            assert!(
                output.lines().any(|actual| actual == line),
                "missing {line:?}: {output}"
            );
        }
        assert!(
            output.lines().any(|line| line
                .starts_with(&format!("{}Started (pid ", expected.replace("STREAM", "s")))),
            "{output}"
        );
    }
}

#[test]
fn prefix_automatic_color_is_stable_across_restarts() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("jobs.yaml");
    fs::write(
        &config,
        "jobs:\n  worker:\n    command: echo payload\n    restart_delay: 100\n    prefix: '{color}{job}{reset}/{stream}: '\n",
    )
    .unwrap();
    let mut runner = Runner::start(&config, temp.path().join("runner.log"), &[]);
    assert!(wait_until(Duration::from_secs(5), || runner
        .log()
        .matches("payload")
        .count()
        >= 3));
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
    let output = runner.log();
    let lines: Vec<_> = output
        .lines()
        .filter(|line| line.ends_with("payload"))
        .collect();
    assert!(lines.len() >= 3, "{output}");
    assert!(lines[0].starts_with("\x1b["));
    assert!(lines[0].ends_with("worker\x1b[0m/1: payload"));
    assert!(lines.iter().all(|line| *line == lines[0]));
}

#[test]
fn invalid_prefix_prevents_all_jobs_from_starting() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("jobs.yaml");
    let marker = temp.path().join("started");
    fs::write(
        &config,
        format!(
            "jobs:\n  first:\n    command: 'touch {}'\n    restart: false\n  invalid:\n    command: echo bad\n    restart: false\n    prefix: '{{unknown}}'\n",
            marker.display()
        ),
    )
    .unwrap();
    let mut runner = Runner::start(&config, temp.path().join("runner.log"), &[]);
    assert!(!runner.wait(Duration::from_secs(5)).success());
    assert!(runner.log().contains("invalid prefix"));
    assert!(!marker.exists());
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
fn job_restart_does_not_wait_for_periodic_reconciliation() {
    let temp = tempfile::tempdir().unwrap();
    let runs = temp.path().join("runs");
    let ready = temp.path().join("ready");
    let release = temp.path().join("release");
    let leaked_override = temp.path().join("leaked-override");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    fs::write(
        &config,
        format!(
            "jobs:\n  restart:\n    command: |\n      env | grep -q '^{override}=' && touch {leaked_override}\n      echo $$ >> {runs}\n      if [ \"$(wc -l < {runs})\" -eq 1 ]; then\n        touch {ready}\n        while [ ! -e {release} ]; do sleep 0.01; done\n      fi\n    restart_delay: 100\n",
            override = TEST_RECONCILE_INTERVAL_ENV,
            leaked_override = leaked_override.display(),
            runs = runs.display(),
            ready = ready.display(),
            release = release.display(),
        ),
    )
    .unwrap();

    let mut runner =
        Runner::start_with_env(&config, log, &[], &[(TEST_RECONCILE_INTERVAL_ENV, "30000")]);
    assert!(wait_until(Duration::from_secs(3), || ready.exists()));
    assert!(!leaked_override.exists());
    File::create(&release).unwrap();
    assert!(
        wait_until(Duration::from_secs(3), || {
            read_pids(std::slice::from_ref(&runs)).len() >= 2
        }),
        "job restart waited for process reconciliation: {}",
        runner.log()
    );
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(3)).success());
}

#[test]
fn child_identity_failure_does_not_overlap_job_generations() {
    let temp = tempfile::tempdir().unwrap();
    let runs = temp.path().join("runs");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    let fault_library = build_identity_fault_library(temp.path());
    fs::write(
        &config,
        format!(
            "jobs:\n  identity_failure:\n    command: \"echo $$ >> {}; exec sleep 30\"\n    restart_delay: 100\n",
            runs.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![runs.clone()]);

    let mut runner = Runner::start_with_env(
        &config,
        log,
        &[],
        &[
            ("LD_PRELOAD", fault_library.to_str().unwrap()),
            ("JOB_RUNNER_IDENTITY_FAULT_READY", runs.to_str().unwrap()),
        ],
    );
    assert!(
        wait_until(Duration::from_secs(5), || {
            read_pids(std::slice::from_ref(&runs)).len() >= 2
        }),
        "job did not restart after confirmed cleanup: {}",
        runner.log()
    );
    let pids = read_pids(std::slice::from_ref(&runs));
    assert!(
        !process_is_running(pids[0]),
        "identity-read failure overlapped job generations: {}",
        runner.log()
    );
    assert!(process_is_running(pids[1]));

    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
}

#[test]
fn forced_shutdown_interrupts_post_spawn_error_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let job_pid = temp.path().join("job.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    let fault_library = build_identity_fault_library(temp.path());
    fs::write(
        &config,
        format!(
            "jobs:\n  identity_failure:\n    command: \"trap '' TERM; echo $$ > {}; exec sleep 30\"\n    shutdown_timeout: 30\n",
            job_pid.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![job_pid.clone()]);

    let mut runner = Runner::start_with_env(
        &config,
        log,
        &[],
        &[
            ("LD_PRELOAD", fault_library.to_str().unwrap()),
            ("JOB_RUNNER_IDENTITY_FAULT_READY", job_pid.to_str().unwrap()),
        ],
    );
    assert!(wait_until(Duration::from_secs(3), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
    }));
    let pid = read_pids(std::slice::from_ref(&job_pid))[0];
    let started = Instant::now();
    runner.signal(libc::SIGTERM);
    thread::sleep(Duration::from_millis(100));
    runner.signal(libc::SIGTERM);

    assert!(!runner.wait(Duration::from_secs(4)).success());
    assert!(started.elapsed() < Duration::from_secs(4));
    assert!(!process_is_running(pid));
}

#[test]
fn post_spawn_error_respects_direct_process_signal_scope() {
    let temp = tempfile::tempdir().unwrap();
    let shell_pid = temp.path().join("shell.pid");
    let child_pid = temp.path().join("child.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    let fault_library = build_identity_fault_library(temp.path());
    fs::write(
        &config,
        format!(
            "jobs:\n  direct:\n    command: \"sleep 30 & echo $! > {child}; echo $$ > {shell}; wait\"\n    process_group: false\n    restart_delay: 30000\n",
            child = child_pid.display(),
            shell = shell_pid.display(),
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![child_pid.clone()]);

    let mut runner = Runner::start_with_env(
        &config,
        log,
        &[],
        &[
            ("LD_PRELOAD", fault_library.to_str().unwrap()),
            (
                "JOB_RUNNER_IDENTITY_FAULT_READY",
                child_pid.to_str().unwrap(),
            ),
        ],
    );
    assert!(wait_until(Duration::from_secs(3), || {
        !read_pids(std::slice::from_ref(&shell_pid)).is_empty()
            && !read_pids(std::slice::from_ref(&child_pid)).is_empty()
    }));
    let shell = read_pids(std::slice::from_ref(&shell_pid))[0];
    let child = read_pids(std::slice::from_ref(&child_pid))[0];
    assert!(wait_until(Duration::from_secs(3), || !process_is_running(
        shell
    )));
    assert!(
        process_is_running(child),
        "direct-process recovery killed an unmanaged background child: {}",
        runner.log()
    );

    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(4)).success());
}

#[test]
fn atomic_binary_replacement_does_not_break_job_restarts() {
    let temp = tempfile::tempdir().unwrap();
    let executable = temp.path().join("jr");
    let replacement = temp.path().join("jr.new");
    let runs = temp.path().join("runs");
    let ready = temp.path().join("ready");
    let release = temp.path().join("release");
    let config = temp.path().join("jobs.yaml");
    let log_path = temp.path().join("runner.log");
    fs::copy(env!("CARGO_BIN_EXE_jr"), &executable).unwrap();
    fs::write(
        &config,
        format!(
            "jobs:\n  restart:\n    command: |\n      echo $$ >> {runs}\n      if [ \"$(wc -l < {runs})\" -eq 1 ]; then\n        touch {ready}\n        while [ ! -e {release} ]; do sleep 0.01; done\n      fi\n    restart_delay: 100\n",
            runs = runs.display(),
            ready = ready.display(),
            release = release.display(),
        ),
    )
    .unwrap();
    let log = File::create(&log_path).unwrap();
    let mut command = Command::new(&executable);
    command
        .arg(&config)
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    let mut runner = Runner::spawn_in_session(command, log_path);
    assert!(wait_until(Duration::from_secs(3), || ready.exists()));

    fs::copy(env!("CARGO_BIN_EXE_jr"), &replacement).unwrap();
    fs::rename(&replacement, &executable).unwrap();
    File::create(&release).unwrap();

    assert!(
        wait_until(Duration::from_secs(3), || {
            fs::read_to_string(&runs).is_ok_and(|contents| contents.lines().count() >= 2)
        }),
        "job did not restart after executable replacement: {}",
        runner.log()
    );
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(4)).success());
}

#[test]
fn invalid_datetime_format_is_rejected_before_jobs_start() {
    for (index, format) in ["%Q", "%#z"].into_iter().enumerate() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("started");
        let config = temp.path().join("jobs.yaml");
        let log = temp.path().join(format!("runner-{index}.log"));
        fs::write(
            &config,
            format!(
                "jobs:\n  invalid_datetime:\n    command: \"touch {}\"\n    datetime:\n      enabled: true\n      format: \"{}\"\n",
                marker.display(),
                format
            ),
        )
        .unwrap();

        let mut runner = Runner::start(&config, log, &[]);
        let status = runner.wait(Duration::from_secs(3));
        assert!(!status.success());
        assert!(
            !marker.exists(),
            "job started with invalid datetime format {format:?}"
        );
        assert!(
            runner.log().contains("invalid datetime format"),
            "invalid format {format:?} was not diagnosed: {}",
            runner.log()
        );
    }
}

#[test]
fn idle_supervision_has_a_bounded_syscall_and_cpu_cost() {
    let _resource_guard = serialize_resource_intensive_test();
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    let shim_paths: Vec<_> = (0..4)
        .map(|job| temp.path().join(format!("shim-{job}.pid")))
        .collect();
    let mut contents = String::from("jobs:\n");
    for (job, shim_path) in shim_paths.iter().enumerate() {
        contents.push_str(&format!(
            "  idle_{job}:\n    command: \"echo $PPID > {}; exec sleep 300\"\n    shutdown_timeout: 1\n",
            shim_path.display()
        ));
    }
    fs::write(&config, contents).unwrap();
    let _cleanup = ProcessCleanup(shim_paths.clone());

    // /proc/<pid>/status reports this counter for the main thread only.
    let mut runner = Runner::start_with_env(
        &config,
        log,
        &["-t", "1"],
        &[(TEST_RECONCILE_INTERVAL_ENV, "30000")],
    );
    assert!(wait_until(Duration::from_secs(4), || {
        read_pids(&shim_paths).len() == shim_paths.len()
    }));
    thread::sleep(Duration::from_millis(250));

    let runner_pid = runner.child.id();
    let mut supervised_pids = read_pids(&shim_paths);
    supervised_pids.push(runner_pid);
    let initial_reads = process_read_syscalls(runner_pid).unwrap();
    let initial_context_switches = process_voluntary_context_switches(runner_pid).unwrap();
    let initial_ticks: u64 = supervised_pids
        .iter()
        .map(|pid| process_cpu_ticks(*pid).unwrap())
        .sum();

    thread::sleep(Duration::from_secs(5));

    let read_delta = process_read_syscalls(runner_pid).unwrap() - initial_reads;
    let context_switch_delta =
        process_voluntary_context_switches(runner_pid).unwrap() - initial_context_switches;
    let final_ticks: u64 = supervised_pids
        .iter()
        .map(|pid| process_cpu_ticks(*pid).unwrap())
        .sum();
    let cpu_time = cpu_time_for_ticks(final_ticks - initial_ticks);
    assert!(
        read_delta <= 500,
        "idle runner issued {read_delta} read syscalls in five seconds"
    );
    assert!(
        context_switch_delta <= 50,
        "idle runner made {context_switch_delta} voluntary context switches in five seconds"
    );
    assert!(
        cpu_time <= Duration::from_millis(100),
        "possible runner or shim spin consumed {cpu_time:?} of CPU in five seconds"
    );

    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
}

#[test]
fn non_utf8_process_name_does_not_stop_scanner() {
    let temp = tempfile::tempdir().unwrap();
    let helper_pid = fork_with_retry();
    if helper_pid == 0 {
        let name = b"runner-\xff\xfe\0";
        unsafe {
            if libc::prctl(libc::PR_SET_NAME, name.as_ptr() as libc::c_ulong, 0, 0, 0) == -1 {
                libc::_exit(1);
            }
            loop {
                libc::pause();
            }
        }
    }
    let helper = ForkedProcess(helper_pid);
    assert!(
        wait_until(Duration::from_secs(3), || fs::read(format!(
            "/proc/{}/stat",
            helper.0
        ))
        .is_ok_and(|stat| stat
            .windows(9)
            .any(|window| window == b"runner-\xff\xfe"))),
        "helper process did not expose a non-UTF-8 comm"
    );

    let job_ready = temp.path().join("job-ready");
    let script = temp.path().join("long-job.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    write_executable(
        &script,
        &format!(
            "#!/bin/sh\ntouch {}\ntrap 'exit 0' TERM INT HUP QUIT\nwhile :; do sleep 1; done\n",
            job_ready.display()
        ),
    );
    fs::write(
        &config,
        format!("jobs:\n  long:\n    command: \"{}\"\n", script.display()),
    )
    .unwrap();

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(3), || job_ready.exists()));
    thread::sleep(Duration::from_millis(250));
    assert!(runner.is_running(), "non-UTF-8 comm stopped the supervisor");
    assert!(
        !runner.log().contains("Process scanner failed"),
        "{}",
        runner.log()
    );
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(4)).success());
}

#[test]
fn proc_churn_does_not_stop_scanner() {
    let _resource_guard = serialize_resource_intensive_test();
    let temp = tempfile::tempdir().unwrap();
    let ready = temp.path().join("ready");
    let script = temp.path().join("long-job.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    write_executable(
        &script,
        &format!(
            "#!/bin/sh\ntouch {}\ntrap 'exit 0' TERM INT HUP QUIT\nwhile :; do sleep 1; done\n",
            ready.display()
        ),
    );
    fs::write(
        &config,
        format!("jobs:\n  long:\n    command: \"{}\"\n", script.display()),
    )
    .unwrap();

    let mut runner =
        Runner::start_with_env(&config, log, &[], &[(TEST_RECONCILE_INTERVAL_ENV, "50")]);
    assert!(wait_until(Duration::from_secs(3), || ready.exists()));
    let fork_count = Arc::new(AtomicUsize::new(0));
    let churn = ChurnGuard::start(2, fork_count.clone());
    assert!(wait_until(Duration::from_secs(3), || {
        fork_count.load(Ordering::Relaxed) >= 20
    }));
    let reads_before = process_read_syscalls(runner.child.id()).unwrap();
    thread::sleep(Duration::from_millis(250));
    let reads_after = process_read_syscalls(runner.child.id()).unwrap();
    churn.stop();
    assert!(
        reads_after > reads_before,
        "process scanner did not capture while process churn was active"
    );
    assert!(runner.is_running(), "process churn stopped the supervisor");
    assert!(
        !runner.log().contains("Process scanner failed"),
        "{}",
        runner.log()
    );
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(4)).success());
}

#[test]
fn job_group_signal_cannot_directly_signal_the_supervisor() {
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
    let status = runner.wait(Duration::from_secs(3));
    assert!(
        !status.success() && status.code() == Some(1),
        "lost shim did not stop supervision safely: {}",
        runner.log()
    );
    assert_eq!(
        fs::read_to_string(&runs).unwrap().lines().count(),
        1,
        "job restarted without shim cleanup confirmation"
    );
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
fn replacement_checks_previous_identity_before_starting_and_reruns_one_shots_per_generation() {
    for wrap in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("jobs.json");
        let previous = temp.path().join("previous");
        let overlap = temp.path().join("overlap");
        let children = temp.path().join("children");
        let shim = temp.path().join("shim");
        let once = temp.path().join("once");
        let script = temp.path().join("generation.sh");
        write_executable(
            &script,
            &format!(
                r#"#!/bin/sh
read_identity() {{
    record=$(cat /proc/$1/stat 2>/dev/null) || return 1
    record=${{record##*) }}
    set -- $record
    printf '%s %s\n' "$1" "${{20}}"
}}
if [ -s {previous} ]; then
    read -r old_pid old_start < {previous}
    if current=$(read_identity "$old_pid"); then
        set -- $current
        if [ "$2" = "$old_start" ] && [ "$1" != Z ] && [ "$1" != X ]; then
            echo overlap >> {overlap}
        fi
    fi
fi
sleep 30 &
pid=$!
set -- $(read_identity "$pid")
echo "$pid $2" > {previous}
echo $PPID > {shim}
echo "$pid" >> {children}
{finish}
"#,
                previous = previous.display(),
                overlap = overlap.display(),
                shim = shim.display(),
                children = children.display(),
                finish = if wrap {
                    "while :; do sleep 1; done"
                } else {
                    "exit 0"
                }
            ),
        );
        fs::write(
            &config,
            serde_json::json!({"jobs": {
                "once": {"command": format!("echo run >> {}", once.display()), "restart": false},
                "repeated": {"command": format!("exec {}", script.display()), "restart_delay": 100}
            }})
            .to_string(),
        )
        .unwrap();
        let _cleanup = ProcessCleanup(vec![children.clone(), shim.clone()]);
        let args = if wrap { vec!["--wrap"] } else { vec![] };
        let mut runner = Runner::start(&config, temp.path().join("log"), &args);
        assert!(
            wait_until(Duration::from_secs(5), || !read_pids(std::slice::from_ref(
                &children
            ))
            .is_empty()
                && fs::read_to_string(&once)
                    .unwrap_or_default()
                    .lines()
                    .count()
                    == 1
                && !read_pids(std::slice::from_ref(&shim)).is_empty()),
            "{}",
            runner.log()
        );
        if wrap {
            let inner = parent_process_id(read_pids(&[shim])[0]).unwrap();
            assert_eq!(parent_process_id(inner), Some(runner.child.id()));
            assert_eq!(
                unsafe { libc::kill(inner as libc::pid_t, libc::SIGKILL) },
                0
            );
        }
        assert!(
            wait_until(Duration::from_secs(if wrap { 25 } else { 5 }), || {
                read_pids(std::slice::from_ref(&children)).len() >= 2
                    && fs::read_to_string(&once)
                        .unwrap_or_default()
                        .lines()
                        .count()
                        == if wrap { 2 } else { 1 }
            }),
            "{}",
            runner.log()
        );
        runner.signal(libc::SIGTERM);
        assert!(
            runner.wait(Duration::from_secs(5)).success(),
            "{}",
            runner.log()
        );
        assert!(
            !overlap.exists(),
            "a replacement observed its previous generation alive"
        );
        assert_eq!(
            fs::read_to_string(once).unwrap().lines().count(),
            if wrap { 2 } else { 1 }
        );
        assert!(
            read_pids(&[children])
                .into_iter()
                .all(|pid| !process_is_running(pid))
        );
    }
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
    assert!(
        wait_until(Duration::from_secs(5), || {
            read_pids(std::slice::from_ref(&descendants)).len() >= 2
        }),
        "escaped job did not restart: {}",
        runner.log()
    );
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
fn natural_exit_cleans_daemon_reparented_before_the_first_scan() {
    let temp = tempfile::tempdir().unwrap();
    let descendants = temp.path().join("descendants");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    fs::write(
        &config,
        format!(
            "jobs:\n  daemonizer:\n    command: \"before=$(wc -l < {descendants}); ( setsid sh -c 'echo $$ >> {descendants}; exec sleep 30' & ); while [ $(wc -l < {descendants}) -le $before ]; do :; done\"\n    restart_delay: 100\n",
            descendants = descendants.display()
        ),
    )
    .unwrap();
    File::create(&descendants).unwrap();
    let _cleanup = ProcessCleanup(vec![descendants.clone()]);

    let mut runner = Runner::start(&config, log, &[]);
    assert!(
        wait_until(Duration::from_secs(5), || {
            read_pids(std::slice::from_ref(&descendants)).len() >= 2
        }),
        "daemonizing job did not restart: {}",
        runner.log()
    );
    let descendant_pids = read_pids(std::slice::from_ref(&descendants));
    assert!(
        wait_until(Duration::from_secs(2), || !process_is_running(
            descendant_pids[0]
        )),
        "daemon from the first run survived its leader and restart: {}",
        runner.log()
    );

    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
    assert!(
        descendant_pids.iter().all(|pid| !process_is_running(*pid)),
        "daemonized descendants remained after shutdown"
    );
}

#[test]
fn reconciled_escapee_is_contained_after_abnormal_shim_exit() {
    let temp = tempfile::tempdir().unwrap();
    let shim_pid = temp.path().join("shim.pid");
    let workload_pid = temp.path().join("workload.pid");
    let descendant_pid = temp.path().join("descendant.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    fs::write(
        &config,
        format!(
            "jobs:\n  observed_escapee:\n    command: |\n      echo $$ > {workload_pid}\n      ( setsid sh -c 'echo $$ > {descendant_pid}; exec sleep 30' & )\n      while [ ! -s {descendant_pid} ]; do sleep 0.01; done\n      echo $PPID > {shim_pid}\n      while :; do sleep 1; done\n    restart_delay: 10000\n",
            workload_pid = workload_pid.display(),
            descendant_pid = descendant_pid.display(),
            shim_pid = shim_pid.display(),
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![workload_pid, descendant_pid.clone()]);

    let mut runner =
        Runner::start_with_env(&config, log, &[], &[(TEST_RECONCILE_INTERVAL_ENV, "100")]);
    assert!(wait_until(Duration::from_secs(3), || {
        !read_pids(std::slice::from_ref(&shim_pid)).is_empty()
            && !read_pids(std::slice::from_ref(&descendant_pid)).is_empty()
    }));
    thread::sleep(Duration::from_millis(500));
    let shim = read_pids(std::slice::from_ref(&shim_pid))[0];
    let descendant = read_pids(std::slice::from_ref(&descendant_pid))[0];
    assert_eq!(unsafe { libc::kill(shim as libc::pid_t, libc::SIGKILL) }, 0);
    assert!(
        wait_until(Duration::from_secs(4), || !process_is_running(descendant)),
        "previously reconciled escapee survived abnormal shim exit: {}",
        runner.log()
    );
    assert!(
        !runner.wait(Duration::from_secs(4)).success(),
        "abnormal shim exit did not fail supervision"
    );
}

#[test]
fn unreconciled_escapee_stops_restart_after_abnormal_shim_exit() {
    let temp = tempfile::tempdir().unwrap();
    let shim_pid = temp.path().join("shim.pid");
    let workload_pid = temp.path().join("workload.pid");
    let descendant_pid = temp.path().join("descendant.pid");
    let release = temp.path().join("release");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    fs::write(
        &config,
        format!(
            "jobs:\n  new_escapee:\n    command: |\n      echo $$ > {workload_pid}\n      echo $PPID > {shim_pid}\n      while [ ! -e {release} ]; do sleep 0.01; done\n      ( setsid sh -c 'echo $$ > {descendant_pid}; exec sleep 30' & )\n      while [ ! -s {descendant_pid} ]; do sleep 0.01; done\n      while :; do sleep 1; done\n    restart_delay: 10000\n",
            workload_pid = workload_pid.display(),
            descendant_pid = descendant_pid.display(),
            shim_pid = shim_pid.display(),
            release = release.display(),
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![workload_pid, descendant_pid.clone()]);

    let mut runner =
        Runner::start_with_env(&config, log, &[], &[(TEST_RECONCILE_INTERVAL_ENV, "30000")]);
    assert!(wait_until(Duration::from_secs(3), || {
        !read_pids(std::slice::from_ref(&shim_pid)).is_empty()
            && runner.log().contains("Started (pid")
    }));
    File::create(&release).unwrap();
    assert!(wait_until(Duration::from_secs(3), || {
        !read_pids(std::slice::from_ref(&descendant_pid)).is_empty()
    }));
    let shim = read_pids(std::slice::from_ref(&shim_pid))[0];
    let descendant = read_pids(std::slice::from_ref(&descendant_pid))[0];
    assert_eq!(unsafe { libc::kill(shim as libc::pid_t, libc::SIGKILL) }, 0);
    let status = runner.wait(Duration::from_secs(4));
    assert!(!status.success(), "abnormal shim exit returned success");
    assert!(
        process_is_running(descendant),
        "unreconciled escapee was unexpectedly attributed after abnormal shim exit: {}",
        runner.log()
    );
    assert!(
        runner
            .log()
            .contains("without confirming descendant cleanup"),
        "missing shim completion was not reported: {}",
        runner.log()
    );
}

#[test]
fn externally_terminated_leader_restarts_after_bounded_descendant_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let leaders = temp.path().join("leaders");
    let descendants = temp.path().join("descendants");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    File::create(&leaders).unwrap();
    File::create(&descendants).unwrap();
    fs::write(
        &config,
        format!(
            "jobs:\n  externally_killed:\n    command: \"before=$(wc -l < {descendants}); echo $$ >> {leaders}; setsid sh -c 'echo $$ >> {descendants}; trap \\\"\\\" TERM; while :; do sleep 1; done' & while [ $(wc -l < {descendants}) -le $before ]; do sleep 0.01; done; wait\"\n    restart_delay: 100\n    shutdown_timeout: 1\n",
            leaders = leaders.display(),
            descendants = descendants.display(),
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![leaders.clone(), descendants.clone()]);

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(4), || {
        read_pids(std::slice::from_ref(&leaders)).len() == 1
            && read_pids(std::slice::from_ref(&descendants)).len() == 1
    }));
    let first_leader = read_pids(std::slice::from_ref(&leaders))[0];
    let first_descendant = read_pids(std::slice::from_ref(&descendants))[0];
    assert_eq!(
        unsafe { libc::kill(first_leader as libc::pid_t, libc::SIGTERM) },
        0
    );

    assert!(
        wait_until(Duration::from_secs(5), || {
            read_pids(std::slice::from_ref(&leaders)).len() >= 2
        }),
        "job did not restart after its leader was terminated: {}",
        runner.log()
    );
    assert!(
        !process_is_running(first_descendant),
        "first run's descendant survived the restart: {}",
        runner.log()
    );

    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
}

#[test]
fn job_shim_reaps_adopted_children_while_the_main_command_runs() {
    let temp = tempfile::tempdir().unwrap();
    let shim_pid = temp.path().join("shim.pid");
    let ready = temp.path().join("ready");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    fs::write(
        &config,
        format!(
            "jobs:\n  reaper:\n    command: |\n      echo $PPID > {shim_pid}\n      for _ in $(seq 1 30); do ( setsid sh -c 'sleep 0.05' & ); done\n      touch {ready}\n      trap 'exit 0' TERM\n      while :; do sleep 1; done\n",
            shim_pid = shim_pid.display(),
            ready = ready.display(),
        ),
    )
    .unwrap();

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(5), || ready.exists()));
    let shim = read_pids(std::slice::from_ref(&shim_pid))[0];
    thread::sleep(Duration::from_millis(500));
    assert_eq!(
        zombie_child_count(shim),
        0,
        "job shim retained adopted zombies: {}",
        runner.log()
    );

    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
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

    let mut runner = Runner::start_with_unread_stderr(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(3), || pid_file.exists()));
    thread::sleep(Duration::from_millis(300));
    runner.signal(libc::SIGTERM);
    assert!(
        runner.wait(Duration::from_secs(4)).success(),
        "blocked stderr stalled shutdown"
    );
}

#[test]
fn blocked_wrapper_stderr_does_not_block_reconciliation() {
    let _resource_guard = serialize_resource_intensive_test();
    let temp = tempfile::tempdir().unwrap();
    let pid_file = temp.path().join("job.pids");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("wrapper.stdout");
    fs::write(
        &config,
        format!(
            "jobs:\n  blocked:\n    command: \"echo $$ > {0}; echo $PPID >> {0}; head -c 8000000 /dev/zero >&2; exec sleep 30\"\n    restart_delay: 10000\n    shutdown_timeout: 0\n",
            pid_file.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![pid_file.clone()]);

    let mut wrapper = Runner::start_with_unread_stderr(&config, log, &["--wrap"]);
    assert!(wait_until(Duration::from_secs(5), || {
        read_pids(std::slice::from_ref(&pid_file)).len() == 2
    }));
    let job_pids = read_pids(std::slice::from_ref(&pid_file));
    let shim = job_pids[1];
    let inner = parent_process_id(shim).expect("inner runner PID unavailable");
    assert_eq!(
        unsafe { libc::kill(inner as libc::pid_t, libc::SIGKILL) },
        0
    );
    wrapper.signal(libc::SIGTERM);

    assert!(
        wrapper.wait(Duration::from_secs(6)).success(),
        "blocked stderr stalled wrapper reconciliation"
    );
    assert!(
        wait_until(Duration::from_secs(2), || job_pids
            .iter()
            .all(|pid| !process_is_running(*pid))),
        "wrapper left descendants alive"
    );
}

#[test]
fn blocked_stderr_does_not_block_final_supervision_error() {
    let _resource_guard = serialize_resource_intensive_test();
    let temp = tempfile::tempdir().unwrap();
    let pid_file = temp.path().join("job.pids");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.stdout");
    fs::write(
        &config,
        format!(
            "jobs:\n  blocked:\n    command: \"echo $$ > {0}; echo $PPID >> {0}; head -c 8000000 /dev/zero >&2; exec sleep 30\"\n    restart_delay: 10000\n",
            pid_file.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![pid_file.clone()]);

    let mut runner = Runner::start_with_unread_stderr(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(5), || {
        read_pids(std::slice::from_ref(&pid_file)).len() == 2
    }));
    let job_pids = read_pids(std::slice::from_ref(&pid_file));
    let shim = job_pids[1];
    assert_eq!(unsafe { libc::kill(shim as libc::pid_t, libc::SIGKILL) }, 0);

    assert!(
        !runner.wait(Duration::from_secs(6)).success(),
        "blocked stderr stalled final supervisor failure reporting"
    );
}

#[test]
fn direct_process_mode_signals_only_the_shell_and_bounds_output_draining() {
    for natural_exit in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("jobs.json");
        let child = temp.path().join("child.pid");
        let ready = temp.path().join("ready");
        let received = temp.path().join("received");
        let command = if natural_exit {
            format!(
                "sleep 30 & echo $! > {}; echo tail; exit 0",
                child.display()
            )
        } else {
            format!(
                "sleep 30 & echo $! > {}; trap 'echo usr1 >> {}' USR1; trap 'exit 0' TERM; touch {}; while :; do sleep 0.05; done",
                child.display(),
                received.display(),
                ready.display()
            )
        };
        fs::write(
            &config,
            serde_json::json!({"jobs": {"direct": {
                "command": command, "process_group": false, "restart": false
            }}})
            .to_string(),
        )
        .unwrap();
        let _cleanup = ProcessCleanup(vec![child.clone()]);
        let mut runner = Runner::start(&config, temp.path().join("log"), &[]);
        assert!(
            wait_until(Duration::from_secs(5), || !read_pids(std::slice::from_ref(
                &child
            ))
            .is_empty()
                && (natural_exit || ready.exists())),
            "{}",
            runner.log()
        );
        let pid = read_pids(std::slice::from_ref(&child))[0];
        let identity = support::ProcessIdentity::read(pid).unwrap();
        if !natural_exit {
            runner.signal(libc::SIGUSR1);
            assert!(
                wait_until(Duration::from_secs(3), || received.exists()),
                "{}",
                runner.log()
            );
            assert_eq!(fs::read_to_string(&received).unwrap(), "usr1\n");
            assert!(identity.running(), "USR1 was incorrectly sent to the group");
            runner.signal(libc::SIGTERM);
        }
        assert!(
            runner.wait(Duration::from_secs(3)).success(),
            "{}",
            runner.log()
        );
        assert!(
            identity.running(),
            "direct mode unexpectedly killed a descendant"
        );
        if natural_exit {
            assert!(runner.log().contains("tail"));
        }
    }
}

#[test]
fn int_hup_and_quit_request_graceful_shutdown() {
    for signal in [libc::SIGINT, libc::SIGHUP, libc::SIGQUIT] {
        for wrap in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let config = temp.path().join("jobs.json");
            let ready = temp.path().join("ready");
            let graceful = temp.path().join("graceful");
            let pid_path = temp.path().join("job.pid");
            fs::write(&config, serde_json::json!({"jobs": {"job": {
                "command": format!("echo $$ > {}; trap 'echo term >> {}; exit 0' TERM; touch {}; while :; do sleep 0.1; done", pid_path.display(), graceful.display(), ready.display()),
                "restart": false
            }}}).to_string()).unwrap();
            let _cleanup = ProcessCleanup(vec![pid_path.clone()]);
            let args = if wrap { vec!["--wrap"] } else { vec![] };
            let mut runner = Runner::start(&config, temp.path().join("log"), &args);
            assert!(
                wait_until(Duration::from_secs(5), || ready.exists()
                    && !read_pids(std::slice::from_ref(&pid_path)).is_empty()),
                "{}",
                runner.log()
            );
            let identity = support::ProcessIdentity::read(read_pids(&[pid_path])[0]).unwrap();
            runner.signal(signal);
            assert_eq!(
                runner.wait(Duration::from_secs(5)).code(),
                Some(0),
                "{}",
                runner.log()
            );
            assert_eq!(fs::read_to_string(graceful).unwrap(), "term\n");
            assert!(!identity.running());
            let name = match signal {
                libc::SIGINT => "INT",
                libc::SIGHUP => "HUP",
                _ => "QUIT",
            };
            assert!(
                runner.log().contains(&format!("Received {name} signal")),
                "{}",
                runner.log()
            );
        }
    }
}

#[test]
fn second_sigint_forces_shutdown_without_waiting_for_grace_period() {
    for wrap in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("jobs.json");
        let pid_path = temp.path().join("job.pid");
        fs::write(&config, serde_json::json!({"jobs": {"job": {
            "command": format!("trap '' TERM; echo $$ > {}; while :; do sleep 1; done", pid_path.display()),
            "shutdown_timeout": 30, "restart": false
        }}}).to_string()).unwrap();
        let _cleanup = ProcessCleanup(vec![pid_path.clone()]);
        let args = if wrap { vec!["--wrap"] } else { vec![] };
        let mut runner = Runner::start(&config, temp.path().join("log"), &args);
        assert!(
            wait_until(Duration::from_secs(5), || !read_pids(std::slice::from_ref(
                &pid_path
            ))
            .is_empty()),
            "{}",
            runner.log()
        );
        let identity = support::ProcessIdentity::read(read_pids(&[pid_path])[0]).unwrap();
        runner.signal(libc::SIGINT);
        assert!(
            wait_until(Duration::from_secs(5), || runner
                .log()
                .contains("shutting down active processes")),
            "{}",
            runner.log()
        );
        runner.signal(libc::SIGINT);
        assert_eq!(
            runner.wait(Duration::from_secs(4)).code(),
            Some(0),
            "{}",
            runner.log()
        );
        assert!(!identity.running());
    }
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
fn shim_startup_output_is_drained_before_readiness() {
    let temp = tempfile::tempdir().unwrap();
    let job_pid = temp.path().join("job.pid");
    let shim_pid = temp.path().join("shim.pid");
    let config = temp.path().join("jobs.json");
    let log = temp.path().join("runner.log");
    let probe = build_shim_startup_probe_library(temp.path());
    let command = format!("echo $$ > {}; exec sleep 30", job_pid.display());
    let config_data = serde_json::json!({
        "jobs": {
            "job": {
                "command": command,
                "shutdown_timeout": 1,
                "env": {
                    "LD_DEBUG": "all",
                    "LD_PRELOAD": probe,
                    "JOB_RUNNER_SHIM_STARTUP_PID": shim_pid,
                }
            }
        }
    });
    fs::write(&config, serde_json::to_vec(&config_data).unwrap()).unwrap();
    let _cleanup = ProcessCleanup(vec![job_pid.clone(), shim_pid.clone()]);

    let mut runner = Runner::start(&config, log, &[]);
    assert!(
        wait_until(Duration::from_secs(10), || {
            !read_pids(std::slice::from_ref(&job_pid)).is_empty()
        }),
        "loader diagnostics blocked shim readiness: {}",
        runner.log()
    );
    assert!(!read_pids(std::slice::from_ref(&shim_pid)).is_empty());
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
}

#[test]
fn shutdown_interrupts_a_stopped_unready_shim() {
    let temp = tempfile::tempdir().unwrap();
    let shim_pid = temp.path().join("shim.pid");
    let config = temp.path().join("jobs.json");
    let log = temp.path().join("runner.log");
    let probe = build_shim_startup_probe_library(temp.path());
    let config_data = serde_json::json!({
        "jobs": {
            "job": {
                "command": "exec sleep 30",
                "shutdown_timeout": 1,
                "env": {
                    "LD_PRELOAD": probe,
                    "JOB_RUNNER_SHIM_STARTUP_PID": shim_pid,
                    "JOB_RUNNER_STOP_SHIM_STARTUP": "1",
                }
            }
        }
    });
    fs::write(&config, serde_json::to_vec(&config_data).unwrap()).unwrap();
    let _cleanup = ProcessCleanup(vec![shim_pid.clone()]);

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&shim_pid)).is_empty()
    }));
    let shim = read_pids(std::slice::from_ref(&shim_pid))[0];
    assert_eq!(process_state(shim), Some('T'));
    runner.signal(libc::SIGTERM);
    assert!(
        runner.wait(Duration::from_secs(5)).success(),
        "stopped unready shim disabled shutdown: {}",
        runner.log()
    );
    assert!(!process_is_running(shim));
}

#[test]
fn unready_shim_cleanup_kills_an_already_started_escapee() {
    let temp = tempfile::tempdir().unwrap();
    let shim_pid = temp.path().join("shim.pid");
    let escapee_pid = temp.path().join("escapee.pid");
    let config = temp.path().join("jobs.json");
    let log = temp.path().join("runner.log");
    let probe = build_shim_startup_probe_library(temp.path());
    let command = format!(
        "setsid sh -c 'echo $$ > {}; exec sleep 30' & wait",
        escapee_pid.display()
    );
    let config_data = serde_json::json!({
        "jobs": {
            "job": {
                "command": command,
                "shutdown_timeout": 1,
                "env": {
                    "LD_PRELOAD": probe,
                    "JOB_RUNNER_SHIM_STARTUP_PID": shim_pid,
                    "JOB_RUNNER_STOP_READY_WRITE_FOR": escapee_pid,
                }
            }
        }
    });
    fs::write(&config, serde_json::to_vec(&config_data).unwrap()).unwrap();
    let _cleanup = ProcessCleanup(vec![shim_pid.clone(), escapee_pid.clone()]);

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&shim_pid)).is_empty()
            && !read_pids(std::slice::from_ref(&escapee_pid)).is_empty()
    }));
    let shim = read_pids(std::slice::from_ref(&shim_pid))[0];
    let escapee = read_pids(std::slice::from_ref(&escapee_pid))[0];
    assert!(wait_until(Duration::from_secs(1), || {
        process_state(shim) == Some('T')
    }));
    runner.signal(libc::SIGTERM);

    assert!(runner.wait(Duration::from_secs(5)).success());
    assert!(
        wait_until(Duration::from_secs(2), || !process_is_running(escapee)),
        "unready shim cleanup left an escaped workload alive"
    );
}

#[test]
fn inherited_completion_writer_cannot_block_unready_shim_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let shim_pid = temp.path().join("shim.pid");
    let holder_pid = temp.path().join("holder.pid");
    let config = temp.path().join("jobs.json");
    let log = temp.path().join("runner.log");
    let probe = build_shim_startup_probe_library(temp.path());
    let config_data = serde_json::json!({
        "jobs": {
            "job": {
                "command": "exec sleep 30",
                "shutdown_timeout": 1,
                "env": {
                    "LD_PRELOAD": probe,
                    "JOB_RUNNER_SHIM_STARTUP_PID": shim_pid,
                    "JOB_RUNNER_COMPLETION_HOLDER_PID": holder_pid,
                    "JOB_RUNNER_STOP_SHIM_STARTUP": "1",
                }
            }
        }
    });
    fs::write(&config, serde_json::to_vec(&config_data).unwrap()).unwrap();
    let _cleanup = ProcessCleanup(vec![shim_pid.clone(), holder_pid.clone()]);

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&shim_pid)).is_empty()
            && !read_pids(std::slice::from_ref(&holder_pid)).is_empty()
    }));
    let holder = read_pids(std::slice::from_ref(&holder_pid))[0];
    assert!(process_is_running(holder));
    runner.signal(libc::SIGTERM);

    let started = Instant::now();
    let status = runner.wait(Duration::from_secs(5));
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(
        !status.success(),
        "inherited completion writer was accepted as confirmed cleanup"
    );
    assert!(
        process_is_running(holder),
        "test did not retain the inherited completion writer"
    );
}

#[test]
fn unready_sudo_cleanup_propagates_signal_helper_containment_failure() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let shim_pid = temp.path().join("shim.pid");
    let workload_pid = temp.path().join("workload.pid");
    let helper_escapee_pid = temp.path().join("helper-escapee.pid");
    let config = temp.path().join("jobs.json");
    let log = temp.path().join("runner.log");
    for path in [&workload_pid, &helper_escapee_pid] {
        File::create(path).unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o666);
        fs::set_permissions(path, permissions).unwrap();
    }
    let mut permissions = fs::metadata(temp.path()).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(temp.path(), permissions).unwrap();
    let probe = build_shim_startup_probe_library(temp.path());
    let path = install_unready_cleanup_leaking_sudo(temp.path(), &helper_escapee_pid);
    let config_data = serde_json::json!({
        "jobs": {
            "job": {
                "command": format!(
                    "exec setsid sh -c 'echo $$ > {}; exec sleep 30'",
                    workload_pid.display()
                ),
                "sudo_user": username,
                "shutdown_timeout": 1,
            }
        }
    });
    fs::write(&config, serde_json::to_vec(&config_data).unwrap()).unwrap();
    let _workload_cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: workload_pid.clone(),
    };
    let _helper_cleanup = ProcessCleanup(vec![helper_escapee_pid.clone()]);

    let environment = [
        ("LD_PRELOAD", probe.to_str().unwrap()),
        ("JOB_RUNNER_SHIM_STARTUP_PID", shim_pid.to_str().unwrap()),
        (
            "JOB_RUNNER_STOP_READY_WRITE_FOR",
            workload_pid.to_str().unwrap(),
        ),
    ];
    let mut runner = Runner::start_with_path_and_env(&config, log, &path, &environment);
    assert!(
        wait_until(Duration::from_secs(5), || {
            !read_pids(std::slice::from_ref(&shim_pid)).is_empty()
                && !read_pids(std::slice::from_ref(&workload_pid)).is_empty()
        }),
        "unready sudo workload did not start: {}",
        runner.log()
    );
    runner.signal(libc::SIGTERM);
    let status = runner.wait(Duration::from_secs(8));

    assert!(
        !status.success(),
        "unready cleanup ignored signal-helper containment failure: {}",
        runner.log()
    );
    assert!(
        read_pids(std::slice::from_ref(&helper_escapee_pid))
            .into_iter()
            .any(process_is_running),
        "test did not exercise a surviving signal-helper descendant"
    );
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
            "#!/bin/bash\necho $$ >> {}\npid=$PPID\nwhile [ \"$pid\" -gt 1 ]; do\n  cmdline=$(tr '\\0' ' ' < /proc/$pid/cmdline 2>/dev/null)\n  if [ \"$(cat /proc/$pid/comm 2>/dev/null)\" = jr ] && [[ \"$cmdline\" != *--job-shim* ]]; then\n    kill -KILL \"$pid\"\n    exit 0\n  fi\n  pid=$(awk '{{print $4}}' /proc/$pid/stat 2>/dev/null || echo 1)\ndone\nexit 1\n",
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
fn wrapper_identity_failure_reconciles_started_jobs() {
    let temp = tempfile::tempdir().unwrap();
    let job_pid = temp.path().join("job.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("wrapper.log");
    let fault_library = build_identity_fault_library(temp.path());
    fs::write(
        &config,
        format!(
            "jobs:\n  identity_failure:\n    command: \"echo $$ > {}; exec sleep 30\"\n    shutdown_timeout: 0\n",
            job_pid.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![job_pid.clone()]);
    let environment = [
        ("LD_PRELOAD", fault_library.to_str().unwrap()),
        ("JOB_RUNNER_IDENTITY_FAULT_READY", job_pid.to_str().unwrap()),
    ];

    let mut wrapper = Runner::start_with_env(&config, log, &["--wrap"], &environment);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
    }));
    let job = read_pids(std::slice::from_ref(&job_pid))[0];
    assert!(
        wait_until(Duration::from_secs(5), || !process_is_running(job)),
        "wrapper identity failure left a started job alive: {}",
        wrapper.log()
    );
    assert!(
        wrapper.is_running(),
        "wrapper stopped after successful reconciliation: {}",
        wrapper.log()
    );

    wrapper.signal(libc::SIGTERM);
    assert!(wrapper.wait(Duration::from_secs(5)).success());
}

#[test]
fn wrapper_reconciles_jobs_left_by_a_killed_inner_runner_before_restart() {
    let temp = tempfile::tempdir().unwrap();
    let runs = temp.path().join("runs");
    let killed_once = temp.path().join("killed-once");
    let script = temp.path().join("kill-runner-once.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("wrapper.log");
    write_executable(
        &script,
        &format!(
            "#!/bin/bash\necho $$ >> {runs}\nif [ ! -e {killed_once} ]; then\n  touch {killed_once}\n  pid=$PPID\n  shim=\n  inner=\n  while [ \"$pid\" -gt 1 ]; do\n    cmdline=$(tr '\\0' ' ' < /proc/$pid/cmdline 2>/dev/null)\n    if [ \"$(cat /proc/$pid/comm 2>/dev/null)\" = jr ]; then\n      if [[ \"$cmdline\" = *--job-shim* ]]; then shim=$pid; else inner=$pid; break; fi\n    fi\n    pid=$(awk '{{print $4}}' /proc/$pid/stat 2>/dev/null || echo 1)\n  done\n  kill -KILL \"$inner\" \"$shim\"\nfi\nexec sleep 30\n",
            runs = runs.display(),
            killed_once = killed_once.display(),
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  survivor:\n    command: \"{}\"\n    restart_delay: 10000\n",
            script.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![runs.clone()]);

    let mut wrapper = Runner::start(&config, log, &["--wrap"]);
    assert!(
        wait_until(Duration::from_secs(25), || {
            read_pids(std::slice::from_ref(&runs)).len() >= 2
        }),
        "inner runner did not restart: {}",
        wrapper.log()
    );
    let job_pids = read_pids(std::slice::from_ref(&runs));
    assert!(
        !process_is_running(job_pids[0]),
        "previous-generation job overlapped its replacement: {}",
        wrapper.log()
    );
    assert!(process_is_running(job_pids[1]));

    wrapper.signal(libc::SIGTERM);
    assert!(wrapper.wait(Duration::from_secs(5)).success());
    assert!(wait_until(Duration::from_secs(2), || job_pids
        .iter()
        .all(|pid| !process_is_running(*pid))));
}

#[test]
fn wrapper_contains_new_escapee_after_inner_and_shim_exit() {
    let temp = tempfile::tempdir().unwrap();
    let descendant_pid = temp.path().join("descendant.pid");
    let script = temp.path().join("escape-and-kill-supervisors.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("wrapper.log");
    write_executable(
        &script,
        &format!(
            "#!/bin/bash\n( setsid sh -c 'echo $$ > {descendant_pid}; exec sleep 30' & )\nwhile [ ! -s {descendant_pid} ]; do sleep 0.01; done\npid=$PPID\nshim=\ninner=\nwhile [ \"$pid\" -gt 1 ]; do\n  cmdline=$(tr '\\0' ' ' < /proc/$pid/cmdline 2>/dev/null)\n  if [ \"$(cat /proc/$pid/comm 2>/dev/null)\" = jr ]; then\n    if [[ \"$cmdline\" = *--job-shim* ]]; then shim=$pid; else inner=$pid; break; fi\n  fi\n  pid=$(awk '{{print $4}}' /proc/$pid/stat 2>/dev/null || echo 1)\ndone\nkill -KILL \"$shim\" \"$inner\"\n",
            descendant_pid = descendant_pid.display(),
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  new_escapee:\n    command: \"{}\"\n    restart_delay: 10000\n",
            script.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![descendant_pid.clone()]);

    let mut wrapper = Runner::start_with_env(
        &config,
        log,
        &["--wrap"],
        &[(TEST_RECONCILE_INTERVAL_ENV, "30000")],
    );
    assert!(wait_until(Duration::from_secs(3), || {
        !read_pids(std::slice::from_ref(&descendant_pid)).is_empty()
    }));
    let descendant = read_pids(std::slice::from_ref(&descendant_pid))[0];
    assert!(
        wait_until(Duration::from_secs(5), || !process_is_running(descendant)),
        "wrapper did not reconcile a newly escaped process: {}",
        wrapper.log()
    );
    assert!(wrapper.is_running(), "wrapper exited during reconciliation");

    wrapper.signal(libc::SIGTERM);
    assert!(wrapper.wait(Duration::from_secs(4)).success());
}

#[test]
fn wrapper_reconciles_cross_uid_jobs_before_restart() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }

    let temp = tempfile::tempdir().unwrap();
    let runs = temp.path().join("runs");
    let killed_once = temp.path().join("killed-once");
    let script = temp.path().join("kill-runner-once.sh");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("wrapper.log");
    File::create(&runs).unwrap();
    let mut permissions = fs::metadata(&runs).unwrap().permissions();
    permissions.set_mode(0o666);
    fs::set_permissions(&runs, permissions).unwrap();
    let mut permissions = fs::metadata(temp.path()).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(temp.path(), permissions).unwrap();
    write_executable(
        &script,
        &format!(
            "#!/bin/bash\necho $$ >> {runs}\nif [ ! -e {killed_once} ]; then\n  touch {killed_once}\n  pid=$PPID\n  shim=\n  inner=\n  while [ \"$pid\" -gt 1 ]; do\n    cmdline=$(tr '\\0' ' ' < /proc/$pid/cmdline 2>/dev/null)\n    if [ \"$(cat /proc/$pid/comm 2>/dev/null)\" = jr ]; then\n      if [[ \"$cmdline\" = *--job-shim* ]]; then shim=$pid; else inner=$pid; break; fi\n    fi\n    pid=$(awk '{{print $4}}' /proc/$pid/stat 2>/dev/null || echo 1)\n  done\n  kill -KILL \"$inner\" \"$shim\"\nfi\nexec sleep 30\n",
            runs = runs.display(),
            killed_once = killed_once.display(),
        ),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  root_survivor:\n    command: \"{}\"\n    sudo_user: {username}\n    restart_delay: 10000\n",
            script.display()
        ),
    )
    .unwrap();
    let _cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: runs.clone(),
    };

    let mut wrapper = Runner::start(&config, log, &["--wrap"]);
    assert!(
        wait_until(Duration::from_secs(25), || {
            read_pids(std::slice::from_ref(&runs)).len() >= 2
        }),
        "inner runner did not restart after cross-UID cleanup: {}",
        wrapper.log()
    );
    let job_pids = read_pids(std::slice::from_ref(&runs));
    assert!(
        !process_is_running(job_pids[0]),
        "previous cross-UID generation overlapped its replacement: {}",
        wrapper.log()
    );
    assert!(process_is_running(job_pids[1]));

    wrapper.signal(libc::SIGTERM);
    assert!(wrapper.wait(Duration::from_secs(5)).success());
    assert!(wait_until(Duration::from_secs(2), || job_pids
        .iter()
        .all(|pid| !process_is_running(*pid))));
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
            "#!/bin/bash\necho run >> {runs}\nif [ \"$(wc -l < {runs})\" -eq 1 ]; then\n  pid=$PPID\n  while [ \"$pid\" -gt 1 ]; do\n    cmdline=$(tr '\\0' ' ' < /proc/$pid/cmdline 2>/dev/null)\n    if [ \"$(cat /proc/$pid/comm 2>/dev/null)\" = jr ] && [[ \"$cmdline\" != *--job-shim* ]]; then\n      kill -KILL \"$pid\"\n      exit 0\n    fi\n    pid=$(awk '{{print $4}}' /proc/$pid/stat 2>/dev/null || echo 1)\n  done\n  exit 1\nfi\necho $$ > {job_pid}\ntrap 'touch {stale_signal}' USR1\ntouch {stable}\nwhile :; do sleep 1; done\n",
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
    let username = if unsafe { libc::geteuid() } == 0 {
        "nobody"
    } else {
        "root"
    };
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    for _ in 0..3 {
        exercise_sudo_signal_route(username);
    }
}

#[test]
fn stalled_sudo_term_helper_respects_zero_shutdown_timeout() {
    let Some(username) = current_username() else {
        eprintln!("skipped: current username unavailable");
        return;
    };
    if !sudo_available_for(&username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let path = install_hanging_term_signal_sudo(temp.path(), None);
    let pid_file = temp.path().join("job.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    fs::write(
        &config,
        format!(
            "jobs:\n  deadline:\n    command: \"echo $$ > {}; exec sleep 30\"\n    sudo_user: {}\n    shutdown_timeout: 0\n",
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
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&pid_file)).is_empty()
    }));
    let started = Instant::now();
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(4)).success());
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "zero shutdown timeout waited for stalled TERM helper"
    );
}

#[test]
fn second_termination_signal_interrupts_stalled_sudo_delivery() {
    let Some(username) = current_username() else {
        eprintln!("skipped: current username unavailable");
        return;
    };
    if !sudo_available_for(&username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let path = install_hanging_term_signal_sudo(temp.path(), None);
    let pid_file = temp.path().join("job.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    fs::write(
        &config,
        format!(
            "jobs:\n  escalation:\n    command: \"echo $$ > {}; exec sleep 30\"\n    sudo_user: {}\n    shutdown_timeout: 30\n",
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
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&pid_file)).is_empty()
    }));
    let started = Instant::now();
    runner.signal(libc::SIGTERM);
    thread::sleep(Duration::from_millis(100));
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(4)).success());
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "second termination signal did not interrupt stalled delivery"
    );
}

#[test]
fn shutdown_interrupts_stalled_user_signal_delivery_across_jobs() {
    let Some(username) = current_username() else {
        eprintln!("skipped: current username unavailable");
        return;
    };
    if !sudo_available_for(&username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let path = install_hanging_usr1_signal_sudo(temp.path());
    let pid_file = temp.path().join("jobs.pids");
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
            "jobs:\n  first:\n    command: \"echo $$ >> {0}; exec sleep 30\"\n    sudo_user: {1}\n    shutdown_timeout: 0\n  second:\n    command: \"echo $$ >> {0}; exec sleep 30\"\n    sudo_user: {1}\n    shutdown_timeout: 0\n",
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
    assert!(wait_until(Duration::from_secs(5), || {
        read_pids(std::slice::from_ref(&pid_file)).len() == 2
    }));
    let started = Instant::now();
    runner.signal(libc::SIGUSR1);
    thread::sleep(Duration::from_millis(100));
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(4)).success());
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "stalled user-signal delivery delayed shutdown"
    );
}

#[test]
fn stalled_user_signal_forwarding_is_coalesced() {
    let Some(username) = current_username() else {
        eprintln!("skipped: current username unavailable");
        return;
    };
    if !sudo_available_for(&username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let invocations = temp.path().join("helpers.pids");
    let job_pid = temp.path().join("job.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    let path = install_counting_hanging_usr1_signal_sudo(temp.path(), &invocations);
    fs::write(
        &config,
        format!(
            "jobs:\n  bounded_forwarding:\n    command: \"echo $$ > {}; exec sleep 30\"\n    sudo_user: {}\n    shutdown_timeout: 0\n",
            job_pid.display(),
            username
        ),
    )
    .unwrap();
    let _cleanup = SudoProcessCleanup {
        username,
        pid_path: job_pid.clone(),
    };
    let _helper_cleanup = ProcessCleanup(vec![invocations.clone()]);

    let mut runner = Runner::start_with_path(&config, log, &path);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
    }));
    for _ in 0..12 {
        runner.signal(libc::SIGUSR1);
        thread::sleep(Duration::from_millis(35));
    }
    assert!(wait_until(Duration::from_secs(2), || {
        !read_pids(std::slice::from_ref(&invocations)).is_empty()
    }));
    thread::sleep(Duration::from_millis(250));
    assert_eq!(
        read_pids(std::slice::from_ref(&invocations)).len(),
        1,
        "stalled forwarding spawned concurrent helpers: {}",
        runner.log()
    );

    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
    assert!(
        read_pids(std::slice::from_ref(&invocations))
            .iter()
            .all(|pid| !process_is_running(*pid))
    );
}

#[test]
fn coalesced_user_signal_forwarding_preserves_signal_order() {
    let Some(username) = current_username() else {
        eprintln!("skipped: current username unavailable");
        return;
    };
    if !sudo_available_for(&username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let invocations = temp.path().join("signals");
    let job_pid = temp.path().join("job.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    let path = install_slow_signal_sudo(temp.path(), &invocations);
    fs::write(
        &config,
        format!(
            "jobs:\n  ordered_forwarding:\n    command: \"echo $$ > {}; exec sleep 30\"\n    sudo_user: {}\n    shutdown_timeout: 0\n",
            job_pid.display(),
            username
        ),
    )
    .unwrap();
    let _cleanup = SudoProcessCleanup {
        username,
        pid_path: job_pid.clone(),
    };

    let mut runner = Runner::start_with_path(&config, log, &path);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
    }));
    runner.signal(libc::SIGUSR1);
    thread::sleep(Duration::from_millis(20));
    runner.signal(libc::SIGUSR2);
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline
        && !fs::read_to_string(&invocations)
            .is_ok_and(|contents| contents.lines().any(|signal| signal == "-USR2"))
    {
        runner.signal(libc::SIGUSR1);
        thread::sleep(Duration::from_millis(20));
    }
    assert!(
        fs::read_to_string(&invocations)
            .is_ok_and(|contents| contents.lines().any(|signal| signal == "-USR2")),
        "pending USR2 was starved by USR1 traffic: {}",
        runner.log()
    );

    runner.signal(libc::SIGTERM);
    let status = runner.wait(Duration::from_secs(5));
    assert!(
        status.success(),
        "runner exited with {status}: {}",
        runner.log()
    );
}

#[test]
fn shutdown_cleans_forked_signal_helper_descendants() {
    let Some(username) = current_username() else {
        eprintln!("skipped: current username unavailable");
        return;
    };
    if !sudo_available_for(&username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let helper_pid = temp.path().join("helper.pid");
    let job_pid = temp.path().join("job.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    fs::write(
        &config,
        format!(
            "jobs:\n  helper_cleanup:\n    command: \"echo $$ > {}; exec sleep 30\"\n    sudo_user: {}\n    shutdown_timeout: 0\n",
            job_pid.display(),
            username
        ),
    )
    .unwrap();
    let _cleanup = SudoProcessCleanup {
        username: username.clone(),
        pid_path: job_pid.clone(),
    };
    let _helper_cleanup = ProcessCleanup(vec![helper_pid.clone()]);
    let path = install_forking_usr1_signal_sudo(temp.path(), &helper_pid);

    let mut runner = Runner::start_with_path(&config, log, &path);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
    }));
    runner.signal(libc::SIGUSR1);
    assert!(wait_until(Duration::from_secs(3), || {
        !read_pids(std::slice::from_ref(&helper_pid)).is_empty()
    }));
    let helper = read_pids(std::slice::from_ref(&helper_pid))[0];
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
    assert!(
        wait_until(Duration::from_secs(2), || !process_is_running(helper)),
        "signal helper descendant survived shutdown: {}",
        runner.log()
    );
}

#[test]
fn completed_failing_signal_helper_cleans_its_descendants() {
    let Some(username) = current_username() else {
        eprintln!("skipped: current username unavailable");
        return;
    };
    if !sudo_available_for(&username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let helper_pid = temp.path().join("helper.pid");
    let job_pid = temp.path().join("job.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    fs::write(
        &config,
        format!(
            "jobs:\n  helper_cleanup:\n    command: \"echo $$ > {}; exec sleep 30\"\n    sudo_user: {}\n    shutdown_timeout: 0\n",
            job_pid.display(),
            username
        ),
    )
    .unwrap();
    let _cleanup = SudoProcessCleanup {
        username: username.clone(),
        pid_path: job_pid.clone(),
    };
    let _helper_cleanup = ProcessCleanup(vec![helper_pid.clone()]);
    let path = install_exiting_forking_usr1_signal_sudo(temp.path(), &helper_pid);

    let mut runner = Runner::start_with_path(&config, log, &path);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
    }));
    runner.signal(libc::SIGUSR1);
    assert!(wait_until(Duration::from_secs(3), || {
        !read_pids(std::slice::from_ref(&helper_pid)).is_empty()
    }));
    let helper = read_pids(std::slice::from_ref(&helper_pid))[0];
    assert!(
        wait_until(Duration::from_secs(3), || !process_is_running(helper)),
        "completed signal helper left a child alive: {}",
        runner.log()
    );
    assert!(
        runner.is_running(),
        "signal-helper failure stopped supervision"
    );

    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
}

#[test]
fn surviving_signal_helper_root_stops_supervision() {
    let Some(username) = current_username() else {
        eprintln!("skipped: current username unavailable");
        return;
    };
    if !sudo_available_for(&username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let helper_pid = temp.path().join("helper.pid");
    let job_pid = temp.path().join("job.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    let fault_library =
        build_helper_root_fault_library(temp.path(), &helper_pid, "job-runner-signal");
    fs::write(
        &config,
        format!(
            "jobs:\n  helper_cleanup:\n    command: \"echo $$ > {}; exec sleep 30\"\n    sudo_user: {}\n    shutdown_timeout: 0\n",
            job_pid.display(),
            username
        ),
    )
    .unwrap();
    let _job_cleanup = SudoProcessCleanup {
        username,
        pid_path: job_pid.clone(),
    };
    let _helper_cleanup = ProcessCleanup(vec![helper_pid.clone()]);
    let environment = [("LD_PRELOAD", fault_library.to_str().unwrap())];

    let mut runner = Runner::start_with_env(&config, log, &[], &environment);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
    }));
    runner.signal(libc::SIGUSR1);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&helper_pid)).is_empty()
    }));
    let helper = read_pids(std::slice::from_ref(&helper_pid))[0];
    let status = runner.wait(Duration::from_secs(8));

    assert!(
        !status.success(),
        "surviving signal-helper root did not stop supervision: {}",
        runner.log()
    );
    assert!(
        process_is_running(helper),
        "test did not retain the signal-helper root"
    );
}

#[test]
fn unresolved_signal_helper_root_stops_supervision() {
    exercise_unresolved_signal_helper_root(false);
}

#[test]
fn unresolved_own_root_prevents_shim_cleanup_confirmation() {
    exercise_unresolved_signal_helper_root(true);
}

fn exercise_unresolved_signal_helper_root(fault_shim_own_root: bool) {
    let Some(username) = current_username() else {
        eprintln!("skipped: current username unavailable");
        return;
    };
    if !sudo_available_for(&username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let job_pid = temp.path().join("job.pid");
    let escapee_pid = temp.path().join("signal-helper-escapee.pid");
    let fault_marker = temp.path().join("partial-capture-faults");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    let fault_library = build_partial_helper_capture_fault_library(temp.path());
    fs::write(
        &config,
        format!(
            "jobs:\n  helper_cleanup:\n    command: \"echo $$ > {}; exec sleep 30\"\n    sudo_user: {}\n    shutdown_timeout: 0\n",
            job_pid.display(),
            username
        ),
    )
    .unwrap();
    let _job_cleanup = SudoProcessCleanup {
        username,
        pid_path: job_pid.clone(),
    };
    let _escapee_cleanup = ProcessCleanup(vec![escapee_pid.clone()]);
    let path = install_partial_capture_signal_sudo(temp.path(), &escapee_pid, !fault_shim_own_root);
    let mut environment = vec![
        ("LD_PRELOAD", fault_library.to_str().unwrap()),
        ("JOB_RUNNER_PARTIAL_CAPTURE_HELPER", "job-runner-signal"),
    ];
    if fault_shim_own_root {
        environment.push(("JOB_RUNNER_PARTIAL_CAPTURE_OWN_ROOT", "1"));
        environment.push((
            "JOB_RUNNER_PARTIAL_CAPTURE_FAULT_MARKER",
            fault_marker.to_str().unwrap(),
        ));
    }

    let mut runner = Runner::start_with_path_and_env(&config, log, &path, &environment);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
    }));
    runner.signal(libc::SIGUSR1);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&escapee_pid)).is_empty()
    }));
    let escapee = read_pids(std::slice::from_ref(&escapee_pid))[0];
    if fault_shim_own_root {
        assert!(
            wait_until(Duration::from_secs(5), || !process_is_running(escapee)),
            "helper shim falsely confirmed cleanup after its root was unresolved: {}",
            runner.log()
        );
        assert!(
            fs::metadata(&fault_marker).is_ok_and(|metadata| metadata.len() > 0),
            "own-root fault was not exercised"
        );
        assert!(runner.is_running());
        runner.signal(libc::SIGTERM);
        assert!(runner.wait(Duration::from_secs(5)).success());
    } else {
        let status = runner.wait(Duration::from_secs(8));
        assert!(
            !status.success(),
            "unresolved signal-helper root did not stop supervision: {}",
            runner.log()
        );
        assert!(
            process_is_running(escapee),
            "test did not retain the unattributable helper escapee"
        );
    }
}

#[test]
fn cross_uid_helper_cleanup_contains_reconciliation_escapees() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let job_pid = temp.path().join("job.pid");
    let privileged_descendant = temp.path().join("privileged-helper.pid");
    let reconcile_escapee = temp.path().join("reconcile-escapee.pid");
    let invocations = temp.path().join("sudo-invocations");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    for path in [&privileged_descendant, &reconcile_escapee, &invocations] {
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
            "jobs:\n  helper_cleanup:\n    command: \"echo $$ > {}; exec sleep 30\"\n    sudo_user: {username}\n    shutdown_timeout: 0\n",
            job_pid.display()
        ),
    )
    .unwrap();
    let _cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: job_pid.clone(),
    };
    let _privileged_cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: privileged_descendant.clone(),
    };
    let _helper_cleanup = ProcessCleanup(vec![reconcile_escapee.clone()]);
    let path = install_cross_uid_helper_escape_sudo(
        temp.path(),
        &privileged_descendant,
        &reconcile_escapee,
        &invocations,
    );

    let mut runner = Runner::start_with_path(&config, log, &path);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
    }));
    runner.signal(libc::SIGUSR1);
    assert!(
        wait_until(Duration::from_secs(5), || {
            !read_pids(std::slice::from_ref(&privileged_descendant)).is_empty()
        }),
        "privileged signal-helper descendant did not start: {}",
        runner.log()
    );
    assert!(
        wait_until(Duration::from_secs(5), || {
            !read_pids(std::slice::from_ref(&reconcile_escapee)).is_empty()
        }),
        "privileged reconciliation helper did not run: {}; sudo invocations: {}",
        runner.log(),
        fs::read_to_string(&invocations).unwrap_or_default()
    );
    let helper_pids = read_pids(&[privileged_descendant.clone(), reconcile_escapee.clone()]);
    assert_eq!(helper_pids.len(), 2);
    assert!(
        wait_until(Duration::from_secs(5), || helper_pids
            .iter()
            .all(|pid| !process_is_running(*pid))),
        "helper cleanup left a privileged or escaped descendant alive: {}",
        runner.log()
    );
    assert!(
        runner.is_running(),
        "signal-helper failure stopped supervision"
    );

    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
}

#[test]
fn surviving_reconciliation_helper_root_stops_supervision() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let job_pid = temp.path().join("job.pid");
    let privileged_pid = temp.path().join("privileged.pid");
    let helper_pid = temp.path().join("reconcile-helper.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    for path in [&job_pid, &privileged_pid] {
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
            "jobs:\n  helper_cleanup:\n    command: \"echo $$ > {}; exec sleep 30\"\n    sudo_user: {username}\n    shutdown_timeout: 0\n",
            job_pid.display()
        ),
    )
    .unwrap();
    let _job_cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: job_pid.clone(),
    };
    let _privileged_cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: privileged_pid.clone(),
    };
    let _helper_cleanup = ProcessCleanup(vec![helper_pid.clone()]);
    let fault_library =
        build_helper_root_fault_library(temp.path(), &helper_pid, "job-runner-reconcile");
    let path = install_expiring_privileged_helper_sudo(temp.path(), &privileged_pid);
    let environment = [("LD_PRELOAD", fault_library.to_str().unwrap())];

    let mut runner = Runner::start_with_path_and_env(&config, log, &path, &environment);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
    }));
    runner.signal(libc::SIGUSR1);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&helper_pid)).is_empty()
    }));
    let helper = read_pids(std::slice::from_ref(&helper_pid))[0];
    let status = runner.wait(Duration::from_secs(8));

    assert!(
        !status.success(),
        "surviving reconciliation-helper root did not stop supervision: {}",
        runner.log()
    );
    assert!(
        process_is_running(helper),
        "test did not retain the reconciliation-helper root"
    );
}

#[test]
fn unresolved_reconciliation_helper_root_stops_supervision() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let job_pid = temp.path().join("job.pid");
    let privileged_pid = temp.path().join("privileged.pid");
    let escapee_pid = temp.path().join("reconcile-helper-escapee.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    for path in [&job_pid, &privileged_pid] {
        File::create(path).unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o666);
        fs::set_permissions(path, permissions).unwrap();
    }
    let mut permissions = fs::metadata(temp.path()).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(temp.path(), permissions).unwrap();
    let fault_library = build_partial_helper_capture_fault_library(temp.path());
    fs::write(
        &config,
        format!(
            "jobs:\n  helper_cleanup:\n    command: \"echo $$ > {}; exec sleep 30\"\n    sudo_user: {username}\n    shutdown_timeout: 0\n",
            job_pid.display()
        ),
    )
    .unwrap();
    let _job_cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: job_pid.clone(),
    };
    let _privileged_cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: privileged_pid.clone(),
    };
    let _escapee_cleanup = ProcessCleanup(vec![escapee_pid.clone()]);
    let path = install_partial_capture_reconcile_sudo(temp.path(), &privileged_pid, &escapee_pid);
    let environment = [
        ("LD_PRELOAD", fault_library.to_str().unwrap()),
        ("JOB_RUNNER_PARTIAL_CAPTURE_HELPER", "job-runner-reconcile"),
    ];

    let mut runner = Runner::start_with_path_and_env(&config, log, &path, &environment);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
    }));
    runner.signal(libc::SIGUSR1);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&escapee_pid)).is_empty()
    }));
    let escapee = read_pids(std::slice::from_ref(&escapee_pid))[0];
    let status = runner.wait(Duration::from_secs(8));

    assert!(
        !status.success(),
        "unresolved reconciliation-helper root did not stop supervision: {}",
        runner.log()
    );
    assert!(
        process_is_running(escapee),
        "test did not retain the unattributable reconciliation escapee"
    );
}

#[test]
fn reconciliation_completion_failure_is_not_erased_after_targets_exit() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let job_pid = temp.path().join("job.pid");
    let runs = temp.path().join("runs");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    for path in [&job_pid, &runs] {
        File::create(path).unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o666);
        fs::set_permissions(path, permissions).unwrap();
    }
    let mut permissions = fs::metadata(temp.path()).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(temp.path(), permissions).unwrap();
    let command = format!(
        "echo run >> {runs}; (setsid sh -c 'echo $$ > {job_pid}; exec sleep 30' &); while [ ! -s {job_pid} ]; do sleep 0.01; done",
        runs = runs.display(),
        job_pid = job_pid.display(),
    );
    fs::write(
        &config,
        format!(
            "jobs:\n  cleanup_confirmation:\n    command: {}\n    sudo_user: {username}\n    restart_delay: 100\n    shutdown_timeout: 0\n",
            serde_json::to_string(&command).unwrap()
        ),
    )
    .unwrap();
    let _cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: job_pid.clone(),
    };
    let path = install_killing_reconcile_sudo(temp.path());

    let mut runner = Runner::start_with_path(&config, log, &path);
    let status = runner.wait(Duration::from_secs(8));

    assert!(
        !status.success(),
        "reconciliation-helper containment failure allowed a restart: {}",
        runner.log()
    );
    assert_eq!(
        fs::read_to_string(&runs).unwrap().lines().count(),
        1,
        "job restarted after unconfirmed reconciliation-helper cleanup"
    );
}

#[test]
fn reconciliation_helper_containment_failure_stops_supervision() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let job_pid = temp.path().join("job.pid");
    let privileged_descendant = temp.path().join("privileged-helper.pid");
    let reconcile_escapee = temp.path().join("reconcile-escapee.pid");
    let reconcile_marker = temp.path().join("reconcile-started");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    for path in [&privileged_descendant, &reconcile_escapee] {
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
            "jobs:\n  helper_cleanup:\n    command: \"echo $$ > {}; exec sleep 30\"\n    sudo_user: {username}\n    shutdown_timeout: 0\n",
            job_pid.display()
        ),
    )
    .unwrap();
    let _job_cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: job_pid.clone(),
    };
    let _privileged_cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: privileged_descendant.clone(),
    };
    let _escapee_cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: reconcile_escapee.clone(),
    };
    let path = install_leaking_cross_uid_reconcile_sudo(
        temp.path(),
        &privileged_descendant,
        &reconcile_escapee,
        &reconcile_marker,
    );

    let mut runner = Runner::start_with_path(&config, log, &path);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
    }));
    runner.signal(libc::SIGUSR1);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&privileged_descendant)).is_empty()
    }));
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&reconcile_escapee)).is_empty()
    }));

    let status = runner.wait(Duration::from_secs(8));
    assert!(
        !status.success(),
        "runner ignored helper containment failure"
    );
    assert!(
        runner.log().contains("Signal helper containment failed"),
        "{}",
        runner.log()
    );
    let privileged = read_pids(std::slice::from_ref(&privileged_descendant))[0];
    assert!(
        !process_is_running(privileged),
        "original privileged helper target survived"
    );
    assert!(
        read_pids(std::slice::from_ref(&reconcile_escapee))
            .into_iter()
            .any(process_is_running),
        "test did not exercise a surviving reconciliation-helper escapee"
    );
}

#[test]
fn reconciliation_scan_failure_stops_supervision() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let job_pid = temp.path().join("job.pid");
    let privileged_descendant = temp.path().join("privileged-helper.pid");
    let reconcile_escapee = temp.path().join("reconcile-escapee.pid");
    let reconcile_marker = temp.path().join("reconcile-started");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    for path in [&privileged_descendant, &reconcile_escapee] {
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
            "jobs:\n  helper_cleanup:\n    command: \"echo $$ > {}; exec sleep 30\"\n    sudo_user: {username}\n    shutdown_timeout: 0\n",
            job_pid.display()
        ),
    )
    .unwrap();
    let _job_cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: job_pid.clone(),
    };
    let _privileged_cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: privileged_descendant.clone(),
    };
    let _escapee_cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: reconcile_escapee.clone(),
    };
    let fault_library = build_proc_capture_fault_library(temp.path());
    let path = install_leaking_cross_uid_reconcile_sudo(
        temp.path(),
        &privileged_descendant,
        &reconcile_escapee,
        &reconcile_marker,
    );
    let environment = [
        ("LD_PRELOAD", fault_library.to_str().unwrap()),
        (
            "JOB_RUNNER_PROC_FAULT_MARKER",
            reconcile_marker.to_str().unwrap(),
        ),
    ];

    let mut runner = Runner::start_with_path_and_env(&config, log, &path, &environment);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
    }));
    runner.signal(libc::SIGUSR1);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&privileged_descendant)).is_empty()
    }));
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&reconcile_escapee)).is_empty()
    }));

    let status = runner.wait(Duration::from_secs(8));
    assert!(
        !status.success(),
        "runner ignored an unconfirmed reconciliation-helper scan"
    );
    assert!(
        runner
            .log()
            .contains("could not verify sudo cleanup helper descendants"),
        "{}",
        runner.log()
    );
    let privileged = read_pids(std::slice::from_ref(&privileged_descendant))[0];
    assert!(
        !process_is_running(privileged),
        "original privileged helper target survived"
    );
    assert!(
        read_pids(std::slice::from_ref(&reconcile_escapee))
            .into_iter()
            .any(process_is_running),
        "test did not exercise the descendant hidden by the failed scan"
    );
}

#[test]
fn graceful_shutdown_fails_when_the_shim_reports_cleanup_failure() {
    let temp = tempfile::tempdir().unwrap();
    let ready = temp.path().join("ready");
    let escapee = temp.path().join("escapee.pid");
    let config = temp.path().join("jobs.json");
    let log = temp.path().join("runner.log");
    let fault_library = build_partial_helper_capture_fault_library(temp.path());
    let command = format!(
        "trap 'setsid sh -c \"echo \\$\\$ > {}; exec sleep 30\" & while [ ! -s {} ]; do sleep 0.001; done; exit 0' TERM; touch {}; while :; do sleep 1; done",
        escapee.display(),
        escapee.display(),
        ready.display()
    );
    fs::write(
        &config,
        format!(
            "{{\"jobs\":{{\"job\":{{\"command\":{},\"shutdown_timeout\":5}}}}}}",
            serde_json::to_string(&command).unwrap()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![escapee.clone()]);
    let environment = [
        ("LD_PRELOAD", fault_library.to_str().unwrap()),
        ("JOB_RUNNER_PARTIAL_CAPTURE_HELPER", ready.to_str().unwrap()),
        ("JOB_RUNNER_PARTIAL_CAPTURE_OWN_ROOT", "1"),
        ("JOB_RUNNER_PARTIAL_CAPTURE_INCLUDE_READY", "1"),
    ];

    let mut runner = Runner::start_with_env(&config, log, &[], &environment);
    assert!(wait_until(Duration::from_secs(5), || ready.exists()));
    runner.signal(libc::SIGTERM);
    let status = runner.wait(Duration::from_secs(8));

    assert!(
        !status.success(),
        "runner ignored the shim's graceful-shutdown cleanup failure"
    );
    assert!(
        runner
            .log()
            .contains("job shim reported a descendant supervision failure"),
        "{}",
        runner.log()
    );
    assert!(
        !read_pids(std::slice::from_ref(&escapee)).is_empty(),
        "TERM handler did not create the escaped descendant"
    );
}

#[test]
fn abrupt_shim_exit_cleans_startup_observed_group_member() {
    let temp = tempfile::tempdir().unwrap();
    let job_pid = temp.path().join("job.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    fs::write(
        &config,
        format!(
            "jobs:\n  job:\n    command: \"echo $$ > {}; exec sleep 30\"\n    shutdown_timeout: 1\n",
            job_pid.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![job_pid.clone()]);

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
            && first_started_pid(&runner.log()).is_some()
    }));
    let job = read_pids(std::slice::from_ref(&job_pid))[0];
    let shim = first_started_pid(&runner.log()).unwrap();
    assert_eq!(unsafe { libc::kill(shim as libc::pid_t, libc::SIGKILL) }, 0);

    let status = runner.wait(Duration::from_secs(6));
    assert!(!status.success(), "abrupt shim exit was not fatal");
    assert!(
        wait_until(Duration::from_secs(2), || !process_is_running(job)),
        "startup-observed same-group workload survived: {}",
        runner.log()
    );
}

#[test]
fn externally_killed_shim_is_not_mistaken_for_later_escalation() {
    let temp = tempfile::tempdir().unwrap();
    let job_pid = temp.path().join("job.pid");
    let shim_pid = temp.path().join("shim.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    fs::write(
        &config,
        format!(
            "jobs:\n  job:\n    command: \"trap '' TERM; echo $$ > {}; echo $PPID > {}; exec sleep 30\"\n    shutdown_timeout: 2\n",
            job_pid.display(),
            shim_pid.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![job_pid.clone()]);

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(5), || {
        read_pids(&[job_pid.clone(), shim_pid.clone()]).len() == 2
    }));
    runner.signal(libc::SIGTERM);
    assert!(wait_until(Duration::from_secs(3), || runner
        .log()
        .contains("shutting down active processes")));
    let shim = read_pids(std::slice::from_ref(&shim_pid))[0];
    assert_eq!(unsafe { libc::kill(shim as libc::pid_t, libc::SIGKILL) }, 0);
    assert!(wait_until(Duration::from_secs(1), || !process_is_running(
        shim
    )));

    let status = runner.wait(Duration::from_secs(6));
    assert!(
        !status.success(),
        "runner treated an earlier external shim kill as its own escalation"
    );
    assert!(
        runner
            .log()
            .contains("job shim exited without confirming descendant cleanup"),
        "{}",
        runner.log()
    );
}

#[test]
fn stalled_delivery_does_not_hide_a_pre_escalation_shim_exit() {
    let Some(username) = current_username() else {
        eprintln!("skipped: current username unavailable");
        return;
    };
    if !sudo_available_for(&username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let helper_pid = temp.path().join("term-helper.pid");
    let job_pid = temp.path().join("job.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    let path = install_hanging_term_signal_sudo(temp.path(), Some(&helper_pid));
    fs::write(
        &config,
        format!(
            "jobs:\n  job:\n    command: \"trap '' TERM; echo $$ > {}; exec sleep 30\"\n    sudo_user: {}\n    shutdown_timeout: 30\n",
            job_pid.display(),
            username
        ),
    )
    .unwrap();
    let _job_cleanup = SudoProcessCleanup {
        username,
        pid_path: job_pid.clone(),
    };
    let _helper_cleanup = ProcessCleanup(vec![helper_pid.clone()]);

    let mut runner = Runner::start_with_path(&config, log, &path);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
            && first_started_pid(&runner.log()).is_some()
    }));
    let shim = first_started_pid(&runner.log()).unwrap();
    runner.signal(libc::SIGTERM);
    assert!(wait_until(Duration::from_secs(3), || {
        !read_pids(std::slice::from_ref(&helper_pid)).is_empty()
    }));
    assert_eq!(unsafe { libc::kill(shim as libc::pid_t, libc::SIGKILL) }, 0);
    assert!(wait_until(Duration::from_secs(1), || !process_is_running(
        shim
    )));
    runner.signal(libc::SIGTERM);

    let status = runner.wait(Duration::from_secs(6));
    assert!(
        !status.success(),
        "stalled signal delivery hid a pre-escalation shim exit"
    );
    assert!(
        runner
            .log()
            .contains("job shim exited without confirming descendant cleanup"),
        "{}",
        runner.log()
    );
}

#[test]
fn queued_deadline_does_not_hide_a_pre_escalation_shim_exit() {
    let temp = tempfile::tempdir().unwrap();
    let job_pid = temp.path().join("job.pid");
    let term_seen = temp.path().join("term-seen");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    fs::write(
        &config,
        format!(
            "jobs:\n  job:\n    command: \"trap 'touch {}' TERM; echo $$ > {}; while :; do sleep 1; done\"\n    shutdown_timeout: 1\n",
            term_seen.display(),
            job_pid.display()
        ),
    )
    .unwrap();
    let _cleanup = ProcessCleanup(vec![job_pid.clone()]);

    let mut runner = Runner::start(&config, log, &["-t", "1"]);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&job_pid)).is_empty()
            && first_started_pid(&runner.log()).is_some()
    }));
    let runner_pid = runner.child.id();
    let shim = first_started_pid(&runner.log()).unwrap();
    runner.signal(libc::SIGTERM);
    assert!(
        wait_until(Duration::from_secs(3), || term_seen.exists()),
        "graceful delivery did not complete: {}",
        runner.log()
    );

    assert_eq!(
        unsafe { libc::kill(runner_pid as libc::pid_t, libc::SIGSTOP) },
        0
    );
    assert!(wait_until(Duration::from_secs(1), || {
        process_state(runner_pid) == Some('T')
    }));
    assert_eq!(unsafe { libc::kill(shim as libc::pid_t, libc::SIGKILL) }, 0);
    assert!(wait_until(Duration::from_secs(1), || {
        process_state(shim).is_none_or(|state| state == 'Z')
    }));
    thread::sleep(Duration::from_millis(1100));
    assert_eq!(
        unsafe { libc::kill(runner_pid as libc::pid_t, libc::SIGCONT) },
        0
    );

    let status = runner.wait(Duration::from_secs(5));
    assert!(
        !status.success(),
        "runner treated a queued pre-escalation shim exit as its own SIGKILL: {}",
        runner.log()
    );
    assert!(
        runner
            .log()
            .contains("job shim exited without confirming descendant cleanup"),
        "{}",
        runner.log()
    );
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
    let signal_invocations = temp.path().join("signal-invocations");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    for path in [&pids, &received, &ready, &signal_invocations] {
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
    let path = install_counting_signal_sudo(temp.path(), &signal_invocations);

    let mut runner = Runner::start_with_path(&config, log, &path);
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
    assert_eq!(
        fs::read_to_string(&signal_invocations)
            .unwrap()
            .lines()
            .count(),
        1,
        "sudo targets were not delivered in one helper invocation"
    );
    assert!(runner.is_running(), "USR1 restarted the sudo job");
    assert!(process_is_running(process_ids[0]));
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
    assert!(wait_until(Duration::from_secs(2), || process_ids
        .iter()
        .all(|pid| !process_is_running(*pid))));
}

#[test]
fn sudo_named_workload_still_receives_forwarded_signals() {
    let Some(username) = current_username() else {
        eprintln!("skipped: current username unavailable");
        return;
    };
    if !sudo_available_for(&username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let received = temp.path().join("received");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    File::create(&received).unwrap();
    let mut permissions = fs::metadata(&received).unwrap().permissions();
    permissions.set_mode(0o666);
    fs::set_permissions(&received, permissions).unwrap();
    let mut permissions = fs::metadata(temp.path()).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(temp.path(), permissions).unwrap();
    let probe = build_signal_probe_named_sudo(temp.path());
    fs::write(
        &config,
        format!(
            "jobs:\n  named_sudo:\n    command: \"exec {} {}\"\n    sudo_user: {}\n    shutdown_timeout: 1\n",
            probe.display(),
            received.display(),
            username
        ),
    )
    .unwrap();
    let _cleanup = IdentityScopedSudoProcessCleanup {
        username: username.clone(),
        identity_path: received.clone(),
    };

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(5), || {
        fs::read_to_string(&received)
            .is_ok_and(|contents| contents.lines().any(|line| line == "ready"))
    }));
    runner.signal(libc::SIGUSR1);
    assert!(
        wait_until(Duration::from_secs(3), || {
            fs::read_to_string(&received)
                .is_ok_and(|contents| contents.lines().any(|line| line.starts_with("received ")))
        }),
        "workload named sudo did not receive USR1: {}",
        runner.log()
    );

    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(5)).success());
}

#[test]
fn identity_scoped_cleanup_rejects_a_reused_pid() {
    let pid = std::process::id();
    let (process_group, start_time) = process_group_and_start_time(pid).unwrap();

    assert_eq!(
        process_group_for_identity(pid, start_time),
        Some(process_group)
    );
    assert_eq!(process_group_for_identity(pid, start_time + 1), None);
}

#[test]
fn sudo_sigkill_retries_after_a_transient_helper_failure() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }

    let temp = tempfile::tempdir().unwrap();
    let pid_file = temp.path().join("job.pid");
    let invocations = temp.path().join("kill-invocations");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    for path in [&pid_file, &invocations] {
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
            "jobs:\n  retry_kill:\n    command: |\n      echo $$ > {pid_file}\n      trap '' TERM\n      exec sleep 30\n    sudo_user: {username}\n    shutdown_timeout: 0\n",
            pid_file = pid_file.display(),
        ),
    )
    .unwrap();
    let _cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: pid_file.clone(),
    };
    let path = install_fail_first_kill_sudo(temp.path(), &invocations);

    let mut runner = Runner::start_with_path(&config, log, &path);
    assert!(wait_until(Duration::from_secs(5), || {
        !read_pids(std::slice::from_ref(&pid_file)).is_empty()
    }));
    let pid = read_pids(std::slice::from_ref(&pid_file))[0];
    runner.signal(libc::SIGTERM);
    assert!(runner.wait(Duration::from_secs(6)).success());
    assert!(!process_is_running(pid));
    assert_eq!(
        fs::read_to_string(&invocations).unwrap().lines().count(),
        2,
        "SIGKILL helper was not retried exactly once"
    );
}

#[test]
fn sudo_job_shim_kills_escaped_descendant_before_exiting() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }

    let temp = tempfile::tempdir().unwrap();
    let descendant = temp.path().join("descendant.pid");
    let log = temp.path().join("shim.log");
    File::create(&descendant).unwrap();
    let mut permissions = fs::metadata(&descendant).unwrap().permissions();
    permissions.set_mode(0o666);
    fs::set_permissions(&descendant, permissions).unwrap();
    let mut permissions = fs::metadata(temp.path()).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(temp.path(), permissions).unwrap();
    let command = format!(
        "setsid sh -c 'echo $$ > {}; exec sleep 30' & while [ ! -s {} ]; do sleep 0.01; done",
        descendant.display(),
        descendant.display()
    );
    let output = File::create(&log).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_jr"))
        .args([
            "--job-shim",
            "--shutdown-timeout",
            "1",
            "--sudo-user",
            username,
            "--",
            &command,
        ])
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .spawn_guarded()
        .unwrap();
    let _cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: descendant.clone(),
    };
    let mut shim = Runner {
        child,
        log_path: log,
    };

    let status = shim.wait(Duration::from_secs(5));
    assert!(status.success(), "{}", shim.log());
    let descendant_pid = read_pids(std::slice::from_ref(&descendant))[0];
    assert!(
        wait_until(Duration::from_secs(1), || !process_is_running(
            descendant_pid
        )),
        "sudo descendant survived the shim: {}",
        shim.log()
    );
}

#[test]
fn sudo_job_shim_bounds_a_stalled_cleanup_helper() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }

    let temp = tempfile::tempdir().unwrap();
    let descendant = temp.path().join("descendant.pid");
    let helper_pid = temp.path().join("helper.pid");
    let helper_descendant_pid = temp.path().join("helper-descendant.pid");
    let log = temp.path().join("shim.log");
    File::create(&descendant).unwrap();
    let mut permissions = fs::metadata(&descendant).unwrap().permissions();
    permissions.set_mode(0o666);
    fs::set_permissions(&descendant, permissions).unwrap();
    let mut permissions = fs::metadata(temp.path()).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(temp.path(), permissions).unwrap();
    let command = format!(
        "setsid sh -c 'echo $$ > {}; exec sleep 30' & while [ ! -s {} ]; do sleep 0.01; done",
        descendant.display(),
        descendant.display()
    );
    let output = File::create(&log).unwrap();
    let path = install_hanging_reconcile_sudo(temp.path(), &helper_pid, &helper_descendant_pid);
    let child = Command::new(env!("CARGO_BIN_EXE_jr"))
        .args([
            "--job-shim",
            "--shutdown-timeout",
            "1",
            "--sudo-user",
            username,
            "--",
            &command,
        ])
        .env("PATH", path)
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .spawn_guarded()
        .unwrap();
    let _cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: descendant.clone(),
    };
    let _helper_cleanup = ProcessCleanup(vec![helper_pid.clone(), helper_descendant_pid.clone()]);
    let mut shim = Runner {
        child,
        log_path: log,
    };

    let started = Instant::now();
    assert!(wait_until(Duration::from_secs(3), || {
        !read_pids(std::slice::from_ref(&helper_pid)).is_empty()
            && !read_pids(std::slice::from_ref(&helper_descendant_pid)).is_empty()
    }));
    let status = shim.wait(Duration::from_secs(5));
    assert!(!status.success(), "stalled privileged cleanup was accepted");
    assert!(started.elapsed() < Duration::from_secs(3));
    for pid_path in [&helper_pid, &helper_descendant_pid] {
        let pid = read_pids(std::slice::from_ref(pid_path))[0];
        assert!(
            wait_until(Duration::from_secs(1), || !Path::new(&format!(
                "/proc/{pid}"
            ))
            .exists()),
            "sudo cleanup helper process {pid} was not reaped: {}",
            shim.log()
        );
    }
}

#[test]
fn sudo_graceful_shutdown_notifies_shim_before_workload_exit() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }

    let temp = tempfile::tempdir().unwrap();
    let ready = temp.path().join("ready");
    let graceful = temp.path().join("graceful");
    let release = temp.path().join("release");
    let child_pid = temp.path().join("child.pid");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    for path in [&ready, &graceful, &child_pid] {
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
            "jobs:\n  sudo_graceful:\n    command: |\n      sh -c 'echo $$ > {child_pid}; trap \"while [ ! -e {release} ]; do sleep 0.01; done; echo done > {graceful}; exit 0\" TERM; while :; do sleep 1; done' &\n      touch {ready}\n      trap 'exit 0' TERM\n      wait\n    sudo_user: {username}\n    shutdown_timeout: 2\n",
            child_pid = child_pid.display(),
            graceful = graceful.display(),
            release = release.display(),
            ready = ready.display(),
        ),
    )
    .unwrap();
    let _cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: child_pid.clone(),
    };

    let mut runner = Runner::start(&config, log, &[]);
    assert!(wait_until(Duration::from_secs(5), || ready.exists()
        && !read_pids(std::slice::from_ref(&child_pid)).is_empty()));
    runner.signal(libc::SIGTERM);
    thread::sleep(Duration::from_millis(300));
    File::create(&release).unwrap();
    assert!(runner.wait(Duration::from_secs(6)).success());
    assert!(
        fs::metadata(&graceful).is_ok_and(|metadata| metadata.len() > 0),
        "shim killed a gracefully exiting sudo descendant: {}",
        runner.log()
    );
}

#[test]
fn sudo_signal_helper_failure_fails_the_supervisor() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let path = install_failing_signal_sudo(temp.path());
    let pid_file = temp.path().join("escaped.pid");
    let release = temp.path().join("release");
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
            "jobs:\n  failed_signal:\n    command: |\n      setsid sh -c 'echo $$ > {pid_file}; while :; do sleep 1; done' &\n      while [ ! -s {pid_file} ]; do sleep 0.01; done\n      while [ ! -e {release} ]; do sleep 0.01; done\n      exit 0\n    sudo_user: {username}\n    restart_delay: 10000\n",
            pid_file = pid_file.display(),
            release = release.display(),
            username = username,
        ),
    )
    .unwrap();
    let _cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: pid_file.clone(),
    };
    let mut runner = Runner::start_with_path(&config, log, &path);
    assert!(wait_until(Duration::from_secs(4), || {
        !read_pids(std::slice::from_ref(&pid_file)).is_empty()
    }));
    // The signal-time refresh records the escapee before the shim loses its ancestry.
    runner.signal(libc::SIGUSR1);
    assert!(wait_until(Duration::from_secs(3), || runner
        .log()
        .contains("Signal forwarding failed")));
    File::create(&release).unwrap();
    let status = runner.wait(Duration::from_secs(8));
    assert!(!status.success(), "signal helper failure returned success");
    let escaped_pid = read_pids(std::slice::from_ref(&pid_file))[0];
    assert!(
        process_is_running(escaped_pid),
        "supervisor failed even though no cross-UID survivor remained"
    );
    let output = runner.log();
    assert!(output.contains("sudo signal helper exited"), "{output}");
    assert!(
        !output.contains("Starting in 10.000s"),
        "job restarted: {output}"
    );
}

#[test]
fn sudo_shim_cleanup_failure_stops_restart_without_prior_discovery() {
    let username = "root";
    if !sudo_available_for(username) {
        eprintln!("skipped: noninteractive sudo unavailable for {username}");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let path = install_failing_signal_sudo(temp.path());
    let escaped_pids = temp.path().join("escaped.pids");
    let config = temp.path().join("jobs.yaml");
    let log = temp.path().join("runner.log");
    File::create(&escaped_pids).unwrap();
    let mut permissions = fs::metadata(&escaped_pids).unwrap().permissions();
    permissions.set_mode(0o666);
    fs::set_permissions(&escaped_pids, permissions).unwrap();
    let mut permissions = fs::metadata(temp.path()).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(temp.path(), permissions).unwrap();
    fs::write(
        &config,
        format!(
            "jobs:\n  failed_cleanup:\n    command: |\n      ( setsid sh -c 'echo $$ >> {escaped_pids}; exec sleep 30' & )\n      while [ ! -s {escaped_pids} ]; do sleep 0.01; done\n      exit 0\n    sudo_user: {username}\n    restart_delay: 100\n",
            escaped_pids = escaped_pids.display(),
        ),
    )
    .unwrap();
    let _cleanup = SudoProcessCleanup {
        username: username.to_string(),
        pid_path: escaped_pids.clone(),
    };

    let mut runner = Runner::start_with_path(&config, log, &path);
    let status = runner.wait(Duration::from_secs(8));
    assert!(!status.success(), "shim cleanup failure returned success");
    assert_eq!(
        read_pids(std::slice::from_ref(&escaped_pids)).len(),
        1,
        "job restarted after shim cleanup failure: {}",
        runner.log()
    );
    assert!(
        runner
            .log()
            .contains("job shim reported a descendant supervision failure"),
        "shim cleanup failure was not propagated: {}",
        runner.log()
    );
}

#[test]
fn sudo_user_signal_failure_does_not_stop_supervision() {
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
    assert!(wait_until(Duration::from_secs(3), || runner
        .log()
        .contains("Signal forwarding failed")));
    thread::sleep(Duration::from_millis(500));
    assert!(
        runner.is_running(),
        "failed USR1 forwarding stopped the supervisor"
    );
    assert_eq!(fs::metadata(&received).unwrap().len(), 0);
    let output = runner.log();
    assert!(output.contains("Signal forwarding failed"), "{output}");
    assert!(
        !output.contains("shutting down active processes"),
        "forwarding failure initiated shutdown: {output}"
    );
    runner.signal(libc::SIGTERM);
    let _ = runner.wait(Duration::from_secs(8));
}

#[test]
fn runner_refuses_to_run_as_container_pid_one() {
    if !docker_available() {
        eprintln!("skipped: Docker or ubuntu:24.04 image unavailable");
        return;
    }

    let binary = format!("{}:/runner:ro", env!("CARGO_BIN_EXE_jr"));
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
