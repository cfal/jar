#![cfg(unix)]

include!("support/mod.rs");

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
    fn observing_a_historical_pid_cannot_authorize_an_unrelated_process() {
        let mut unrelated = Command::new("sleep").arg("30").spawn_guarded().unwrap();
        let identity = ProcessIdentity::read(unrelated.id()).unwrap();
        // Retain emergency cleanup, but model a process not owned by a live fixture.
        GUARDED_ROOTS
            .lock()
            .unwrap()
            .retain(|root| *root != identity);
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("old-generation.pid");
        fs::write(&path, identity.pid.to_string()).unwrap();
        assert_eq!(observe_pid_file(&path), vec![identity.pid]);
        assert!(RECORDED.get().unwrap().lock().unwrap()[&path].is_empty());
        cleanup_pid_file(&path, None);
        assert!(unrelated.try_wait().unwrap().is_none());
    }

    #[test]
    fn pid_file_cleanup_accepts_a_verified_fixture_descendant() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("child.pid");
        let mut root = Command::new("sh")
            .arg("-c")
            .arg(format!("sleep 30 & echo $! > {}; wait", path.display()))
            .spawn_guarded()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let pid = loop {
            if let Some(pid) = observe_pid_file(&path).first() {
                break *pid;
            }
            assert!(Instant::now() < deadline, "child never started");
            thread::sleep(Duration::from_millis(20));
        };
        let identity = ProcessIdentity::read(pid).unwrap();
        cleanup_pid_file(&path, None);
        root.wait().unwrap();
        assert!(!identity.running());
    }

    #[test]
    fn privileged_cleanup_script_rechecks_identity_before_signalling() {
        let mut child = Command::new("sleep").arg("30").spawn_guarded().unwrap();
        let identity = ProcessIdentity::read(child.id()).unwrap();
        for start_time in [identity.start_time + 1, identity.start_time] {
            assert!(
                Command::new("sh")
                    .args([
                        "-c",
                        IDENTITY_CHECKED_KILL,
                        "--",
                        &identity.pid.to_string(),
                        &start_time.to_string(),
                        &libc::SIGTERM.to_string(),
                    ])
                    .status()
                    .unwrap()
                    .success()
            );
            if start_time != identity.start_time {
                assert!(child.try_wait().unwrap().is_none());
            }
        }
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
            assert_eq!(
                result.unwrap_err().downcast_ref::<&str>(),
                Some(&"intentional fixture failure"),
                "fixture setup failed before the intended unwind"
            );
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
