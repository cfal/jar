use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProcessIdentity {
    pub(crate) pid: u32,
    pub(crate) process_group: u32,
    pub(crate) start_time: u64,
}

impl PartialEq for ProcessIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.pid == other.pid && self.start_time == other.start_time
    }
}

impl Eq for ProcessIdentity {}

impl Hash for ProcessIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.pid.hash(state);
        self.start_time.hash(state);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProcessStat {
    identity: ProcessIdentity,
    parent_pid: u32,
    state: char,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProcessInspection {
    Live(ProcessStat),
    Departed,
}

#[derive(Debug)]
struct SnapshotContents {
    processes: Vec<ProcessStat>,
    anomaly_count: usize,
    first_anomaly: Option<String>,
    anomaly_kinds: HashSet<io::ErrorKind>,
    unresolved_pids: HashSet<u32>,
}

#[derive(Debug)]
pub(crate) struct ProcessSnapshot {
    processes: Vec<ProcessStat>,
    started_at: std::time::Instant,
    anomaly_count: usize,
    first_anomaly: Option<String>,
    anomaly_kinds: HashSet<io::ErrorKind>,
    unresolved_pids: HashSet<u32>,
}

impl ProcessSnapshot {
    pub(crate) fn empty() -> Self {
        Self {
            processes: Vec::new(),
            started_at: std::time::Instant::now(),
            anomaly_count: 0,
            first_anomaly: None,
            anomaly_kinds: HashSet::new(),
            unresolved_pids: HashSet::new(),
        }
    }

    pub(crate) fn capture() -> io::Result<Self> {
        let started_at = std::time::Instant::now();
        let contents = snapshot()?;
        Ok(Self {
            processes: contents.processes,
            started_at,
            anomaly_count: contents.anomaly_count,
            first_anomaly: contents.first_anomaly,
            anomaly_kinds: contents.anomaly_kinds,
            unresolved_pids: contents.unresolved_pids,
        })
    }

    pub(crate) fn related_processes(
        &self,
        root: ProcessIdentity,
        include_process_group: bool,
        known: &HashSet<ProcessIdentity>,
    ) -> HashSet<ProcessIdentity> {
        collect_related(
            &self.processes,
            root,
            include_process_group,
            known,
            include_process_group,
            &self.unresolved_pids,
        )
    }

    pub(crate) fn started_after(&self, instant: std::time::Instant) -> bool {
        self.started_at >= instant
    }

    pub(crate) fn anomaly_summary(&self) -> Option<String> {
        self.first_anomaly.as_ref().map(|first| {
            format!(
                "skipped {} uninspectable /proc entr{}; first: {first}",
                self.anomaly_count,
                if self.anomaly_count == 1 { "y" } else { "ies" }
            )
        })
    }

    pub(crate) fn anomaly_kinds(&self) -> &HashSet<io::ErrorKind> {
        &self.anomaly_kinds
    }

    #[cfg(test)]
    pub(crate) fn with_anomaly(count: usize, kind: io::ErrorKind, message: &str) -> Self {
        Self::with_anomaly_kinds(count, HashSet::from([kind]), message)
    }

    #[cfg(test)]
    pub(crate) fn with_anomaly_kinds(
        count: usize,
        kinds: HashSet<io::ErrorKind>,
        message: &str,
    ) -> Self {
        Self {
            processes: Vec::new(),
            started_at: std::time::Instant::now(),
            anomaly_count: count,
            first_anomaly: Some(message.to_string()),
            anomaly_kinds: kinds,
            unresolved_pids: HashSet::new(),
        }
    }
}

pub(crate) fn identity(pid: u32) -> io::Result<ProcessIdentity> {
    match read_stat(pid)? {
        ProcessInspection::Live(stat) => Ok(stat.identity),
        ProcessInspection::Departed => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("process {pid} exited before its identity could be read"),
        )),
    }
}

pub(crate) fn validate_procfs() -> io::Result<()> {
    identity(std::process::id()).map(|_| ())
}

pub(crate) fn is_running_checked(process: ProcessIdentity) -> io::Result<bool> {
    is_running_checked_with(process, read_stat)
}

fn is_running_checked_with<R>(process: ProcessIdentity, mut read_process: R) -> io::Result<bool>
where
    R: FnMut(u32) -> io::Result<ProcessInspection>,
{
    match read_process(process.pid) {
        Ok(ProcessInspection::Live(stat)) => {
            Ok(stat.identity == process && !matches!(stat.state, 'Z' | 'X'))
        }
        Ok(ProcessInspection::Departed) => Ok(false),
        Err(error) if process_disappeared(&error) => Ok(false),
        Err(error) => Err(io::Error::new(
            error.kind(),
            format!(
                "could not inspect tracked process /proc/{}/stat: {error}",
                process.pid
            ),
        )),
    }
}

pub(crate) fn exists_checked(process: ProcessIdentity) -> io::Result<bool> {
    exists_checked_with(process, read_stat)
}

fn exists_checked_with<R>(process: ProcessIdentity, mut read_process: R) -> io::Result<bool>
where
    R: FnMut(u32) -> io::Result<ProcessInspection>,
{
    match read_process(process.pid) {
        Ok(ProcessInspection::Live(stat)) => Ok(stat.identity == process),
        Ok(ProcessInspection::Departed) => Ok(false),
        Err(error) if process_disappeared(&error) => Ok(false),
        Err(error) => Err(io::Error::new(
            error.kind(),
            format!(
                "could not inspect tracked process /proc/{}/stat: {error}",
                process.pid
            ),
        )),
    }
}

pub(crate) fn exists(process: ProcessIdentity) -> bool {
    exists_checked(process).unwrap_or(false)
}

fn snapshot() -> io::Result<SnapshotContents> {
    let entries = std::fs::read_dir("/proc").map_err(|error| {
        io::Error::new(error.kind(), format!("could not enumerate /proc: {error}"))
    })?;

    snapshot_entries(entries)
}

fn snapshot_entries(
    entries: impl IntoIterator<Item = io::Result<std::fs::DirEntry>>,
) -> io::Result<SnapshotContents> {
    snapshot_entries_with(entries, read_stat)
}

fn snapshot_entries_with<R>(
    entries: impl IntoIterator<Item = io::Result<std::fs::DirEntry>>,
    mut read_process: R,
) -> io::Result<SnapshotContents>
where
    R: FnMut(u32) -> io::Result<ProcessInspection>,
{
    let mut processes = Vec::new();
    let mut anomaly_count = 0;
    let mut first_anomaly = None;
    let mut first_anomaly_kind = None;
    let mut anomaly_kinds = HashSet::new();
    let mut unresolved_pids = HashSet::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            io::Error::new(error.kind(), format!("could not enumerate /proc: {error}"))
        })?;
        let name = entry.file_name();
        let Some(pid) = name.to_str().and_then(|name| name.parse::<u32>().ok()) else {
            continue;
        };
        match read_process(pid) {
            Ok(ProcessInspection::Live(stat)) => processes.push(stat),
            Ok(ProcessInspection::Departed) => {}
            Err(error) if process_disappeared(&error) => {}
            Err(error) => {
                anomaly_count += 1;
                first_anomaly_kind.get_or_insert(error.kind());
                anomaly_kinds.insert(error.kind());
                unresolved_pids.insert(pid);
                first_anomaly
                    .get_or_insert_with(|| format!("could not inspect /proc/{pid}/stat: {error}"));
            }
        }
    }

    if processes.is_empty() && anomaly_count > 0 {
        return Err(io::Error::new(
            first_anomaly_kind.unwrap_or(io::ErrorKind::Other),
            format!(
                "could not inspect any process in /proc; first error: {}",
                first_anomaly
                    .as_deref()
                    .unwrap_or("unknown inspection error")
            ),
        ));
    }

    Ok(SnapshotContents {
        processes,
        anomaly_count,
        first_anomaly,
        anomaly_kinds,
        unresolved_pids,
    })
}

fn process_disappeared(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ESRCH)
}

fn read_stat(pid: u32) -> io::Result<ProcessInspection> {
    read_stat_with(pid, |pid| std::fs::read(format!("/proc/{pid}/stat")))
}

fn read_stat_with<R>(pid: u32, mut read_process: R) -> io::Result<ProcessInspection>
where
    R: FnMut(u32) -> io::Result<Vec<u8>>,
{
    let contents = read_process(pid)?;
    match parse_stat(&contents) {
        Err(error) if error.kind() == io::ErrorKind::InvalidData => parse_stat(&read_process(pid)?),
        result => result,
    }
}

fn parse_stat(contents: &[u8]) -> io::Result<ProcessInspection> {
    let open = contents
        .iter()
        .position(|byte| *byte == b'(')
        .ok_or_else(invalid_stat)?;
    let close = contents
        .iter()
        .rposition(|byte| *byte == b')')
        .ok_or_else(invalid_stat)?;
    if close <= open {
        return Err(invalid_stat());
    }

    let pid = parse_field(contents[..open].trim_ascii())?;
    let fields: Vec<&[u8]> = contents[close + 1..]
        .split(u8::is_ascii_whitespace)
        .filter(|field| !field.is_empty())
        .collect();
    if fields.len() <= 19 {
        return Err(invalid_stat());
    }

    if fields[0].len() != 1 || !fields[0][0].is_ascii_alphabetic() {
        return Err(invalid_stat());
    }
    let state = fields[0][0] as char;
    let process_group: i32 = parse_field(fields[2])?;
    if state == 'X' || process_group < 0 {
        return Ok(ProcessInspection::Departed);
    }

    Ok(ProcessInspection::Live(ProcessStat {
        identity: ProcessIdentity {
            pid,
            process_group: u32::try_from(process_group).map_err(|_| invalid_stat())?,
            start_time: parse_field(fields[19])?,
        },
        parent_pid: u32::try_from(parse_field::<i32>(fields[1])?).map_err(|_| invalid_stat())?,
        state,
    }))
}

fn parse_field<T: std::str::FromStr>(value: &[u8]) -> io::Result<T> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(invalid_stat)
}

fn invalid_stat() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid /proc process stat")
}

fn collect_related(
    snapshot: &[ProcessStat],
    root: ProcessIdentity,
    include_process_group: bool,
    known: &HashSet<ProcessIdentity>,
    process_group_was_validated: bool,
    unresolved_pids: &HashSet<u32>,
) -> HashSet<ProcessIdentity> {
    let by_pid: HashMap<u32, ProcessStat> = snapshot
        .iter()
        .copied()
        .map(|stat| (stat.identity.pid, stat))
        .collect();
    let root_exists = by_pid
        .get(&root.pid)
        .is_some_and(|stat| stat.identity == root);

    let mut owned: HashSet<u32> = HashSet::new();
    if root_exists {
        owned.insert(root.pid);
    }
    for process in known {
        if by_pid
            .get(&process.pid)
            .is_some_and(|stat| stat.identity == *process)
        {
            owned.insert(process.pid);
        }
    }

    let old_group_is_owned = process_group_was_validated
        || root_exists
        || known.iter().any(|process| {
            by_pid.get(&process.pid).is_some_and(|stat| {
                stat.identity == *process && stat.identity.process_group == root.pid
            })
        });
    if include_process_group && old_group_is_owned {
        owned.extend(
            snapshot
                .iter()
                .filter(|stat| stat.identity.process_group == root.pid)
                .map(|stat| stat.identity.pid),
        );
    }

    loop {
        let before = owned.len();
        for stat in snapshot {
            if owned.contains(&stat.parent_pid) {
                owned.insert(stat.identity.pid);
            }
        }
        if owned.len() == before {
            break;
        }
    }

    let mut related: HashSet<ProcessIdentity> = known
        .iter()
        .copied()
        .filter(|process| unresolved_pids.contains(&process.pid))
        .collect();
    for identity in owned
        .into_iter()
        .filter(|pid| *pid != root.pid)
        .filter_map(|pid| by_pid.get(&pid).map(|stat| stat.identity))
    {
        related.replace(identity);
    }
    related.remove(&root);
    related
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stat(pid: u32, parent_pid: u32, process_group: u32, start_time: u64) -> ProcessStat {
        ProcessStat {
            identity: ProcessIdentity {
                pid,
                process_group,
                start_time,
            },
            parent_pid,
            state: 'S',
        }
    }

    fn stat_bytes(
        pid: u32,
        comm: &[u8],
        state: char,
        parent_pid: i32,
        process_group: i32,
        start_time: u64,
    ) -> Vec<u8> {
        let mut contents = format!("{pid} (").into_bytes();
        contents.extend_from_slice(comm);
        contents.extend_from_slice(
            format!(") {state} {parent_pid} {process_group} {process_group}").as_bytes(),
        );
        for _ in 0..15 {
            contents.extend_from_slice(b" 0");
        }
        contents.extend_from_slice(format!(" {start_time}\n").as_bytes());
        contents
    }

    fn live(inspection: ProcessInspection) -> ProcessStat {
        match inspection {
            ProcessInspection::Live(stat) => stat,
            ProcessInspection::Departed => panic!("expected a live process"),
        }
    }

    #[test]
    fn parses_comm_with_spaces_and_parentheses() {
        let parsed =
            live(parse_stat(&stat_bytes(123, b"worker (busy)\nname", 'S', 10, 123, 999)).unwrap());

        assert_eq!(parsed.identity.pid, 123);
        assert_eq!(parsed.parent_pid, 10);
        assert_eq!(parsed.identity.process_group, 123);
        assert_eq!(parsed.identity.start_time, 999);
    }

    #[test]
    fn parses_non_utf8_comm() {
        let parsed =
            live(parse_stat(&stat_bytes(123, b"worker\xff\xfe", 'S', 10, 123, 999)).unwrap());

        assert_eq!(parsed.identity, stat(123, 10, 123, 999).identity);
        assert_eq!(parsed.parent_pid, 10);
    }

    #[test]
    fn treats_kernel_dead_task_records_as_departed() {
        let captured = b"627717 (reap_race) X 0 -1 -1 0 -1 4227148 33 0 0 0 0 0 0 0 20 0 0 0 18736228 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 17 0 0 0 0 0 0 0 0 0 0 0 0 0";
        assert_eq!(parse_stat(captured).unwrap(), ProcessInspection::Departed);
        assert_eq!(
            parse_stat(&stat_bytes(123, b"worker", 'S', 10, -1, 999)).unwrap(),
            ProcessInspection::Departed
        );
        assert_eq!(
            parse_stat(&stat_bytes(123, b"worker", 'X', 10, 123, 999)).unwrap(),
            ProcessInspection::Departed
        );
    }

    #[test]
    fn rejects_structurally_invalid_stat_records() {
        for contents in [
            b"".as_slice(),
            b"123 worker".as_slice(),
            b"123 (worker) S 1".as_slice(),
        ] {
            assert_eq!(
                parse_stat(contents).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[test]
    fn retries_a_structurally_invalid_stat_record_once() {
        let valid = stat_bytes(123, b"worker", 'S', 10, 123, 999);
        let mut reads = vec![b"incomplete".to_vec(), valid];

        let parsed = read_stat_with(123, |_| Ok(reads.remove(0))).unwrap();

        assert_eq!(live(parsed).identity, stat(123, 10, 123, 999).identity);
        assert!(reads.is_empty());
    }

    #[test]
    fn rejects_a_persistently_invalid_stat_record_after_one_retry() {
        let mut reads = 0;

        let error = read_stat_with(123, |_| {
            reads += 1;
            Ok(b"incomplete".to_vec())
        })
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(reads, 2);
    }

    #[test]
    fn collects_deep_descendants_and_escaped_groups() {
        let root = stat(100, 1, 100, 1).identity;
        let snapshot = [
            stat(100, 1, 100, 1),
            stat(101, 100, 100, 2),
            stat(102, 101, 102, 3),
            stat(103, 102, 102, 4),
            stat(200, 1, 200, 5),
        ];

        let related = collect_related(
            &snapshot,
            root,
            true,
            &HashSet::new(),
            true,
            &HashSet::new(),
        );
        assert_eq!(
            related,
            HashSet::from([
                snapshot[1].identity,
                snapshot[2].identity,
                snapshot[3].identity,
            ])
        );
    }

    #[test]
    fn retains_reparented_known_processes_without_accepting_pid_reuse() {
        let root = stat(100, 1, 100, 1).identity;
        let escaped = stat(102, 1, 102, 3).identity;
        let known = HashSet::from([escaped]);
        let snapshot = [stat(102, 1, 102, 3), stat(103, 102, 102, 4)];

        let related = collect_related(&snapshot, root, true, &known, true, &HashSet::new());
        assert_eq!(related, HashSet::from([escaped, snapshot[1].identity]));

        let reused = [stat(102, 1, 102, 99), stat(103, 102, 102, 4)];
        assert!(collect_related(&reused, root, true, &known, true, &HashSet::new()).is_empty());
        assert!(collect_related(&[], root, true, &known, true, &HashSet::new()).is_empty());
        assert_eq!(
            collect_related(&[], root, true, &known, true, &HashSet::from([102])),
            known
        );
    }

    #[test]
    fn refreshes_process_group_without_changing_stable_identity() {
        let root = stat(100, 1, 100, 1).identity;
        let before_setsid = stat(102, 100, 100, 3).identity;
        let known = HashSet::from([before_setsid]);
        let snapshot = [stat(102, 1, 102, 3), stat(103, 102, 102, 4)];

        let related = collect_related(&snapshot, root, true, &known, true, &HashSet::new());
        let refreshed = related
            .iter()
            .find(|process| process.pid == before_setsid.pid)
            .unwrap();
        assert_eq!(refreshed.process_group, 102);
        assert!(related.contains(&snapshot[1].identity));
    }

    #[test]
    fn requires_a_validated_member_before_adopting_a_leaderless_group() {
        let root = stat(100, 1, 100, 1).identity;
        let snapshot = [stat(101, 1, 100, 2), stat(102, 1, 100, 3)];

        assert!(
            collect_related(
                &snapshot,
                root,
                true,
                &HashSet::new(),
                false,
                &HashSet::new(),
            )
            .is_empty()
        );

        let known = HashSet::from([snapshot[0].identity]);
        assert_eq!(
            collect_related(&snapshot, root, true, &known, false, &HashSet::new(),),
            HashSet::from([snapshot[0].identity, snapshot[1].identity])
        );

        assert_eq!(
            collect_related(
                &snapshot,
                root,
                true,
                &HashSet::new(),
                true,
                &HashSet::new(),
            ),
            HashSet::from([snapshot[0].identity, snapshot[1].identity])
        );
    }

    #[test]
    fn propagates_proc_directory_iteration_errors() {
        let entries = std::iter::once(Err(io::Error::from(io::ErrorKind::PermissionDenied)));

        let error = snapshot_entries(entries).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("could not enumerate /proc"));
    }

    #[test]
    fn ignores_disappearing_process_stat_errors() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("123"), "").unwrap();
        std::fs::write(temp.path().join("124"), "").unwrap();

        for errno in [libc::ENOENT, libc::ESRCH] {
            let disappeared =
                snapshot_entries_with(std::fs::read_dir(temp.path()).unwrap(), |_| {
                    Err(io::Error::from_raw_os_error(errno))
                })
                .unwrap();
            assert!(disappeared.processes.is_empty());
            assert_eq!(disappeared.anomaly_count, 0);
        }

        let anomalous = snapshot_entries_with(std::fs::read_dir(temp.path()).unwrap(), |pid| {
            if pid == 123 {
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            } else {
                Ok(ProcessInspection::Live(stat(124, 1, 124, 1)))
            }
        })
        .unwrap();
        assert_eq!(anomalous.processes, vec![stat(124, 1, 124, 1)]);
        assert_eq!(anomalous.anomaly_count, 1);
        assert!(
            anomalous
                .first_anomaly
                .unwrap()
                .to_string()
                .contains("could not inspect /proc/123/stat")
        );
    }

    #[test]
    fn skips_departed_and_malformed_processes_during_a_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        for pid in [123, 124, 125, 126] {
            std::fs::write(temp.path().join(pid.to_string()), "").unwrap();
        }

        let contents =
            snapshot_entries_with(std::fs::read_dir(temp.path()).unwrap(), |pid| match pid {
                123 => Ok(ProcessInspection::Live(stat(123, 1, 123, 1))),
                124 => Ok(ProcessInspection::Departed),
                125 => Err(invalid_stat()),
                126 => Err(io::Error::from(io::ErrorKind::PermissionDenied)),
                _ => unreachable!(),
            })
            .unwrap();

        assert_eq!(contents.processes, vec![stat(123, 1, 123, 1)]);
        assert_eq!(contents.anomaly_count, 2);
        assert_eq!(
            contents.anomaly_kinds,
            HashSet::from([io::ErrorKind::InvalidData, io::ErrorKind::PermissionDenied,])
        );
        assert_eq!(contents.unresolved_pids, HashSet::from([125, 126]));
        assert!(
            contents
                .first_anomaly
                .unwrap()
                .contains("could not inspect /proc/")
        );
    }

    #[test]
    fn rejects_a_snapshot_when_no_process_can_be_inspected() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("123"), "").unwrap();

        let error = snapshot_entries_with(std::fs::read_dir(temp.path()).unwrap(), |_| {
            Err(io::Error::from(io::ErrorKind::PermissionDenied))
        })
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("could not inspect any process"));
    }

    #[test]
    fn summarizes_snapshot_anomalies() {
        let singular =
            ProcessSnapshot::with_anomaly(1, io::ErrorKind::PermissionDenied, "first failure");
        let plural = ProcessSnapshot::with_anomaly(2, io::ErrorKind::InvalidData, "first failure");

        assert_eq!(
            singular.anomaly_summary().as_deref(),
            Some("skipped 1 uninspectable /proc entry; first: first failure")
        );
        assert_eq!(
            plural.anomaly_summary().as_deref(),
            Some("skipped 2 uninspectable /proc entries; first: first failure")
        );
        assert_eq!(
            singular.anomaly_kinds(),
            &HashSet::from([io::ErrorKind::PermissionDenied])
        );
    }

    #[test]
    fn treats_disappearing_process_as_not_running() {
        let process = stat(123, 1, 123, 1).identity;

        for errno in [libc::ENOENT, libc::ESRCH] {
            assert!(
                !is_running_checked_with(process, |_| { Err(io::Error::from_raw_os_error(errno)) })
                    .unwrap()
            );
        }

        let error = is_running_checked_with(process, |_| {
            Err(io::Error::from(io::ErrorKind::PermissionDenied))
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn treats_dead_process_states_as_not_running() {
        let process = stat(123, 1, 123, 1).identity;

        assert!(!is_running_checked_with(process, |_| Ok(ProcessInspection::Departed)).unwrap());
        for state in ['Z', 'X'] {
            let mut dead = stat(123, 1, 123, 1);
            dead.state = state;
            assert!(
                !is_running_checked_with(process, |_| Ok(ProcessInspection::Live(dead))).unwrap()
            );
        }
    }

    #[test]
    fn zombies_still_hold_their_process_identity() {
        let process = stat(123, 1, 123, 1).identity;
        let mut zombie = stat(123, 1, 123, 1);
        zombie.state = 'Z';

        assert!(exists_checked_with(process, |_| Ok(ProcessInspection::Live(zombie))).unwrap());
        assert!(
            !is_running_checked_with(process, |_| Ok(ProcessInspection::Live(zombie))).unwrap()
        );
        assert!(!exists_checked_with(process, |_| Ok(ProcessInspection::Departed)).unwrap());
    }

    #[test]
    fn parses_the_current_process_stat() {
        let inspection = read_stat(std::process::id()).unwrap();
        let parsed = live(inspection);

        assert_eq!(parsed.identity.pid, std::process::id());
        assert_ne!(parsed.identity.process_group, 0);
        assert_ne!(parsed.identity.start_time, 0);
    }
}
