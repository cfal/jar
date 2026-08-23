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

#[derive(Debug)]
pub(crate) struct ProcessSnapshot {
    processes: Vec<ProcessStat>,
    started_at: std::time::Instant,
}

impl ProcessSnapshot {
    pub(crate) fn empty() -> Self {
        Self {
            processes: Vec::new(),
            started_at: std::time::Instant::now(),
        }
    }

    pub(crate) fn capture() -> io::Result<Self> {
        let started_at = std::time::Instant::now();
        Ok(Self {
            processes: snapshot()?,
            started_at,
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
        )
    }

    pub(crate) fn started_after(&self, instant: std::time::Instant) -> bool {
        self.started_at >= instant
    }
}

pub(crate) fn identity(pid: u32) -> io::Result<ProcessIdentity> {
    Ok(read_stat(pid)?.identity)
}

pub(crate) fn is_running(process: ProcessIdentity) -> bool {
    is_running_checked(process).unwrap_or(false)
}

pub(crate) fn is_running_checked(process: ProcessIdentity) -> io::Result<bool> {
    is_running_checked_with(process, read_stat)
}

fn is_running_checked_with<R>(process: ProcessIdentity, mut read_process: R) -> io::Result<bool>
where
    R: FnMut(u32) -> io::Result<ProcessStat>,
{
    read_process(process.pid)
        .map(|stat| stat.identity == process && stat.state != 'Z')
        .or_else(|error| {
            if process_disappeared(&error) {
                Ok(false)
            } else {
                Err(io::Error::new(
                    error.kind(),
                    format!(
                        "could not inspect /proc/{}/stat after signal delivery: {error}",
                        process.pid
                    ),
                ))
            }
        })
}

pub(crate) fn exists(process: ProcessIdentity) -> bool {
    read_stat(process.pid)
        .map(|stat| stat.identity == process)
        .unwrap_or(false)
}

fn snapshot() -> io::Result<Vec<ProcessStat>> {
    let entries = std::fs::read_dir("/proc").map_err(|error| {
        io::Error::new(error.kind(), format!("could not enumerate /proc: {error}"))
    })?;

    snapshot_entries(entries)
}

fn snapshot_entries(
    entries: impl IntoIterator<Item = io::Result<std::fs::DirEntry>>,
) -> io::Result<Vec<ProcessStat>> {
    snapshot_entries_with(entries, read_stat)
}

fn snapshot_entries_with<R>(
    entries: impl IntoIterator<Item = io::Result<std::fs::DirEntry>>,
    mut read_process: R,
) -> io::Result<Vec<ProcessStat>>
where
    R: FnMut(u32) -> io::Result<ProcessStat>,
{
    let mut processes = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            io::Error::new(error.kind(), format!("could not enumerate /proc: {error}"))
        })?;
        let name = entry.file_name();
        let Some(pid) = name.to_str().and_then(|name| name.parse::<u32>().ok()) else {
            continue;
        };
        match read_process(pid) {
            Ok(stat) => processes.push(stat),
            Err(error) if process_disappeared(&error) => {}
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("could not inspect /proc/{pid}/stat: {error}"),
                ));
            }
        }
    }

    Ok(processes)
}

fn process_disappeared(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ESRCH)
}

fn read_stat(pid: u32) -> io::Result<ProcessStat> {
    let contents = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    parse_stat(&contents)
}

fn parse_stat(contents: &str) -> io::Result<ProcessStat> {
    let open = contents.find('(').ok_or_else(invalid_stat)?;
    let close = contents.rfind(')').ok_or_else(invalid_stat)?;
    if close <= open {
        return Err(invalid_stat());
    }

    let pid = contents[..open]
        .trim()
        .parse::<u32>()
        .map_err(|_| invalid_stat())?;
    let fields: Vec<&str> = contents[close + 1..].split_whitespace().collect();
    if fields.len() <= 19 {
        return Err(invalid_stat());
    }

    Ok(ProcessStat {
        identity: ProcessIdentity {
            pid,
            process_group: fields[2].parse().map_err(|_| invalid_stat())?,
            start_time: fields[19].parse().map_err(|_| invalid_stat())?,
        },
        parent_pid: fields[1].parse().map_err(|_| invalid_stat())?,
        state: fields[0].chars().next().ok_or_else(invalid_stat)?,
    })
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

    owned
        .into_iter()
        .filter(|pid| *pid != root.pid)
        .filter_map(|pid| by_pid.get(&pid).map(|stat| stat.identity))
        .collect()
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

    #[test]
    fn parses_comm_with_spaces_and_parentheses() {
        let mut fields = vec!["S", "10", "123", "123"];
        fields.extend(std::iter::repeat_n("0", 15));
        fields.push("999");
        let parsed = parse_stat(&format!("123 (worker (busy) name) {}", fields.join(" "))).unwrap();

        assert_eq!(parsed.identity.pid, 123);
        assert_eq!(parsed.parent_pid, 10);
        assert_eq!(parsed.identity.process_group, 123);
        assert_eq!(parsed.identity.start_time, 999);
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

        let related = collect_related(&snapshot, root, true, &HashSet::new(), true);
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

        let related = collect_related(&snapshot, root, true, &known, true);
        assert_eq!(related, HashSet::from([escaped, snapshot[1].identity]));

        let reused = [stat(102, 1, 102, 99), stat(103, 102, 102, 4)];
        assert!(collect_related(&reused, root, true, &known, true).is_empty());
    }

    #[test]
    fn refreshes_process_group_without_changing_stable_identity() {
        let root = stat(100, 1, 100, 1).identity;
        let before_setsid = stat(102, 100, 100, 3).identity;
        let known = HashSet::from([before_setsid]);
        let snapshot = [stat(102, 1, 102, 3), stat(103, 102, 102, 4)];

        let related = collect_related(&snapshot, root, true, &known, true);
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

        assert!(collect_related(&snapshot, root, true, &HashSet::new(), false).is_empty());

        let known = HashSet::from([snapshot[0].identity]);
        assert_eq!(
            collect_related(&snapshot, root, true, &known, false),
            HashSet::from([snapshot[0].identity, snapshot[1].identity])
        );

        assert_eq!(
            collect_related(&snapshot, root, true, &HashSet::new(), true),
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

        for errno in [libc::ENOENT, libc::ESRCH] {
            let disappeared =
                snapshot_entries_with(std::fs::read_dir(temp.path()).unwrap(), |_| {
                    Err(io::Error::from_raw_os_error(errno))
                })
                .unwrap();
            assert!(disappeared.is_empty());
        }

        let error = snapshot_entries_with(std::fs::read_dir(temp.path()).unwrap(), |_| {
            Err(io::Error::from(io::ErrorKind::PermissionDenied))
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(
            error
                .to_string()
                .contains("could not inspect /proc/123/stat")
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
}
