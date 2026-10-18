# jar

A lightweight asynchronous process supervisor for Linux. It runs multiple jobs,
restarts them after exit, forwards signals, and shuts down their process trees.

The Cargo package is `jar`; the executable is `jr`.

## Features

- **Multi-job execution** - Run multiple processes concurrently from a single config file
- **Auto-restart** - Restart completed jobs with configurable delays
- **Graceful shutdown** - SIGTERM with configurable timeout, then SIGKILL
- **Signal forwarding** - Forward SIGUSR1/SIGUSR2 to all managed processes
- **Process isolation** - Run every job in a dedicated session and signal its process tree by default
- **Colored output** - Each job gets its own color for easy visual distinction
- **Timestamps** - Optional timestamp prefixes on log output
- **Sudo support** - Run jobs as different users
- **YAML/JSON config** - Support for both formats, with comments in JSON

## Installation

```bash
cargo build --release
```

The binary will be at `target/release/jr`.

To install `jr` into Cargo's bin directory:

```bash
cargo install --path . --bin jr
```

Install from crates.io with `cargo install jar`.

## Usage

```bash
# Run with a config file
jr config.yaml

# Wrapper mode (auto-restarts jr on failure)
jr --wrap config.yaml

# Show help
jr --help
```

## Configuration

### Minimal Example

```yaml
jobs:
  web_server:
    command: "npm start"

  worker:
    command: "python worker.py"
```

### Full Example

```yaml
jobs:
  api:
    command: "node server.js"
    cwd: "#cwd#/api"              # Working directory (#cwd# = config file dir)
    color: blue                    # Output color
    delay: 1000                    # Initial startup delay (ms)
    restart_delay: 5000            # Delay before restart after failure (ms)
    disabled: false                # Set to true to skip this job
    process_group: true            # Signal the process tree (default: true)
    shutdown_timeout: 10           # Seconds to wait before SIGKILL
    env:                           # Additional environment variables
      NODE_ENV: production
      PORT: "3000"
    datetime:                      # Timestamp configuration
      enabled: true
      format: "%Y-%m-%d %H:%M:%S"
      local: true

  database:
    command: "postgres -D /var/lib/postgres/data"
    sudo_user: postgres            # Run as different user (requires sudo)
    color: green
```

### JSON with Comments

JSON config files may contain full-line comments beginning with `//`:

```json
{
  // Main configuration
  "jobs": {
    "my_job": {
      "command": "echo hello",
      // This is ignored
      "color": "red"
    }
  }
}
```

## Configuration Reference

### Job Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `command` | string | *required* | Shell source executed by `sh -c` |
| `cwd` | string | runner cwd | Working directory |
| `color` | string | random | Output color (see colors below) |
| `delay` | integer | 0 | Initial startup delay in milliseconds |
| `restart` | boolean | true | Restart after exit; set to false to run once |
| `restart_delay` | integer | 15000 | Delay before restart after exit (ms) |
| `disabled` | boolean | false | Skip this job |
| `process_group` | boolean | true | Signal the job's process group and discovered descendants |
| `shutdown_timeout` | integer | 5 | Seconds to wait for graceful exit |
| `sudo_user` | string | none | Run as this user via sudo |
| `env` | object | none | Additional environment variables |
| `datetime` | object | disabled | Timestamp configuration |
| `prefix` | string | existing layout | Output prefix template; empty string disables prefixes |

### Datetime Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | false | Enable timestamps |
| `format` | string | `%Y-%m-%d %H:%M:%S %Z` | strftime format |
| `local` | boolean | false | Use local time (vs UTC) |

### Available Colors

`red`, `green`, `yellow`, `blue`, `magenta`, `cyan`, `white`, `gray`

### Path Substitution

`#...#` placeholders are literal substitutions in `command` and `cwd`:

| Placeholder | Expands To |
|-------------|------------|
| `#cwd#` | Directory containing the config file |
| `#home#` | User's `$HOME` directory |
| `#runner_cwd#` | Directory where `jr` was started |

Jobs also receive `CWD` and `RUNNER_CWD` environment variables, plus the inherited
`HOME`. The shell expands `$CWD`, `$RUNNER_CWD`, and `$HOME` in commands. A `cwd`
value may be one of those variables or begin with one followed by `/`, such as
`$HOME/work`.

Commands already run through `sh -c`; do not add another `sh -c` unless nested
shell semantics are intentional. With the default `restart: true`, each completed
command is restarted after `restart_delay`, whether it succeeded or failed.
Restarts always wait at least 100 ms to prevent an accidental busy loop.

Set `restart: false` to run a job once, including when it fails to start or exits
unsuccessfully. Its initial `delay`, output draining, and process-tree cleanup
still apply; `restart_delay` is unused. Other jobs continue independently. When
all enabled jobs finish, `jr` exits with status 0 if every job succeeded, or 1
if any non-restarting job failed. Fatal supervision or containment failures still
stop all jobs immediately.

```yaml
jobs:
  migrate:
    command: "./migrate"
    restart: false
```

## Signal Handling

| Signal | Behavior |
|--------|----------|
| `SIGTERM`, `SIGINT`, `SIGHUP`, `SIGQUIT` | Initiate graceful shutdown |
| `SIGUSR1`, `SIGUSR2` | Forward to all managed processes |

A second termination signal skips the remaining graceful timeout and sends
`SIGKILL`.

### Graceful Shutdown

When a termination signal is received:

1. Snapshot and send `SIGTERM` to all managed process trees
2. Wait up to `shutdown_timeout` seconds per job
3. Send `SIGKILL` to processes that haven't exited
4. Reap every direct child, drain output for a bounded interval, and exit

### Process Groups

With the default `process_group: true`, each job runs under a small internal
reaper shim. The shim starts a dedicated noninteractive session via `setsid()`,
acts as a Linux child subreaper, and launches the configured command. This
prevents a job using `kill(0, ...)` from signaling `jr` itself.

The shim:

- Signals target the dedicated process group.
- Reaps daemonized helpers while the main command is running.
- Kills and reaps adopted descendants before reporting a natural command exit,
  including descendants that daemonize before the first `/proc` scan.
- Confirms successful descendant cleanup separately from the command exit status;
  a missing or failed confirmation stops supervision instead of starting an
  overlapping replacement.
- After a termination, waits up to `shutdown_timeout` for descendants, then
  force-cleans them so an externally terminated leader cannot stall restarts.
- Linux `/proc` ancestry is also tracked with PID and process start time so
  descendants that create another session can be signaled when discovered.
- Previously observed descendants, including processes that created another
  session, are killed before the job restarts.

Set `process_group: false` only when direct-shell signaling is required. The job
starts directly in a session without the reaper shim; shutdown signals only its
shell and descendants may survive. Jobs using `sudo_user` always use the shim
and group/tree signaling because the direct `sudo` client is not the target
workload. Validated workload descendants are signaled as the configured user;
if delivery fails and a target remains alive, supervision fails instead of
silently restarting or reporting success.

```yaml
jobs:
  # Process-group signaling is enabled by default; this is explicit for clarity.
  supervisor:
    command: "python manage.py runserver"
    process_group: true
    shutdown_timeout: 10
```

The reaper shim closes the ordinary daemonization gap without privileges, and a
bounded post-`SIGKILL` sweep signals descendants first observed after the
initial kill. It is still not kernel-enforced containment: PID validation has a
check/signal race, an uninterruptible kernel task may remain after `SIGKILL`,
and abruptly killing an unwrapped supervisor can leave jobs alive. If a shim is
abruptly killed, the supervisor refuses to restart that job without cleanup
confirmation. An escaped descendant that was not yet observed may still remain;
wrapper mode reclaims it when the resulting inner-runner failure exits that
generation. Use a delegated cgroup v2 subtree and `cgroup.kill` when zero
surviving descendants or strict isolation is required.

At startup, the supervisor immediately verifies that it can parse its own
`/proc` record and enumerate the process table. After startup, a total capture
failure is observed on the next capture and retried for up to one second while
the last successful snapshot remains published. A capture that keeps failing
past that window stops the scanner and exits the supervisor with an error. The
host-wide reconciliation scan runs every 30 seconds during normal supervision
and every 50 ms during shutdown; startup, signal handling, and natural-exit
cleanup request an immediate snapshot. Healthy child exits are event-driven and
do not wait for a scan. Individual processes can exit while a snapshot is being
captured, and Linux may expose a dead task briefly with incomplete process-group
metadata; those entries are skipped. Other per-process inspection anomalies are
skipped with a rate-limited warning so an unrelated host process cannot
terminate every managed job. A tracked process remains associated with its job
while its procfs record is unreadable, and all other verified targets are still
signaled. Descendants behind a persistently unreadable intermediary cannot be
attributed unless they remain in the managed process group. Inspection errors
remain fatal when validating cleanup because successful containment cannot then
be established.

## Output Format

Each line is prefixed with the job name and stream indicator:

```
[color]jobname[reset] (stream) > message
```

Where `stream` is:

- `(s)` - Status messages (started, exited, etc.)
- `(1)` - stdout
- `(2)` - stderr

Example output:

```
api (s) > Starting in 1s
api (s) > Started (pid 12345)
api (1) > Server listening on port 3000
worker (s) > Started (pid 12346)
worker (1) > Processing jobs...
api (2) > Warning: deprecated API used
```

With timestamps enabled:

```
api (s) > 2024-01-15 10:30:45 > Started (pid 12345)
api (1) > 2024-01-15 10:30:46 > Server listening on port 3000
```

Each serialized output frame is capped at 64 KiB. Long payloads are split, and
oversized prefixes or diagnostics are truncated. Each stream has a 4 MiB
bounded queue and a dedicated writer thread, so blocked output cannot stall
shutdown. Output is dropped after a queue fills or a bounded exit flush expires.

### Custom Prefixes

Set a job's `prefix` to customize its stdout, stderr, and job-status prefixes:

```yaml
jobs:
  api:
    command: "node server.js"
    prefix: "{datetime} {color}[{job}:{stream}]{reset} "
    datetime:
      enabled: true
      format: "%H:%M:%S"
      local: true
```

| Variable | Value |
|----------|-------|
| `{color}` | ANSI code for the configured color, or the automatically assigned color |
| `{reset}` | ANSI reset code; empty when the configured color is invalid |
| `{job}` | Job name, inserted literally |
| `{stream}` | `s` for job status, `1` for stdout, `2` for stderr |
| `{datetime}` | Current time using `datetime.format` and `datetime.local`; empty when disabled |

Automatic colors stay fixed per job across output lines and restarts. Timestamps
are rendered for each output frame. Templates are expanded once: braces in a
job name are not interpreted as variables. Use `{{` and `}}` for literal braces.
Unknown variables and unmatched braces are rejected before any job starts.

Templates include all separators and trailing whitespace. No separator or space
is appended automatically. Set `prefix: ""` for unprefixed job output. Global
supervisor and wrapper messages are not affected.

Omitting `prefix` preserves the existing layout exactly:

```yaml
# Without timestamps:
prefix: "{color}{job}{reset} ({stream}) > "
# With timestamps enabled and a nonempty format:
prefix: "{color}{job}{reset} ({stream}) > {datetime} > "
```

## Wrapper Mode

The `--wrap` flag runs `jr` itself under supervision:

```bash
jr --wrap config.yaml
```

If every enabled, nonempty job has `restart: false`, the wrapper runs only one
inner generation, cleans up, and returns its exit status without restarting it.
Otherwise, wrapper recovery remains enabled: a non-restarting job runs once per
inner generation and may run again after a runner failure. Completion is not
persisted across generations.

When the inner `jr` process exits, the wrapper adopts and kills any remaining
previous-generation jobs before waiting 15 seconds and restarting it. Cleanup
failure stops the wrapper instead of allowing old and new generations to
overlap. For `sudo_user` jobs, the wrapper reloads configured usernames and
uses the same bounded noninteractive sudo cleanup path; corresponding sudo
permission must remain available to the wrapper.

Wrapper signal handlers are installed before the inner process starts. The
wrapper forwards termination and `SIGUSR1`/`SIGUSR2` signals, and its restart
backoff is interruptible. Signals are queued only across an in-progress spawn;
user signals received while no inner generation exists are dropped. The wrapper
is a child subreaper, but wrapper mode is not a PID 1 implementation and cannot
recover if the wrapper itself is abruptly killed. `jr` refuses to
start when its own PID is 1. In a container, use an external init:

```bash
tini -- jr config.yaml
```

Each default job shim reaps its command and adopted descendants. The supervisor
retains one Tokio waiter per shim.

## Docker Jobs

Supported contract: attached `docker run` (the default), with container PID 1
handling `SIGTERM`. Docker CLI then proxies the graceful signal, and `--rm`
removes the container after it exits.

Process supervision of the Docker CLI does not provide container ownership:

- Killing or crashing the CLI cannot proxy `SIGKILL` to the container.
- CLI exit, daemon disconnect, or runner force escalation may leave the
  container running.
- `docker run -d` exits immediately and must not be used as a supervised job.
- Reliable forced cleanup requires a job shutdown hook that calls the Docker
  API/CLI by container ID or native Docker lifecycle integration.

## Examples

### Development Server

```yaml
jobs:
  frontend:
    command: "npm run dev"
    cwd: "#cwd#/frontend"
    color: cyan
    env:
      PORT: "3000"

  backend:
    command: "cargo watch -x run"
    cwd: "#cwd#/backend"
    color: yellow
    env:
      DATABASE_URL: "postgres://localhost/myapp"

  tailwind:
    command: "npx tailwindcss -i input.css -o output.css --watch"
    cwd: "#cwd#/frontend"
    color: magenta
```

### Production Services

```yaml
jobs:
  web:
    command: "gunicorn app:app -w 4 -b 0.0.0.0:8000"
    process_group: true
    shutdown_timeout: 30
    env:
      PYTHONUNBUFFERED: "1"

  celery:
    command: "celery -A tasks worker --loglevel=info"
    process_group: true
    shutdown_timeout: 60  # Allow time for tasks to complete

  nginx:
    command: "nginx -g 'daemon off;'"
    sudo_user: root
    shutdown_timeout: 5
```

### Log Rotation with SIGUSR1

Many daemons reopen log files on SIGUSR1. Forward the signal to all jobs:

```bash
# Rotate logs and signal jr
logrotate /etc/logrotate.conf
kill -USR1 $(pidof jr)
```

## License

[GNU General Public License v3.0](LICENSE)
