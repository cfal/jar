# job-runner-rs

A lightweight asynchronous process supervisor for Linux. It runs multiple jobs,
restarts them after exit, forwards signals, and shuts down their process trees.

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

The binary will be at `target/release/job-runner-rs`.

## Usage

```bash
# Run with a config file
job-runner-rs config.yaml

# Wrapper mode (auto-restarts the entire job-runner on failure)
job-runner-rs --wrap config.yaml

# Show help
job-runner-rs --help
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
| `restart_delay` | integer | 15000 | Delay before restart after exit (ms) |
| `disabled` | boolean | false | Skip this job |
| `process_group` | boolean | true | Signal the job's process group and discovered descendants |
| `shutdown_timeout` | integer | 5 | Seconds to wait for graceful exit |
| `sudo_user` | string | none | Run as this user via sudo |
| `env` | object | none | Additional environment variables |
| `datetime` | object | disabled | Timestamp configuration |

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
| `#runner_cwd#` | Directory where job-runner was started |

Jobs also receive `CWD` and `RUNNER_CWD` environment variables, plus the inherited
`HOME`. The shell expands `$CWD`, `$RUNNER_CWD`, and `$HOME` in commands. An exact
`cwd` value of `$CWD`, `$RUNNER_CWD`, or `$HOME` is also supported.

Commands already run through `sh -c`; do not add another `sh -c` unless nested
shell semantics are intentional. Each completed command is restarted after
`restart_delay`, whether it succeeded or failed. Restarts always wait at least
100 ms to prevent an accidental busy loop.

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

Every job starts in a dedicated noninteractive session via `setsid()`. This
prevents a job using `kill(0, ...)` from signaling job-runner itself.

With the default `process_group: true`:

- Signals target the dedicated process group.
- Linux `/proc` ancestry is also tracked with PID and process start time so
  descendants that create another session can be signaled when discovered.
- Previously observed descendants, including processes that created another
  session, are killed before the job restarts.

Set `process_group: false` only when direct-shell signaling is required. The job
is still session-isolated, but shutdown signals only its shell and descendants
may survive. Jobs using `sudo_user` always use group/tree signaling because the
direct `sudo` client is not the target workload. Validated workload descendants
are signaled as the configured user; if delivery fails and a target remains
alive, supervision fails instead of silently restarting or reporting success.

```yaml
jobs:
  # Process-group signaling is enabled by default; this is explicit for clarity.
  supervisor:
    command: "python manage.py runserver"
    process_group: true
    shutdown_timeout: 10
```

Process groups and `/proc` traversal are not hard containment. A descendant can
escape between scans, PID validation has a check/signal race, and an
uninterruptible kernel task may remain after `SIGKILL`. A numeric process group
can also be reused after its leader exits. Use a delegated cgroup v2 subtree and
`cgroup.kill` when zero surviving descendants or strict isolation is required.

The supervisor exits with an error if it cannot enumerate `/proc` or inspect a
listed process for any reason other than that process disappearing mid-scan;
silently continuing would make escaped-descendant tracking unreliable.

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

## Wrapper Mode

The `--wrap` flag runs job-runner itself under supervision:

```bash
job-runner-rs --wrap config.yaml
```

When the inner job-runner exits, the wrapper waits 15 seconds and restarts it.

Wrapper signal handlers are installed before the inner process starts. The
wrapper forwards termination and `SIGUSR1`/`SIGUSR2` signals, and its restart
backoff is interruptible. Signals are queued only across an in-progress spawn;
user signals received while no inner generation exists are dropped. Wrapper
mode is not a PID 1 implementation and cannot guarantee cleanup of jobs left by
an abruptly killed inner runner. `job-runner-rs` refuses to start when its own
PID is 1 because it does not own a global `waitpid(-1)` reaper. In a container,
use an external init:

```bash
tini -- job-runner-rs config.yaml
```

The supervisor does not act as a child subreaper. Each direct child has one
Tokio task responsible for waiting and reaping it.

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
# Rotate logs and signal job-runner
logrotate /etc/logrotate.conf
kill -USR1 $(pidof job-runner-rs)
```

## License

[GNU General Public License v3.0](LICENSE)
