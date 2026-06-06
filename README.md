# job-runner-rs

A lightweight, async process supervisor written in Rust. Manages multiple background jobs with automatic restarts, graceful shutdown, signal forwarding, and colored output.

## Features

- **Multi-job execution** - Run multiple processes concurrently from a single config file
- **Auto-restart** - Automatically restart failed jobs with configurable delays
- **Graceful shutdown** - SIGTERM with configurable timeout, then SIGKILL
- **Signal forwarding** - Forward SIGUSR1/SIGUSR2 to all managed processes
- **Process groups** - Optionally run jobs in their own session (setsid) to kill all children together
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
    process_group: true            # Run in own session (kills children on exit)
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

JSON config files support C++ style comments:

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
| `command` | string | *required* | Shell command to execute |
| `cwd` | string | current dir | Working directory |
| `color` | string | random | Output color (see colors below) |
| `delay` | integer | 0 | Initial startup delay in milliseconds |
| `restart_delay` | integer | 15000 | Delay before restart after exit (ms) |
| `disabled` | boolean | false | Skip this job |
| `process_group` | boolean | false | Run in own session (setsid) |
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

Use these placeholders in `command` and `cwd`:

| Placeholder | Expands To |
|-------------|------------|
| `#cwd#` | Directory containing the config file |
| `#home#` | User's `$HOME` directory |
| `#runner_cwd#` | Directory where job-runner was started |

## Signal Handling

| Signal | Behavior |
|--------|----------|
| `SIGTERM`, `SIGINT`, `SIGHUP`, `SIGQUIT` | Initiate graceful shutdown |
| `SIGUSR1`, `SIGUSR2` | Forward to all managed processes |

### Graceful Shutdown

When a termination signal is received:

1. Send `SIGTERM` to all managed processes
2. Wait up to `shutdown_timeout` seconds per job
3. Send `SIGKILL` to processes that haven't exited
4. Exit

### Process Groups

When `process_group: true`:

- The job runs in its own session via `setsid()`
- All child processes inherit the process group
- On shutdown, the entire process group is killed together
- Useful for jobs that spawn child processes

```yaml
jobs:
  # This job spawns workers - process_group ensures they're all killed
  supervisor:
    command: "python manage.py runserver"
    process_group: true
    shutdown_timeout: 10
```

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

## Wrapper Mode

The `--wrap` flag runs job-runner itself under supervision:

```bash
job-runner-rs --wrap config.yaml
```

If the inner job-runner exits (crash, config error, etc.), the wrapper waits 15 seconds and restarts it. Useful for production deployments where you want the supervisor to be resilient.

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

## Building from Source

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# Run tests
cargo test
```

## License

MIT
