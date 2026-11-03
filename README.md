# Limulus execution server

The Python execution backend for [Limulus](https://github.com/LimulusGraph).
It validates graph paths, manages persistent Python universes, and streams
execution events to the browser application and VS Code extension.

For the visual editor, use the [combined clients repository](https://github.com/LimulusGraph/clients).
Its standalone web app and VS Code extension include this backend and manage
project-local Python environments.

## Headless backend

This repository builds only the execution server. It never opens a browser,
embeds frontend assets, or serves an editor. The API listens on
**http://127.0.0.1:8080** by default; `/api/health` reports readiness.

GitHub Actions tests the backend on Linux, macOS, and Windows and produces
`limulus-server-<os>-<arch>` archives for amd64 and arm64. Each archive contains
`limulus` (`limulus.exe` on Windows), examples, build metadata, and a SHA-256
checksum. Pushing a `v*` tag publishes these backend artifacts as a GitHub Release.

Standalone installers and VS Code extensions are built, tested, and released
from [LimulusGraph/clients](https://github.com/LimulusGraph/clients). Choose a
client installer for an interactive workspace; a server archive contains only
the execution API.

## Set up a project

Create a directory for your workflow and its Python environment. On macOS/Linux:

```sh
mkdir -p ~/limulus-projects/my-experiment
cd ~/limulus-projects/my-experiment
python3 -m venv .venv
.venv/bin/python -m pip install numpy pandas
```

On Windows, in PowerShell:

```powershell
New-Item -ItemType Directory -Force "$HOME\limulus-projects\my-experiment"
Set-Location "$HOME\limulus-projects\my-experiment"
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install numpy pandas
```

Replace `numpy pandas` with the packages your workflow needs, or omit that command
if you only use the Python standard library. Limulus automatically uses the
`.venv` beside your saved `.lmg` file. You do not need to activate it in a shell.

Use the combined client to create or open a `.lmg` graph in this directory.
The [sample universe project](examples/universes) contains two paths that share
source files while keeping independent models and runtime state.

## Run workflows

- **Run all universes** restarts every route and runs every cell. Routes run
  concurrently.
- **Run through node** restarts each route containing the selected cell and runs
  that route up to and including the cell.
- **Run cell** runs only the selected cell in each matching live runtime. Earlier
  cells on the route must have succeeded first; root cells can start fresh runtimes.
- **Reset runtimes** stops every runtime for the project and releases its memory.
- **Stop** cancels the current execution.

A standalone cell is one universe. A diamond `A → B → D`, `A → C → D` creates two:
each runs its own copy of A and D, and D never merges B and C. Disabled edges do
not participate. Cycles, dangling edges, and graphs with more than 128 routes
are rejected.

Within a universe, cells share ordinary Python variables, functions, classes, and
live objects. Rerunning a cell does not automatically rerun later cells or erase
old variables. Use **Run through node** or **Run all universes** for fresh state
after changing upstream code. Existing port-based workflows can also pass values
through `outputs['out']` and `inputs['in']`.

A failed cell stops its route and discards that runtime's state; other routes
continue. Runtime state is also cleared on restart, reset, cancellation, or server
shutdown. Save graphs and Python scripts to keep your work; live Python state is
not saved to disk.

Universes have separate Python memory but share the project directory and other
machine resources. Shared cells execute once per universe, so file writes also
repeat. Use distinct output paths for separate experiments. Interactive Python
input prompts are not supported.

## Settings

Set environment variables before starting Limulus:

| Variable | Purpose |
| --- | --- |
| `LIMULUS_PORT` | HTTP port; defaults to `8080`. Use `0` to choose an available port. |
| `LIMULUS_PYTHON` | Python executable name or full path; defaults to `python` on Windows and `python3` elsewhere. A project `.venv` takes precedence. |
| `LIMULUS_HOST` | Bind address; defaults to `127.0.0.1` for local access. |

For example, on macOS/Linux, from the extracted package directory:

```sh
LIMULUS_PORT=8081 ./limulus
LIMULUS_PYTHON=/path/to/python3 ./limulus
```

In Windows PowerShell:

```powershell
$env:LIMULUS_PORT = "8081"
.\limulus.exe
```

If the default port is in use, choose another port or set `LIMULUS_PORT=0` and use
the URL printed at startup. If a cell cannot import a package, install it into the
project's `.venv` or the interpreter selected with `LIMULUS_PYTHON`, then reset the
runtimes and run again.

## Development

Backend development requires Go 1.26+ and Python 3. No Node.js, npm, or clients
checkout is needed:

```sh
go test ./...
go build -o bin/limulus .
python scripts/package.py --os linux  # Also accepts darwin or windows
```

The server workflow builds only this repository. UI builds, installer validation,
and client releases belong to the clients workflow, which resolves its backend
revision once and reuses that exact commit across all native build jobs.
