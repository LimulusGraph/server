# Limulus

Limulus is a local, browser-based editor for Python workflows. Connect cells into
a graph, run independent experiments, and inspect their output in your browser.
The server hosts the editor and runs your Python code on your machine.

Each complete path through a graph is an independent notebook, called a
**universe**. Universes keep separate Python state, even when they share cells.

## Requirements

- Python 3 and any Python packages your workflows use.
- A web browser.

Python is not bundled with Limulus. Check that it is available before starting:

```sh
python3 --version
```

On Windows, use `py -3 --version`. If Python is missing, install Python 3 before
continuing. The compiled app includes both the server and browser editor.
No build tools are required.

## Install and start

Download the compiled archive for your operating system and processor:

| System | Archive |
| --- | --- |
| macOS, Apple silicon | `limulus-darwin-arm64.tar.gz` |
| macOS, Intel | `limulus-darwin-amd64.tar.gz` |
| Linux, x64 | `limulus-linux-amd64.tar.gz` |
| Linux, ARM64 | `limulus-linux-arm64.tar.gz` |
| Windows, x64 | `limulus-windows-amd64.zip` |
| Windows, ARM64 | `limulus-windows-arm64.zip` |

Once you have the archive, extract it and run the executable. For example, on an
Apple silicon Mac:

```sh
tar -xzf limulus-darwin-arm64.tar.gz
cd limulus-darwin-arm64
./limulus
```

On Linux, substitute the matching Linux archive and directory names. On Windows,
run these commands in PowerShell from the directory containing the ZIP file:

```powershell
Expand-Archive .\limulus-windows-amd64.zip -DestinationPath .
Set-Location .\limulus-windows-amd64
$env:LIMULUS_PYTHON = py -3 -c "import sys; print(sys.executable)"
.\limulus.exe
```

Limulus opens **http://127.0.0.1:8080** in your default browser when it is ready.
Keep the terminal open while you work. Press **Ctrl+C** in that terminal to stop
the server and its Python runtimes. If the browser does not open automatically,
open the URL printed in the terminal.

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

In the browser:

1. Choose **New Graph**, then **Save As** and enter the full path to
   `workflow.lmg` inside your project directory.
2. Choose **New Node** to add cells and connect them into a graph.
3. Use **Save As** to save the graph and create its Python scripts in `nodes/`.
4. Edit those scripts in your usual text editor, then choose **Run all universes**.

**Open** accepts the local path to an existing `.lmg` file. Relative paths are
resolved from the directory where you started the server. **Save As** copies the
workflow's scripts alongside the graph when saving to a new location.

To try the included example, open `examples/universes/experiment.lmg` from the
extracted package directory. Choose **Run all universes**: the
two routes produce separate predictions, 6 and 12. Select Evaluate and choose
**Run cell** to increment each universe's evaluation counter without recreating
its model.

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
| `LIMULUS_NO_BROWSER` | Set to `1` to prevent automatic browser opening. |
| `LIMULUS_PYTHON` | Python executable name or full path; defaults to `python3`. A project `.venv` takes precedence. |
| `LIMULUS_HOST` | Bind address; defaults to `127.0.0.1` for local access. |

For example, on macOS/Linux, from the extracted package directory:

```sh
LIMULUS_PORT=8081 ./limulus
LIMULUS_NO_BROWSER=1 ./limulus
LIMULUS_PYTHON=/path/to/python3 ./limulus
```

In Windows PowerShell:

```powershell
$env:LIMULUS_PORT = "8081"
$env:LIMULUS_NO_BROWSER = "1"
.\limulus.exe
```

If the default port is in use, choose another port or set `LIMULUS_PORT=0` and use
the URL printed at startup. If a cell cannot import a package, install it into the
project's `.venv` or the interpreter selected with `LIMULUS_PYTHON`, then reset the
runtimes and run again.

## Development

Contributors need Git, Go 1.26 or later, Node.js 24 with npm, and Python 3.
Clone the server and client side by side, then build and run:

```sh
git clone https://github.com/LimulusGraph/server.git
git clone https://github.com/LimulusGraph/client.git
cd client
npm ci
npm run build
cd ../server
go run .
```

For frontend live reload, keep the server running and run `npm run dev` from
`client` in another terminal. Open **http://localhost:3000**; API requests are
proxied to the server on port 8080.

Run backend tests with `go test ./...` from `server`. To build a standalone binary,
run `npm run build` in `client`, then `go build -o bin/limulus .` in `server`
(use `bin/limulus.exe` on Windows). Rebuild the server after frontend changes to
include the updated editor. Set `LIMULUS_PYTHON` if Python is not available as
`python3`.
