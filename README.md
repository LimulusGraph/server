# Limulus runtime

Limulus treats every complete path from a root (no enabled incoming edges) to a
leaf (no enabled outgoing edges) as an independent notebook, or **universe**.
A standalone node is also a universe. A diamond `A → B → D`, `A → C → D`
creates two universes. Each runs its own copy of A and D; D never merges B and C.
Disabled edges do not participate. Cycles and dangling edges are rejected.

## Running

Install Go, Node.js (compatible with Vite 8), npm, and Python 3. Put `python3`
on PATH. From this directory run:

```sh
make run
```

Open **http://127.0.0.1:8080**. The Go server hosts both the editor and its API;
no native app or Rust toolchain is required. `make build` creates `bin/limulus`
with the browser assets embedded, so the binary can run from any directory.
Rebuild after frontend changes. Plain `go run .` uses the last built bundle;
without a bundle, the API still works and the home page explains how to build it.

The server binds to loopback by default. `LIMULUS_PORT` changes the port and
`LIMULUS_HOST` overrides the bind address. Open and Save As ask for local `.lmg`
paths; relative paths are resolved from the server's working directory. Save As
copies the workflow's Python scripts alongside the graph. Edit scripts in your
usual editor, then run them from the browser. Files remain on the local machine.

For frontend development, start `go run .` here and `npm run dev` in `../client`.
Open the Vite URL (port 3000); `/api` is proxied to the Go server. Set
`LIMULUS_API_URL` when starting Vite to use a different backend address.
Servers started with `LIMULUS_TOKEN` retain the extension's bearer authentication
and do not expose the browser editor.

No Python serialization package is required. Python packages used by cells must
be installed in that interpreter's environment. Launch the server with the desired
virtual environment on PATH.

The editor provides:

- **Run all universes**: restart all routes and run every cell, concurrently across routes.
- **Run through node**: restart every complete route containing the selected node,
  then execute each route's prefix through that node.
- **Run cell**: run just the selected cell in each matching live runtime. Earlier
  cells on each route must have succeeded first. Root cells can start fresh runtimes.
- **Reset runtimes**: stop every runtime for the project and release its memory.
- **Stop**: disconnect the current execution; active route processes are terminated.

Each universe owns a persistent Python process with a real `__main__` module.
Cells execute in the same global dictionary, in route order. Functions, closures,
classes, instances, imports, tensors, and mutable values retain their live identity.
As in a notebook, changing a variable affects functions that reference that global.
Rerunning a cell does not automatically rerun later cells or erase its old variables.
Use Run through node or Run all for fresh state after editing upstream code.

Ordinary Python globals are the primary cell interface. For existing port-based
code, `outputs['out']` from the route's predecessor becomes `inputs['in']` in the
next cell (using the edge's port names). Values are live references, not JSON.
Each cell starts with a fresh `outputs` dictionary, and predecessor outputs are
retained separately for cell reruns. Runtime objects never enter the graph file.

## Isolation and lifecycle

Routes run concurrently in separate processes; cells within a route run sequentially.
A route ID is derived from its node IDs, script paths, and edge identities/ports.
Runtimes are scoped by the absolute project filename and route ID. Graph changes
retire obsolete runtimes on the next execution. Requests within one project are
serialized; separate projects can execute concurrently.

A failed cell stops its route and discards that runtime's partial state. Other
routes continue. Runtime state lasts until restart, reset, failure, cancellation,
or server shutdown; it is not persisted to disk. Python source and `.lmg` graph
files remain the durable project representation. Old `.limulus_cache` snapshots
are not read by the new runner.

Process isolation covers Python memory, not external side effects: universes share
the project working directory, filesystem, network, and accelerators. Shared-prefix
cells execute once per universe, so their file writes and other side effects also
repeat. Use distinct output paths when experiments write artifacts. All universes
consume their own memory, including model weights. Graphs exceeding 128 routes
are rejected before starting execution.

The fd-based kernel protocol currently targets macOS/Linux. Child process groups
are terminated on reset/cancel to clean up training subprocesses. Interactive
stdin prompts are unsupported. Python text and native extension output are forwarded on separate pipes; stream
barriers drain native output before each cell completion event.

## Architecture and API

`dag/routes.go` validates and enumerates paths. `runner/universe.go` manages Python
processes, reuse, cancellation, and scheduling. `runner/kernel.py` is embedded in
the Go binary and executes cells without serializing their state. The older
`branch`, `namespace`, `storage`, and `executor` packages remain compatibility code;
the HTTP execution path no longer calls them.

`GET /api/node/run?project_path=...&node_id=...&run_mode=...` streams SSE JSON.
`node_id` is optional for `all` and `reset`. Events have `kind`, `route_id`,
`node_id`, `source`, `text`, and `success` where relevant. Lifecycle events are
`route_started`, `node_started`, `node_finished`, `route_finished`, and `completed`;
output events use `log`. Clients must wait for `completed`, including after one
route fails, and must not infer failures from user output text.

Run backend checks with `go test -race ./...`. Tests cover route enumeration,
live classes/closures/port values, cell reruns, diamond isolation, concurrent
execution, independent failures, and cancellation.

## Example

Open `examples/universes/experiment.lmg` in the editor and choose Run all universes.
The two routes share the setup and evaluation source files, but produce separate
predictions (6 and 12). Select Evaluate and choose Run cell: each universe's
evaluation counter increments without recreating its model or closure.
