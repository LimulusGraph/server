package runner

import (
	"bufio"
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"limulus/server/dag"
)

//go:embed kernel.py
var kernelSource string

type DisplayOutput struct {
	Kind         string     `json:"kind"`
	Text         string     `json:"text,omitempty"`
	MIME         string     `json:"mime,omitempty"`
	Data         string     `json:"data,omitempty"`
	Caption      string     `json:"caption,omitempty"`
	Columns      []string   `json:"columns,omitempty"`
	Index        []string   `json:"index,omitempty"`
	Rows         [][]string `json:"rows,omitempty"`
	TotalRows    int        `json:"total_rows"`
	TotalColumns int        `json:"total_columns"`
	Truncated    bool       `json:"truncated,omitempty"`
}

type Event struct {
	Line    int            `json:"line,omitempty"`
	Output  *DisplayOutput `json:"output,omitempty"`
	Kind    string         `json:"kind"`
	Source  string         `json:"source,omitempty"`
	Text    string         `json:"text,omitempty"`
	RouteID string         `json:"route_id,omitempty"`
	NodeID  string         `json:"node_id,omitempty"`
	Success bool           `json:"success"`
}

type kernel struct {
	marker    string
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	events    chan Event
	done      chan struct{}
	completed map[string]bool
}

func startKernel(dir string) (*kernel, error) {
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	python := os.Getenv("LIMULUS_PYTHON")
	if python == "" {
		python = "python3"
	}
	cmd := exec.Command(python, "-u", "-c", kernelSource)
	cmd.Dir = dir
	configureKernelProcess(cmd)
	cmd.ExtraFiles = []*os.File{writer}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		reader.Close()
		writer.Close()
		return nil, err
	}
	k := &kernel{marker: fmt.Sprintf("__LIMULUS_BARRIER_%x", nonce), cmd: cmd, events: make(chan Event, 64), done: make(chan struct{}), completed: map[string]bool{}}
	k.stdin, err = cmd.StdinPipe()
	if err != nil {
		reader.Close()
		writer.Close()
		return nil, err
	}
	// Native extension output stays off the JSON protocol. Forward it as logs.
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		reader.Close()
		writer.Close()
		k.stdin.Close()
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		reader.Close()
		writer.Close()
		k.stdin.Close()
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		reader.Close()
		writer.Close()
		k.stdin.Close()
		return nil, err
	}
	writer.Close()
	var readers sync.WaitGroup
	readers.Add(3)
	send := func(e Event) {
		select {
		case k.events <- e:
		case <-k.done:
		}
	}
	for source, pipe := range map[string]io.Reader{"stdout": stdout, "stderr": stderr} {
		go func(source string, pipe io.Reader) {
			defer readers.Done()
			reader := bufio.NewReader(pipe)
			for {
				line, err := reader.ReadString('\n')
				text := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
				if text == k.marker {
					send(Event{Kind: "barrier", Source: source})
				} else if text != "" {
					send(Event{Kind: "log", Source: source, Text: text + "\n"})
				}
				if err != nil {
					return
				}
			}
		}(source, pipe)
	}
	go func() {
		defer readers.Done()
		defer reader.Close()
		decoder := json.NewDecoder(reader)
		for {
			var event Event
			if err := decoder.Decode(&event); err != nil {
				return
			}
			send(event)
		}
	}()
	go func() { readers.Wait(); cmd.Wait(); close(k.events) }()
	return k, nil
}

func (k *kernel) close() {
	close(k.done)
	k.stdin.Close()
	killKernelProcess(k.cmd)
	// Drain until the process and its readers have been reaped.
	for range k.events {
	}
}

func (k *kernel) execute(ctx context.Context, node dag.Node, edge *dag.Edge, emit func(Event)) error {
	request := map[string]string{"path": node.ScriptPath, "node_id": node.ID, "barrier": k.marker}
	if edge != nil {
		request["parent_id"] = edge.From
		request["from_port"] = edge.FromPort
		request["to_port"] = edge.ToPort
	}
	if err := json.NewEncoder(k.stdin).Encode(request); err != nil {
		return err
	}
	done, success := false, false
	barriers := map[string]bool{}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-k.events:
			if !ok {
				return fmt.Errorf("Python runtime exited")
			}
			switch event.Kind {
			case "cell_done":
				done, success = true, event.Success
			case "barrier":
				barriers[event.Source] = true
			default:
				emit(event)
			}
			if done && len(barriers) == 2 {
				if !success {
					return fmt.Errorf("cell failed")
				}
				k.completed[node.ID] = true
				return nil
			}
		}
	}
}

type projectRuntime struct {
	gate    chan struct{}
	kernels map[string]*kernel
}

// UniverseManager owns live runtimes, scoped by project file and complete route.
// Requests for a project are serialized; routes within a request run concurrently.
type UniverseManager struct {
	mu       sync.Mutex
	projects map[string]*projectRuntime
}

func NewUniverseManager() *UniverseManager {
	return &UniverseManager{projects: map[string]*projectRuntime{}}
}
func (m *UniverseManager) project(path string) *projectRuntime {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.projects[path]
	if p == nil {
		p = &projectRuntime{gate: make(chan struct{}, 1), kernels: map[string]*kernel{}}
		m.projects[path] = p
	}
	return p
}

func (m *UniverseManager) Run(ctx context.Context, projectPath string, wf *dag.Workflow, target, mode string, emit func(Event)) error {
	if mode == "" {
		mode = "upstream"
	}
	if mode != "all" && mode != "upstream" && mode != "node_only" && mode != "reset" {
		return fmt.Errorf("unknown run mode %q", mode)
	}
	absolute, err := filepath.Abs(projectPath)
	if err != nil {
		return err
	}
	p := m.project(absolute)
	select {
	case p.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.gate }()
	if mode == "reset" {
		for id, k := range p.kernels {
			k.close()
			delete(p.kernels, id)
		}
		return nil
	}
	routes, err := wf.Routes(128)
	if err != nil {
		return err
	}
	valid := map[string]bool{}
	for _, route := range routes {
		valid[route.ID] = true
	}
	for id, k := range p.kernels {
		if !valid[id] {
			k.close()
			delete(p.kernels, id)
		}
	}
	selected := []dag.Route{}
	for _, route := range routes {
		for _, n := range route.Nodes {
			if mode == "all" || n.ID == target {
				selected = append(selected, route)
				break
			}
		}
	}
	if len(selected) == 0 {
		return fmt.Errorf("no routes match node %q", target)
	}
	// Preflight before starting any route, so missing scripts do not partially run a graph.
	for _, route := range selected {
		for _, n := range route.Nodes {
			if n.ScriptPath == "" {
				return fmt.Errorf("node %s has no script path", n.ID)
			}
			if _, err := os.Stat(filepath.Join(filepath.Dir(absolute), n.ScriptPath)); err != nil {
				return fmt.Errorf("node %s: %w", n.ID, err)
			}
			if mode != "all" && n.ID == target {
				break
			}
		}
		if mode == "node_only" {
			k := p.kernels[route.ID]
			for _, n := range route.Nodes {
				if n.ID == target {
					break
				}
				if k == nil || !k.completed[n.ID] {
					return fmt.Errorf("universe %s needs Run through node before Run cell", route.ID)
				}
			}
		}
	}
	// Mutate the kernel map only on this goroutine.
	for _, route := range selected {
		if mode != "node_only" {
			if k := p.kernels[route.ID]; k != nil {
				k.close()
				delete(p.kernels, route.ID)
			}
		}
		if p.kernels[route.ID] == nil {
			k, err := startKernel(filepath.Dir(absolute))
			if err != nil {
				return err
			}
			p.kernels[route.ID] = k
		}
	}
	var wg sync.WaitGroup
	failures := make(chan string, len(selected))
	invalid := make(chan string, len(selected))
	for _, route := range selected {
		k := p.kernels[route.ID]
		wg.Add(1)
		go func(route dag.Route, k *kernel) {
			defer wg.Done()
			names := []string{}
			for _, node := range route.Nodes {
				name := node.Title
				if name == "" {
					name = node.ID
				}
				names = append(names, name)
			}
			emit(Event{Kind: "route_started", RouteID: route.ID, Text: strings.Join(names, " → ")})
			for i, n := range route.Nodes {
				if mode == "node_only" && n.ID != target {
					continue
				}
				emit(Event{Kind: "node_started", RouteID: route.ID, NodeID: n.ID})
				var edge *dag.Edge
				if i > 0 {
					edge = &route.Edges[i-1]
				}
				err := k.execute(ctx, n, edge, func(e Event) { e.RouteID = route.ID; e.NodeID = n.ID; emit(e) })
				emit(Event{Kind: "node_finished", RouteID: route.ID, NodeID: n.ID, Success: err == nil})
				if err != nil {
					failures <- route.ID
					// A cancelled or failed runtime may contain partial state; discard it.
					invalid <- route.ID
					emit(Event{Kind: "route_finished", RouteID: route.ID, Text: err.Error(), Success: false})
					return
				}
				if mode != "all" && n.ID == target {
					break
				}
			}
			emit(Event{Kind: "route_finished", RouteID: route.ID, Success: true})
		}(route, k)
	}
	wg.Wait()
	close(invalid)
	for id := range invalid {
		p.kernels[id].close()
		delete(p.kernels, id)
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d of %d universes failed", len(failures), len(selected))
	}
	return nil
}

func (m *UniverseManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.projects {
		p.gate <- struct{}{}
		for id, k := range p.kernels {
			k.close()
			delete(p.kernels, id)
		}
		<-p.gate
	}
}
