package runner

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"limulus/server/dag"
)

type eventLog struct {
	mu     sync.Mutex
	events []Event
}

func (l *eventLog) emit(e Event) { l.mu.Lock(); defer l.mu.Unlock(); l.events = append(l.events, e) }
func (l *eventLog) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var s string
	for _, e := range l.events {
		s += e.Text
	}
	return s
}
func fixture(t *testing.T, scripts map[string]string, edges []dag.Edge) (string, *dag.Workflow) {
	t.Helper()
	dir := t.TempDir()
	wf := &dag.Workflow{}
	for id, code := range scripts {
		path := id + ".py"
		if err := os.WriteFile(filepath.Join(dir, path), []byte(code), 0600); err != nil {
			t.Fatal(err)
		}
		wf.Nodes = append(wf.Nodes, dag.Node{ID: id, ScriptPath: path})
	}
	wf.Edges = edges
	return filepath.Join(dir, "graph.lmg"), wf
}
func link(from, to string) dag.Edge {
	return dag.Edge{ID: from + to, From: from, To: to, FromPort: "out", ToPort: "in"}
}

func TestLiveObjectsAndCellRerun(t *testing.T) {
	path, wf := fixture(t, map[string]string{
		"a": `import math as m
from dataclasses import dataclass
@dataclass
class Model:
    scale: int
    def predict(self, value): return self.scale * m.sqrt(value)
def factory(offset):
    return lambda value: value + offset
f = factory(7)
model = Model(2)
items = []
outputs['out'] = model
`,
		"b": `assert inputs['in'] is model
items.append(f(model.predict(9)))
outputs['out'] = f
`,
		"c": `assert inputs['in'] is f
assert items == [13]
assert isinstance(model, Model)
counter = globals().get('counter', 0) + 1
print('counter', counter)
`,
	}, []dag.Edge{link("a", "b"), link("b", "c")})
	m := NewUniverseManager()
	defer m.Close()
	logs := &eventLog{}
	if err := m.Run(context.Background(), path, wf, "", "all", logs.emit); err != nil {
		t.Fatalf("%v: %s", err, logs.text())
	}
	if err := m.Run(context.Background(), path, wf, "c", "node_only", logs.emit); err != nil {
		t.Fatalf("%v: %s", err, logs.text())
	}
	if !strings.Contains(logs.text(), "counter 2") {
		t.Fatal(logs.text())
	}
	// Restarting replays the route in a fresh namespace.
	if err := m.Run(context.Background(), path, wf, "c", "upstream", logs.emit); err != nil {
		t.Fatal(err)
	}
	if err := m.Run(context.Background(), path, wf, "", "reset", logs.emit); err != nil {
		t.Fatal(err)
	}
	if err := m.Run(context.Background(), path, wf, "c", "node_only", logs.emit); err == nil {
		t.Fatal("expected uninitialized runtime error")
	}
}

func TestDiamondIsolationAndConcurrency(t *testing.T) {
	path, wf := fixture(t, map[string]string{
		"root": "items = []\n",
		"left": `import os, time
items.append('left')
open('left.ready', 'w').close()
deadline = time.monotonic() + 5
while not os.path.exists('right.ready'):
    assert time.monotonic() < deadline, 'routes did not run concurrently'
    time.sleep(.01)
`,
		"right": `import os, time
items.append('right')
open('right.ready', 'w').close()
deadline = time.monotonic() + 5
while not os.path.exists('left.ready'):
    assert time.monotonic() < deadline, 'routes did not run concurrently'
    time.sleep(.01)
`,
		"end": "assert len(items) == 1\nprint(items[0])\n",
	}, []dag.Edge{link("root", "left"), link("root", "right"), link("left", "end"), link("right", "end")})
	m := NewUniverseManager()
	defer m.Close()
	logs := &eventLog{}
	if err := m.Run(context.Background(), path, wf, "", "all", logs.emit); err != nil {
		t.Fatalf("%v: %s", err, logs.text())
	}
	routes := map[string]bool{}
	for _, e := range logs.events {
		if e.Kind == "node_finished" && e.NodeID == "end" && e.Success {
			routes[e.RouteID] = true
		}
	}
	if len(routes) != 2 {
		t.Fatalf("expected two distinct universes: %v", routes)
	}
}

func TestFailureDoesNotCancelOtherUniverse(t *testing.T) {
	path, wf := fixture(t, map[string]string{"bad": "raise ValueError('bad cell')\n", "good": "print('still ran')\n"}, nil)
	m := NewUniverseManager()
	defer m.Close()
	logs := &eventLog{}
	if err := m.Run(context.Background(), path, wf, "", "all", logs.emit); err == nil {
		t.Fatal("expected failure")
	}
	if !strings.Contains(logs.text(), "still ran") {
		t.Fatal(logs.text())
	}
}

func TestCancellationKillsRuntime(t *testing.T) {
	path, wf := fixture(t, map[string]string{"a": "import subprocess, sys\nworker = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\nwhile True: pass\n"}, nil)
	m := NewUniverseManager()
	defer m.Close()
	logs := &eventLog{}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := m.Run(ctx, path, wf, "", "all", logs.emit); err == nil {
		t.Fatal("expected cancellation")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("cancellation did not stop runtime")
	}
}

func TestNativeOutputAndProjectIsolation(t *testing.T) {
	path, wf := fixture(t, map[string]string{"a": `import os
os.write(1, b'native stdout\n')
os.write(2, b'native stderr\n')
count = globals().get('count', 0) + 1
print(count)
`}, nil)
	m := NewUniverseManager()
	defer m.Close()
	logs := &eventLog{}
	if err := m.Run(context.Background(), path, wf, "", "all", logs.emit); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.text(), "native stdout") || !strings.Contains(logs.text(), "native stderr") {
		t.Fatal(logs.text())
	}
	other := &eventLog{}
	if err := m.Run(context.Background(), filepath.Join(filepath.Dir(path), "other.lmg"), wf, "a", "node_only", other.emit); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(other.text(), "2\n") {
		t.Fatal("runtime leaked between projects")
	}
	// Reset must work even if the edited graph is temporarily invalid.
	wf.Edges = append(wf.Edges, dag.Edge{From: "a", To: "a"})
	if err := m.Run(context.Background(), path, wf, "", "reset", logs.emit); err != nil {
		t.Fatal(err)
	}
}

func TestGraphChangesRetireRuntimes(t *testing.T) {
	path, wf := fixture(t, map[string]string{"a": "x = 1\n", "b": "assert x == 1\n"}, []dag.Edge{link("a", "b")})
	m := NewUniverseManager()
	defer m.Close()
	logs := &eventLog{}
	if err := m.Run(context.Background(), path, wf, "", "all", logs.emit); err != nil {
		t.Fatal(err)
	}
	p := m.project(path)
	var old *kernel
	for _, k := range p.kernels {
		old = k
	}
	wf.Edges = nil
	if err := m.Run(context.Background(), path, wf, "a", "node_only", logs.emit); err != nil {
		t.Fatal(err)
	}
	select {
	case <-old.done:
	default:
		t.Fatal("obsolete kernel was not stopped")
	}
	if len(p.kernels) != 1 {
		t.Fatal("expected only the new root runtime")
	}
}
