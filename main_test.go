package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"limulus/server/runner"
)

func TestUniverseSSE(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.py"), []byte("print('ERROR is ordinary output')\n"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "graph.lmg")
	graph := `{"nodes":[{"id":"a","data":{"scriptPath":"a.py"}}],"edges":[]}`
	if err := os.WriteFile(path, []byte(graph), 0600); err != nil {
		t.Fatal(err)
	}
	old := universes
	universes = runner.NewUniverseManager()
	defer func() { universes.Close(); universes = old }()
	req := httptest.NewRequest("GET", "/api/node/run?project_path="+url.QueryEscape(path)+"&run_mode=all", nil)
	response := httptest.NewRecorder()
	handleRunNode(response, req)
	if response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	var events []runner.Event
	for _, line := range strings.Split(response.Body.String(), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event runner.Event
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	if len(events) == 0 || events[len(events)-1].Kind != "completed" || !events[len(events)-1].Success {
		t.Fatal(events)
	}
	found := false
	for _, e := range events {
		if e.Kind == "log" {
			found = true
			if e.NodeID != "a" || e.RouteID == "" {
				t.Fatal(e)
			}
		}
	}
	if !found {
		t.Fatal("missing streamed output")
	}
}

func TestExampleUniverses(t *testing.T) {
	path, err := filepath.Abs("examples/universes/experiment.lmg")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	wf, err := decodeWorkflowToDag(data)
	if err != nil {
		t.Fatal(err)
	}
	manager := runner.NewUniverseManager()
	defer manager.Close()
	var mu sync.Mutex
	var output strings.Builder
	emit := func(e runner.Event) { mu.Lock(); defer mu.Unlock(); output.WriteString(e.Text) }
	if err := manager.Run(context.Background(), path, &wf, "", "all", emit); err != nil {
		t.Fatal(err)
	}
	if err := manager.Run(context.Background(), path, &wf, "evaluate", "node_only", emit); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, want := range []string{"small: prediction=6", "large: prediction=12", "Evaluation #2"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in %s", want, text)
		}
	}
}
