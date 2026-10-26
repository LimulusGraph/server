package runner

import (
	"context"
	"encoding/base64"
	"limulus/server/dag"
	"os/exec"
	"strings"
	"testing"
)

func TestDisplayOutputAndSourceLines(t *testing.T) {
	path, wf := fixture(t, map[string]string{"1": "print('before', end='')\ndisplay({'answer': 42})\nprint('after')\n6 * 7\n"}, nil)
	manager := NewUniverseManager()
	defer manager.Close()
	logs := &eventLog{}
	if err := manager.Run(context.Background(), path, wf, "", "all", logs.emit); err != nil {
		t.Fatal(err, logs.text())
	}
	var outputs []Event
	for _, event := range logs.events {
		if event.Kind == "log" || event.Kind == "display" {
			outputs = append(outputs, event)
		}
	}
	if len(outputs) != 4 {
		t.Fatalf("outputs: %+v", outputs)
	}
	if outputs[0].Text != "before" || outputs[0].Line != 1 || outputs[1].Output.Text != "{'answer': 42}" || outputs[1].Line != 2 || outputs[2].Text != "after\n" || outputs[3].Output.Text != "42" || outputs[3].Line != 4 {
		t.Fatalf("outputs: %+v", outputs)
	}
	for _, event := range outputs {
		if event.NodeID != "1" || event.RouteID == "" {
			t.Fatalf("missing output context: %+v", event)
		}
	}
}

func TestMatplotlibInlinePlots(t *testing.T) {
	if err := exec.Command("python3", "-c", "import matplotlib").Run(); err != nil {
		t.Skip("matplotlib is not installed")
	}
	t.Setenv("MPLCONFIGDIR", t.TempDir())
	path, wf := fixture(t, map[string]string{"1": `import matplotlib.pyplot as plt
plt.plot([1, 2], [3, 4])
plt.show()
assert not plt.get_fignums()
fig, ax = plt.subplots()
ax.plot([1, 2], [4, 3])
display(fig)
`, "2": `assert not plt.get_fignums()
plt.plot([1, 2], [5, 6])
print('automatic figure')
`}, []dag.Edge{link("1", "2")})
	manager := NewUniverseManager()
	defer manager.Close()
	logs := &eventLog{}
	if err := manager.Run(context.Background(), path, wf, "", "all", logs.emit); err != nil {
		t.Fatal(err, logs.text())
	}
	count := 0
	for _, event := range logs.events {
		if event.Output == nil || event.Output.Kind != "image" {
			continue
		}
		count++
		data, err := base64.StdEncoding.DecodeString(event.Output.Data)
		if err != nil || !strings.HasPrefix(string(data), "\x89PNG\r\n\x1a\n") {
			t.Fatal("invalid PNG", err)
		}
	}
	if count != 3 {
		t.Fatalf("want 3 plots, got %d: %s", count, logs.text())
	}
}

func TestPandasInlineTables(t *testing.T) {
	if err := exec.Command("python3", "-c", "import pandas").Run(); err != nil {
		t.Skip("pandas is not installed")
	}
	path, wf := fixture(t, map[string]string{"1": `import pandas as pd
display(pd.Series(['<script>', None], name='values'))
pd.DataFrame({str(i): range(150) for i in range(35)})
`}, nil)
	manager := NewUniverseManager()
	defer manager.Close()
	logs := &eventLog{}
	if err := manager.Run(context.Background(), path, wf, "", "all", logs.emit); err != nil {
		t.Fatal(err, logs.text())
	}
	var tables []*DisplayOutput
	for _, event := range logs.events {
		if event.Output != nil && event.Output.Kind == "table" {
			tables = append(tables, event.Output)
		}
	}
	if len(tables) != 2 || tables[0].Rows[0][0] != "<script>" || tables[1].TotalRows != 150 || tables[1].TotalColumns != 35 || !tables[1].Truncated || len(tables[1].Rows) != 100 || len(tables[1].Columns) != 30 {
		t.Fatalf("unexpected tables: %+v", tables)
	}
}

func TestOutputSurvivesExceptionsAndMarkdownIsSilent(t *testing.T) {
	path, wf := fixture(t, map[string]string{"1": `from __future__ import annotations
print('before failure')
display(123)
raise ValueError('failure after output')
`}, nil)
	manager := NewUniverseManager()
	defer manager.Close()
	logs := &eventLog{}
	if err := manager.Run(context.Background(), path, wf, "", "all", logs.emit); err == nil {
		t.Fatal("expected node failure")
	}
	if !strings.Contains(logs.text(), "before failure") || !strings.Contains(logs.text(), "ValueError: failure after output") {
		t.Fatal(logs.text())
	}
	found := false
	for _, event := range logs.events {
		if event.Output != nil && event.Output.Text == "123" {
			found = true
		}
	}
	if !found {
		t.Fatal("display output lost on error")
	}
	path, wf = fixture(t, map[string]string{"1": "value = 1\n\"\"\"md\n# Description\n\"\"\"\n"}, nil)
	logs = &eventLog{}
	if err := manager.Run(context.Background(), path, wf, "", "all", logs.emit); err != nil {
		t.Fatal(err)
	}
	for _, event := range logs.events {
		if event.Output != nil {
			t.Fatalf("Markdown displayed as execution output: %+v", event)
		}
	}
}
