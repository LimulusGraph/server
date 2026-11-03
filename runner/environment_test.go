package runner

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestProjectVirtualEnvironment(t *testing.T) {
	project, wf := fixture(t, map[string]string{"1": `import sys, os
assert sys.prefix != sys.base_prefix
assert sys.prefix == os.path.join(os.getcwd(), '.venv')
assert os.path.samefile(os.environ['VIRTUAL_ENV'], sys.prefix)
assert os.path.samefile(os.environ['PATH'].split(os.pathsep)[0], os.path.join(sys.prefix, 'Scripts' if os.name == 'nt' else 'bin'))
print('project environment active')
`}, nil)
	cmd := exec.Command(testPython(), "-m", "venv", "--without-pip", filepath.Join(filepath.Dir(project), ".venv"))
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create venv: %v: %s", err, output)
	}
	manager := NewUniverseManager()
	defer manager.Close()
	log := &eventLog{}
	if err := manager.Run(context.Background(), project, wf, "", "all", log.emit); err != nil {
		t.Fatalf("%v: %s", err, log.text())
	}
}

func testPython() string {
	if python := os.Getenv("LIMULUS_PYTHON"); python != "" {
		return python
	}
	return "python3"
}
