package runner

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"limulus/server/dag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// LogLine represents a single line printed during execution
type LogLine struct {
	Source string // "stdout" or "stderr" or "system"
	Text   string
}

// RunNode executes a single python node, streaming logs in real-time through the returned channel.
func RunNode(projectDir string, w *dag.Workflow, nodeID string, logChan chan<- LogLine) error {
	defer close(logChan)

	// 1. Find the node
	var targetNode *dag.Node
	for i := range w.Nodes {
		if w.Nodes[i].ID == nodeID {
			targetNode = &w.Nodes[i]
			break
		}
	}
	if targetNode == nil {
		return fmt.Errorf("node %s not found in workflow", nodeID)
	}

	logChan <- LogLine{Source: "system", Text: fmt.Sprintf("Starting node execution: %s (%s)", targetNode.Title, targetNode.ScriptPath)}

	// 2. Ensure .limulus_cache exists
	cacheDir := filepath.Join(projectDir, ".limulus_cache")
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return fmt.Errorf("failed to create cache directory: %v", err)
	}

	// 3. Find active incoming edges to load inputs
	var inputLoadLines []string
	for _, edge := range w.Edges {
		if edge.Disabled || edge.To != nodeID {
			continue
		}
		// Expecting parent pickle file name format: node_<parent_id>_<from_port>.pkl
		parentCacheFile := filepath.Join(".limulus_cache", fmt.Sprintf("node_%s_%s.pkl", edge.From, edge.FromPort))
		fullParentPath := filepath.Join(projectDir, parentCacheFile)

		// Verify parent has executed and cache exists
		if _, err := os.Stat(fullParentPath); os.IsNotExist(err) {
			return fmt.Errorf("missing input cache from parent node %s on port %s. Please run parent nodes first", edge.From, edge.FromPort)
		}

		// Generate python loading snippet
		// Safe formatting for python strings
		escapedPath := strings.ReplaceAll(parentCacheFile, "\\", "\\\\")
		inputLoadLines = append(inputLoadLines, fmt.Sprintf("    inputs['%s'] = pickle.load(open('%s', 'rb'))", edge.ToPort, escapedPath))
	}

	// 4. Generate the wrapper Python runner script code
	scriptFilename := targetNode.ScriptPath
	fullScriptPath := filepath.Join(projectDir, scriptFilename)
	if _, err := os.Stat(fullScriptPath); os.IsNotExist(err) {
		// Create a default empty python script if it does not exist
		defaultCode := fmt.Sprintf("# Python code for node: %s\n# Use outputs['your_port_name'] = data to pass to downstream nodes\n\nprint('Hello from %s!')\n", targetNode.Title, targetNode.Title)
		if err := os.WriteFile(fullScriptPath, []byte(defaultCode), 0644); err != nil {
			return fmt.Errorf("failed to generate default script: %v", err)
		}
		logChan <- LogLine{Source: "system", Text: fmt.Sprintf("Created default script: %s", scriptFilename)}
	}

	wrapperFilename := fmt.Sprintf("__runner_%s__.py", nodeID)
	wrapperPath := filepath.Join(cacheDir, wrapperFilename)

	var inputBlock string
	if len(inputLoadLines) > 0 {
		inputBlock = strings.Join(inputLoadLines, "\n")
	} else {
		inputBlock = "    pass"
	}

	wrapperCode := fmt.Sprintf(`import pickle
import sys
import os

inputs = {}
outputs = {}

# Load predecessor cached inputs
try:
%s
except Exception as e:
    print(f"Limulus Engine Error: Failed to load parent input cache: {e}", file=sys.stderr)
    sys.exit(1)

# Ensure project working directory is in python search path
sys.path.insert(0, os.path.abspath('.'))

user_script = "%s"
globals_dict = {
    "__name__": "__main__",
    "__file__": user_script,
    "inputs": inputs,
    "outputs": outputs,
}

# Execute target node code
try:
    with open(user_script, "rb") as f:
        code = compile(f.read(), user_script, 'exec')
        exec(code, globals_dict)
except Exception as e:
    import traceback
    traceback.print_exc()
    sys.exit(2)

# Serialize and cache execution outputs
os.makedirs(".limulus_cache", exist_ok=True)
for port_name, val in outputs.items():
    try:
        cache_path = os.path.join(".limulus_cache", f"node_%s_{port_name}.pkl")
        with open(cache_path, "wb") as f:
            pickle.dump(val, f)
    except Exception as e:
        print(f"Limulus Engine Error: Failed to serialize output '{port_name}': {e}", file=sys.stderr)
        sys.exit(3)
`, inputBlock, strings.ReplaceAll(scriptFilename, "\\", "\\\\"), nodeID)

	if err := os.WriteFile(wrapperPath, []byte(wrapperCode), 0644); err != nil {
		return fmt.Errorf("failed to write python runner wrapper: %v", err)
	}

	// 5. Build system exec execution
	// We run from the project root directory so relative paths in Python align
	cmd := exec.Command("python3", filepath.Join(".limulus_cache", wrapperFilename))
	cmd.Dir = projectDir

	// Capture stdout and stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe failed: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe failed: %v", err)
	}

	// Start execution
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start Python process (ensure python3 is in your PATH): %v", err)
	}

	// Read output concurrently
	stdoutDone := make(chan bool)
	stderrDone := make(chan bool)

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			logChan <- LogLine{Source: "stdout", Text: scanner.Text()}
		}
		stdoutDone <- true
	}()

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			logChan <- LogLine{Source: "stderr", Text: scanner.Text()}
		}
		stderrDone <- true
	}()

	// Wait for readers to finish
	<-stdoutDone
	<-stderrDone

	// Wait for process to exit
	err = cmd.Wait()

	// Clean up the temporary wrapper script
	_ = os.Remove(wrapperPath)

	if err != nil {
		return fmt.Errorf("execution failed: %v", err)
	}

	logChan <- LogLine{Source: "system", Text: "Node executed successfully"}
	return nil
}

// GetMD5 returns the MD5 hash of a file's content
func GetMD5(filepath string) (string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}
