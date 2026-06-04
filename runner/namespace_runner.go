package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"limulus/server/dag"
	"limulus/server/namespace"
	"limulus/server/storage"
)

// ============================================================================
// NAMESPACE-AWARE NODE EXECUTION (REDESIGNED FROM PICKLE MODEL)
// ============================================================================

// LogLine represents a single line printed during execution
type LogLine struct {
	Source string // "stdout", "stderr", or "system"
	Text   string
}

// NamespaceRunner executes nodes using shared namespaces instead of pickle files
type NamespaceRunner struct {
	namespaceMgr *namespace.NamespaceManager // Namespace manager for state management
	cacheStorage *storage.CacheStorage       // Storage for persistent namespace data
}

// NewNamespaceRunner creates a new namespace-aware runner instance
func NewNamespaceRunner(
	nsMgr *namespace.NamespaceManager,
	cs *storage.CacheStorage,
) *NamespaceRunner {
	return &NamespaceRunner{
		namespaceMgr: nsMgr,
		cacheStorage: cs,
	}
}

// RunNode executes a single node using shared namespaces for input/output handling
func (r *NamespaceRunner) RunNode(
	ctx context.Context,
	projectDir string,
	wf *dag.Workflow,
	nodeID string,
	logChan chan<- LogLine,
) error {
	defer close(logChan)

	// 1. Find the node
	var targetNode *dag.Node
	for i := range wf.Nodes {
		if wf.Nodes[i].ID == nodeID {
			targetNode = &wf.Nodes[i]
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

	// 3. Get the shared namespace for Jupyter-like data persistence
	sharedNSID := wf.GetSharedNamespace()
	sharedNS, exists := r.namespaceMgr.GetNamespace(sharedNSID)
	if !exists {
		// Create shared namespace if it doesn't exist
		sharedNS, _ = r.namespaceMgr.CreateNamespace(
			sharedNSID,
			wf.Name,
			nil, // No inputs for shared namespace - it's a global state container
			nil, // No input ports
			nil, // Output ports will be added dynamically
		)
	}

	// 4. Load input namespaces and prepare data for Python wrapper
	inputDataMap := make(map[string]interface{}) // portName -> data

	for _, binding := range targetNode.InputBindings {
		ns, exists := r.namespaceMgr.GetNamespace(binding.NamespaceID)
		if !exists {
			// Try to find the input in the shared namespace first (Jupyter-like persistence)
			logChan <- LogLine{
				Source: "system",
				Text: fmt.Sprintf("Input namespace %s not found, checking shared namespace %s for port %s",
					binding.NamespaceID, sharedNSID, binding.PortName),
			}

			if sharedNS.State != nil {
				dataPath := binding.Path
				if dataPath == "" {
					dataPath = fmt.Sprintf("data_%s", binding.PortName)
				}

				if _, ok := sharedNS.State[dataPath]; ok {
					logChan <- LogLine{
						Source: "system",
						Text: fmt.Sprintf("Found input for port %s in shared namespace %s (path: %s)",
							binding.PortName, sharedNSID, dataPath),
					}
				} else {
					return fmt.Errorf("input path %s not found in either namespace %s or shared namespace %s",
						binding.Path, binding.NamespaceID, sharedNSID)
				}
			} else {
				return fmt.Errorf("input namespace %s not found for port %s on node %s",
					binding.NamespaceID, binding.PortName, nodeID)
			}
		} else {
			logChan <- LogLine{
				Source: "system",
				Text: fmt.Sprintf("Loading input from namespace %s for port %s (path: %s)",
					binding.NamespaceID, binding.PortName, binding.Path),
			}

			// Load data from namespace state
			var inputData interface{}
			if ns.State != nil {
				dataPath := binding.Path
				if dataPath == "" {
					dataPath = fmt.Sprintf("data_%s", binding.PortName)
				}

				if val, ok := ns.State[dataPath]; ok {
					inputData = val
				} else {
					// Try shared namespace as fallback
					dataPath := binding.Path
					if dataPath == "" {
						dataPath = fmt.Sprintf("data_%s", binding.PortName)
					}

					if val, ok := sharedNS.State[dataPath]; ok {
						inputData = val
						logChan <- LogLine{
							Source: "system",
							Text: fmt.Sprintf("Input path %s not found in namespace %s, using shared namespace %s (path: %s)",
								binding.Path, binding.NamespaceID, sharedNSID, dataPath),
						}
					} else {
						return fmt.Errorf("input path %s not found in namespace %s state",
							binding.Path, binding.NamespaceID)
					}
				}
			}

			// Store input data keyed by port name for Python wrapper generation
			inputDataMap[binding.PortName] = inputData
		}
	}

	logChan <- LogLine{Source: "system", Text: fmt.Sprintf("All %d input namespaces loaded for node %s",
		len(targetNode.InputBindings), nodeID)}

	// 5. Generate the wrapper Python runner script code with namespace-aware inputs
	scriptFilename := targetNode.ScriptPath
	fullScriptPath := filepath.Join(projectDir, scriptFilename)

	if _, err := os.Stat(fullScriptPath); os.IsNotExist(err) {
		// Create a default empty python script if it does not exist
		defaultCode := fmt.Sprintf("# Python code for node: %s\n# Use inputs['your_port_name'] to access input data\n# Use outputs['your_port_name'] = data to produce output\n\nprint('Hello from %s!')\n", targetNode.Title, targetNode.Title)
		if err := os.WriteFile(fullScriptPath, []byte(defaultCode), 0644); err != nil {
			return fmt.Errorf("failed to generate default script: %v", err)
		}
		logChan <- LogLine{Source: "system", Text: fmt.Sprintf("Created default script: %s", scriptFilename)}
	}

	wrapperFilename := fmt.Sprintf("__runner_%s__.py", nodeID)
	wrapperPath := filepath.Join(cacheDir, wrapperFilename)

	var inputBlock string
	if len(targetNode.InputBindings) > 0 {
		inputLines := make([]string, 0, len(targetNode.InputBindings))
		for _, binding := range targetNode.InputBindings {
			data := inputDataMap[binding.PortName]

			if data != nil {
				inputLines = append(inputLines, fmt.Sprintf("    inputs['%s'] = %s", binding.PortName, r.formatPythonValue(data)))
			} else {
				inputLines = append(inputLines, fmt.Sprintf("    inputs['%s'] = None", binding.PortName))
			}
		}
		inputBlock = strings.Join(inputLines, "\n")
	} else {
		inputBlock = "    pass"
	}

	wrapperCode := r.generateNamespaceAwareWrapper(
		nodeID,
		scriptFilename,
		targetNode.InputBindings,
		targetNode.OutputBindings,
		inputBlock,
	)

	if err := os.WriteFile(wrapperPath, []byte(wrapperCode), 0644); err != nil {
		return fmt.Errorf("failed to write python runner wrapper: %v", err)
	}

	// 6. Build system exec execution
	cmd := exec.Command("python3", filepath.Join(cacheDir, wrapperFilename))
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

	// 7. Store outputs in shared namespace for Jupyter-like persistence
	for _, binding := range targetNode.OutputBindings {
		// Create or get the destination namespace for this output
		nsID := binding.NamespaceID

		var ns *namespace.Namespace
		var err error
		if existingNS, exists := r.namespaceMgr.GetNamespace(nsID); exists {
			ns = existingNS
		} else {
			// Create new namespace for outputs - no input bindings needed
			ns, err = r.namespaceMgr.CreateNamespace(
				fmt.Sprintf("output-%s", nodeID),
				nodeID,
				nil,                        // No inputs for output-only namespace
				nil,                        // No input ports
				[]string{binding.PortName}, // Output ports
			)
			if err != nil {
				return fmt.Errorf("failed to create output namespace: %v", err)
			}
		}

		// Store output in namespace state
		outputPath := binding.Path
		if outputPath == "" {
			outputPath = fmt.Sprintf("output_%s", binding.PortName)
		}

		// Note: In the new model, we need to get actual output data from somewhere.
		// For now, we'll store a placeholder - in production this would come from Python script outputs
		ns.State[outputPath] = map[string]interface{}{
			"port": binding.PortName,
			"node": nodeID,
		}

		// Bind the output port to this namespace
		r.namespaceMgr.BindOutput(nsID, binding.PortName, outputPath)

		logChan <- LogLine{Source: "system", Text: fmt.Sprintf("Stored output for %s in namespace %s (path: %s)",
			binding.PortName, nsID, outputPath)}

		// Also store the same output in the shared namespace for Jupyter-like persistence
		sharedNS.State[fmt.Sprintf("output_%s", binding.PortName)] = map[string]interface{}{
			"port":    binding.PortName,
			"node":    nodeID,
			"version": ns.Version,
		}

		r.namespaceMgr.BindOutput(sharedNSID, fmt.Sprintf("shared_output_%s", binding.PortName), outputPath)
		logChan <- LogLine{Source: "system", Text: fmt.Sprintf("Also stored output for %s in shared namespace %s (path: %s)",
			binding.PortName, sharedNSID, outputPath)}
	}

	return nil
}

// formatPythonValue formats a Go value as Python code string
func (r *NamespaceRunner) formatPythonValue(data interface{}) string {
	switch v := data.(type) {
	case string:
		// Escape quotes for Python strings
		escaped := strings.ReplaceAll(v, "'", "\\'")
		return fmt.Sprintf("'%s'", escaped)
	case float64: // Go uses float64 for all floats
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%.10g", v)
	case bool:
		return fmt.Sprintf("%t", v)
	case nil:
		return "None"
	default:
		// For complex types, try to marshal as JSON and convert to Python-like format
		jsonBytes, _ := json.Marshal(v)
		return string(jsonBytes)
	}
}

// generateNamespaceAwareWrapper generates a Python wrapper script that uses namespaces
func (r *NamespaceRunner) generateNamespaceAwareWrapper(
	nodeID string,
	scriptFilename string,
	inputBindings []dag.InputBinding,
	outputBindings []dag.OutputBinding,
	inputBlock string,
) string {
	// Build output binding code for storing results in namespaces
	var outputBlock string
	if len(outputBindings) > 0 {
		outputLines := make([]string, 0, len(outputBindings))
		for _, binding := range outputBindings {
			path := fmt.Sprintf(".limulus_cache/outputs_%s_%s.json", nodeID, binding.PortName)

			outputLines = append(outputLines, fmt.Sprintf(`    try:
        cache_path = "%s"
        import json
        with open(cache_path, "w") as f:
            json.dump(outputs['%s'], f)
    except Exception as e:
        print(f"Limulus Engine Error: Failed to serialize output '{%s}': {{e}}", file=sys.stderr)
        sys.exit(3)`, path, binding.PortName, binding.PortName))
		}
		outputBlock = strings.Join(outputLines, "\n")
	} else {
		outputBlock = "    pass" // No outputs to store
	}

	return fmt.Sprintf(`import json
import sys
import os

inputs = {}
outputs = {}

# Load predecessor cached inputs from namespaces
try:
%s
except Exception as e:
    print(f"Limulus Engine Error: Failed to load input from namespace: {{e}}", file=sys.stderr)
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
# Serialize and cache execution outputs to namespaces
%s
`, inputBlock, strings.ReplaceAll(scriptFilename, "\\", "\\\\"), outputBlock)
}
