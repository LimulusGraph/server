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

	// Detect multiversal parents
	var parents []string
	for _, edge := range wf.Edges {
		if !edge.Disabled && edge.To == nodeID {
			parents = append(parents, edge.From)
		}
	}

	logChan <- LogLine{Source: "system", Text: fmt.Sprintf("Starting node execution: %s (%s)", targetNode.Title, targetNode.ScriptPath)}

	// Multiversal execution: run once per parent namespace
	multiversalRuns := 1
	if len(parents) > 1 {
		multiversalRuns = len(parents)
		logChan <- LogLine{Source: "system", Text: fmt.Sprintf("Multiversal execution: node %s has %d parents, running %d times", nodeID, len(parents), multiversalRuns)}
	}

	for runIdx := 0; runIdx < multiversalRuns; runIdx++ {
		err := func() error {
		if multiversalRuns > 1 {
			logChan <- LogLine{Source: "system", Text: fmt.Sprintf("Multiversal run %d/%d for node %s", runIdx+1, multiversalRuns, nodeID)}
		}
		// Multiversal filter: for >1 parents, run within each parent's namespace context
		currentBindings := targetNode.InputBindings
		if multiversalRuns > 1 && len(currentBindings) > 0 {
			currentBindings = []dag.InputBinding{currentBindings[runIdx%len(currentBindings)]}
		}

	// 2. Ensure .limulus_cache exists
	cacheDir := filepath.Join(projectDir, ".limulus_cache")
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return fmt.Errorf("failed to create cache directory: %v", err)
	}

	// 3. Get the shared namespace for Jupyter-like data persistence
	// Shared namespace stays original for input/data loading; outputs isolated per run
	originalSharedNSID := wf.GetSharedNamespace()
	sharedNSID := originalSharedNSID
	sharedNS, exists := r.namespaceMgr.GetNamespace(originalSharedNSID)
	if !exists {
		// Try to load from CacheStorage using original shared namespace
		nsData, err := r.cacheStorage.LoadNamespace(originalSharedNSID)
		if err == nil {
			nsBytes, _ := json.Marshal(nsData)
			var ns namespace.Namespace
			if err := json.Unmarshal(nsBytes, &ns); err == nil {
				r.namespaceMgr.AddNamespace(&ns)
				sharedNS = &ns
				exists = true
			}
		}
	}
	if !exists {
		// Create shared namespace if it doesn't exist
		sharedNS, _ = r.namespaceMgr.CreateNamespace(
			sharedNSID,
			sharedNSID,
			wf.Name,
			nil, // No inputs for shared namespace - it's a global state container
			nil, // No input ports
			nil, // Output ports will be added dynamically
		)
	}
	if sharedNS.State == nil {
		sharedNS.State = make(map[string]interface{})
	}
	if sharedNS.ModuleDependencies == nil {
		sharedNS.ModuleDependencies = make(map[string]bool)
	}
	// Clone so each multiversal run modifies its own copy, not shared state
	clonedShared, err := sharedNS.Clone()
	if err == nil {
		sharedNS = clonedShared
	}

	// Load input namespaces from disk if not already in memory (original kept in manager)
	for _, binding := range currentBindings {
		if _, exists := r.namespaceMgr.GetNamespace(binding.NamespaceID); !exists {
			nsData, err := r.cacheStorage.LoadNamespace(binding.NamespaceID)
			if err == nil {
				nsBytes, _ := json.Marshal(nsData)
				var ns namespace.Namespace
				if err := json.Unmarshal(nsBytes, &ns); err == nil {
					r.namespaceMgr.AddNamespace(&ns)
				}
			}
		}
	}

	// Phase 1: Build unique module dependencies list
	dependencies := make(map[string]bool)
	for _, binding := range currentBindings {
		ns, exists := r.namespaceMgr.GetNamespace(binding.NamespaceID)
		if exists && ns != nil {
			// Clone so dependency tracking doesn't corrupt the manager's namespace
			if clonedNS, cloneErr := ns.Clone(); cloneErr == nil {
				ns = clonedNS
			}
			for dep := range ns.ModuleDependencies {
				dependencies[dep] = true
			}
		}
	}
	for dep := range sharedNS.ModuleDependencies {
		dependencies[dep] = true
	}

	depList := []string{}
	for dep := range dependencies {
		depList = append(depList, dep)
	}

	// Collect symbol context paths for the child node's pre-execution scope.
	// The shared namespace (global accumulated context) is loaded first;
	// binding-specific namespaces are appended after so they override it.
	// Within bindings, declaration order applies — last entry wins (closest ancestor wins).
	symbolContextPaths := []string{}
	seenSymCtx := make(map[string]bool)
	if sharedNS.SymbolContextPath != "" {
		seenSymCtx[sharedNS.SymbolContextPath] = true
		symbolContextPaths = append(symbolContextPaths, sharedNS.SymbolContextPath)
	}
	for _, binding := range currentBindings {
		if ns, exists := r.namespaceMgr.GetNamespace(binding.NamespaceID); exists {
			if ns.SymbolContextPath != "" && !seenSymCtx[ns.SymbolContextPath] {
				seenSymCtx[ns.SymbolContextPath] = true
				symbolContextPaths = append(symbolContextPaths, ns.SymbolContextPath)
			}
		}
	}

	// 4. Load input namespaces and prepare data for Python wrapper
	inputDataMap := make(map[string]interface{}) // portName -> data

	for _, binding := range currentBindings {
		ns, exists := r.namespaceMgr.GetNamespace(binding.NamespaceID)
		if !exists {
			// Try to find the input in the shared namespace first (Jupyter-like persistence)
			logChan <- LogLine{
				Source: "system",
				Text: fmt.Sprintf("Input namespace %s not found, checking shared namespace %s for port %s",
					binding.NamespaceID, sharedNSID, binding.PortName),
			}

			dataPath := binding.Path
			if dataPath == "" {
				dataPath = fmt.Sprintf("data_%s", binding.PortName)
			}

			// Search for input data in sharedNS.State
			var inputData interface{}
			found := false
			for _, k := range []string{
				dataPath,
				fmt.Sprintf("data_%s", binding.PortName),
				fmt.Sprintf("output_%s", binding.PortName),
			} {
				if k != "" {
					if val, ok := sharedNS.State[k]; ok {
						inputData = val
						found = true
						break
					}
				}
			}
			if !found {
				for k, val := range sharedNS.State {
					if strings.HasPrefix(k, "output_") || strings.HasPrefix(k, "data_") {
						inputData = val
						found = true
						break
					}
				}
			}

			if found {
				logChan <- LogLine{
					Source: "system",
					Text: fmt.Sprintf("Found input for port %s in shared namespace %s",
						binding.PortName, sharedNSID),
				}
				inputDataMap[binding.PortName] = inputData
			} else {
				return fmt.Errorf("input namespace %s not found and not in shared namespace %s for port %s",
					binding.NamespaceID, sharedNSID, binding.PortName)
			}
		} else {
			// Clone input namespace so this run doesn't corrupt others
			if clonedNS, cloneErr := ns.Clone(); cloneErr == nil {
				ns = clonedNS
			}
			logChan <- LogLine{
				Source: "system",
				Text: fmt.Sprintf("Loading input from namespace %s for port %s (path: %s)",
					binding.NamespaceID, binding.PortName, binding.Path),
			}

			// Load data from namespace state
			var inputData interface{}
			dataPath := binding.Path
			if dataPath == "" {
				dataPath = fmt.Sprintf("data_%s", binding.PortName)
			}

			found := false
			for _, k := range []string{
				dataPath,
				fmt.Sprintf("data_%s", binding.PortName),
				fmt.Sprintf("output_%s", binding.PortName),
			} {
				if k != "" {
					if val, ok := ns.State[k]; ok {
						inputData = val
						found = true
						break
					}
				}
			}
			if !found && ns.State != nil {
				if len(ns.State) == 1 {
					for _, val := range ns.State {
						inputData = val
						found = true
					}
				} else {
					for k, val := range ns.State {
						if strings.HasPrefix(k, "output_") || strings.HasPrefix(k, "data_") {
							inputData = val
							found = true
							break
						}
					}
				}
			}

			if !found {
				// Try shared namespace as fallback
				for _, k := range []string{
					dataPath,
					fmt.Sprintf("data_%s", binding.PortName),
					fmt.Sprintf("output_%s", binding.PortName),
				} {
					if k != "" {
						if val, ok := sharedNS.State[k]; ok {
							inputData = val
							found = true
							logChan <- LogLine{
								Source: "system",
								Text: fmt.Sprintf("Input path %s not found in namespace %s, using shared namespace %s",
									binding.Path, binding.NamespaceID, sharedNSID),
							}
							break
						}
					}
				}
				if !found {
					for k, val := range sharedNS.State {
						if strings.HasPrefix(k, "output_") || strings.HasPrefix(k, "data_") {
							inputData = val
							found = true
							break
						}
					}
				}
			}

			if found {
				inputDataMap[binding.PortName] = inputData
			} else {
				return fmt.Errorf("input path %s not found in namespace %s or shared namespace %s",
					binding.Path, binding.NamespaceID, sharedNSID)
			}
		}
	}

	logChan <- LogLine{Source: "system", Text: fmt.Sprintf("All %d input namespaces loaded for node %s",
		len(currentBindings), nodeID)}

	executionNodeID := nodeID
	if multiversalRuns > 1 {
		executionNodeID = fmt.Sprintf("%s_multiversal_%d", nodeID, runIdx)
	}

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

	wrapperFilename := fmt.Sprintf("__runner_%s__.py", executionNodeID)
	wrapperPath := filepath.Join(cacheDir, wrapperFilename)

	var inputBlock string
	if len(currentBindings) > 0 {
		inputLines := make([]string, 0, len(currentBindings))
		for _, binding := range currentBindings {
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

	// Build input/output pickle paths
	inputNSPaths := []string{}
	seenNS := make(map[string]bool)
	for _, binding := range currentBindings {
		if !seenNS[binding.NamespaceID] {
			seenNS[binding.NamespaceID] = true
			path := filepath.ToSlash(filepath.Join(".limulus_cache", "namespaces", fmt.Sprintf("%s.pkl", binding.NamespaceID)))
			inputNSPaths = append(inputNSPaths, path)
		}
	}
	sharedNSPath := filepath.ToSlash(filepath.Join(".limulus_cache", "namespaces", fmt.Sprintf("%s.pkl", sharedNSID)))
	inputNSPaths = append(inputNSPaths, sharedNSPath)

	outputNSPaths := []string{}
	seenOutputNS := make(map[string]bool)
	for _, binding := range targetNode.OutputBindings {
		if !seenOutputNS[binding.NamespaceID] {
			seenOutputNS[binding.NamespaceID] = true
			path := filepath.ToSlash(filepath.Join(".limulus_cache", "namespaces", fmt.Sprintf("%s.pkl", binding.NamespaceID)))
			outputNSPaths = append(outputNSPaths, path)
		}
	}
	outputNSPaths = append(outputNSPaths, sharedNSPath)

	wrapperCode := r.generateNamespaceAwareWrapper(
		executionNodeID,
		scriptFilename,
		currentBindings,
		targetNode.OutputBindings,
		inputBlock,
		depList,
		symbolContextPaths,
		inputNSPaths,
		outputNSPaths,
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
		if multiversalRuns > 1 {
			nsID = fmt.Sprintf("%s_multiversal_%d", binding.NamespaceID, runIdx)
		}

		var ns *namespace.Namespace
		if existingNS, exists := r.namespaceMgr.GetNamespace(nsID); exists {
			ns = existingNS
		} else {
			// Try to load from CacheStorage first
			nsData, loadErr := r.cacheStorage.LoadNamespace(nsID)
			if loadErr == nil {
				nsBytes, _ := json.Marshal(nsData)
				var loadedNS namespace.Namespace
				if err := json.Unmarshal(nsBytes, &loadedNS); err == nil {
					r.namespaceMgr.AddNamespace(&loadedNS)
					ns = &loadedNS
				}
			}
			if ns == nil {
				// Create new namespace for outputs - no input bindings needed
				var createErr error
				ns, createErr = r.namespaceMgr.CreateNamespace(
					nsID,
					fmt.Sprintf("output-%s", executionNodeID),
					executionNodeID,
					nil,                        // No inputs for output-only namespace
					nil,                        // No input ports
					[]string{binding.PortName}, // Output ports
				)
				if createErr != nil {
					return fmt.Errorf("failed to create output namespace: %v", createErr)
				}
			}
		}
		if ns.State == nil {
			ns.State = make(map[string]interface{})
		}
		if ns.ModuleDependencies == nil {
			ns.ModuleDependencies = make(map[string]bool)
		}

		// Store output in namespace state
		outputPath := binding.Path
		if outputPath == "" {
			outputPath = fmt.Sprintf("output_%s", binding.PortName)
		}

		// Load actual output from the JSON file written by the Python script
		outputPathFile := filepath.Join(cacheDir, fmt.Sprintf("outputs_%s_%s.json", executionNodeID, binding.PortName))
		var outputVal interface{}
		if fileBytes, err := os.ReadFile(outputPathFile); err == nil {
			if err := json.Unmarshal(fileBytes, &outputVal); err != nil {
				// Fallback to placeholder if JSON unmarshal fails
				outputVal = map[string]interface{}{
					"port":  binding.PortName,
					"node":  nodeID,
					"error": fmt.Sprintf("failed to parse output JSON: %v", err),
				}
			}
			// Clean up the output file
			_ = os.Remove(outputPathFile)
		} else {
			// Fallback to placeholder if file does not exist
			outputVal = map[string]interface{}{
				"port": binding.PortName,
				"node": nodeID,
			}
		}

		ns.State[outputPath] = outputVal

		// Bind the output port to this namespace
		r.namespaceMgr.BindOutput(nsID, binding.PortName, outputPath)

		logChan <- LogLine{Source: "system", Text: fmt.Sprintf("Stored output for %s in namespace %s (path: %s)",
			binding.PortName, nsID, outputPath)}

		// Also store the same output in the shared namespace for Jupyter-like persistence
		sharedNS.State[fmt.Sprintf("output_%s", binding.PortName)] = outputVal

		r.namespaceMgr.BindOutput(sharedNSID, fmt.Sprintf("shared_output_%s", binding.PortName), outputPath)
		logChan <- LogLine{Source: "system", Text: fmt.Sprintf("Also stored output for %s in shared namespace %s (path: %s)",
			binding.PortName, sharedNSID, outputPath)}
	}

	// Read dependencies from metadata file and update namespaces
	metaPath := filepath.Join(cacheDir, fmt.Sprintf("metadata_%s.json", executionNodeID))
	var meta struct {
		Dependencies []string `json:"dependencies"`
	}
	if metaBytes, err := os.ReadFile(metaPath); err == nil {
		if err := json.Unmarshal(metaBytes, &meta); err == nil {
			for _, dep := range meta.Dependencies {
				sharedNS.ModuleDependencies[dep] = true
				for _, binding := range targetNode.OutputBindings {
					if ns, exists := r.namespaceMgr.GetNamespace(binding.NamespaceID); exists {
						ns.ModuleDependencies[dep] = true
					}
				}
			}
		}
		// Clean up metadata file
		_ = os.Remove(metaPath)
	}

	// Propagate the accumulated symbol context path to output namespaces and sharedNS.
	// The Python wrapper writes symctx_<nodeID>.pkl only on successful execution;
	// we stat-check before setting so a failed exec doesn't corrupt downstream paths.
	symCtxRelPath := filepath.ToSlash(filepath.Join(".limulus_cache", fmt.Sprintf("symctx_%s.pkl", executionNodeID)))
	symCtxAbsPath := filepath.Join(cacheDir, fmt.Sprintf("symctx_%s.pkl", executionNodeID))
	if _, statErr := os.Stat(symCtxAbsPath); statErr == nil {
		sharedNS.SymbolContextPath = symCtxRelPath
		for _, binding := range targetNode.OutputBindings {
			if ns, exists := r.namespaceMgr.GetNamespace(binding.NamespaceID); exists {
				ns.SymbolContextPath = symCtxRelPath
			}
		}
		logChan <- LogLine{Source: "system", Text: fmt.Sprintf("Symbol context captured: %s", symCtxRelPath)}
	}

	// Save all updated namespaces back to CacheStorage
	if err := r.cacheStorage.SaveNamespace(sharedNSID, sharedNS); err != nil {
		return fmt.Errorf("failed to save shared namespace: %v", err)
	}
	for _, binding := range targetNode.OutputBindings {
		if ns, exists := r.namespaceMgr.GetNamespace(binding.NamespaceID); exists {
			if err := r.cacheStorage.SaveNamespace(binding.NamespaceID, ns); err != nil {
				return fmt.Errorf("failed to save output namespace %s: %v", binding.NamespaceID, err)
			}
		}
		}
		return nil
	}()
	if err != nil {
		return err
	}
	} // end multiversal loop

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

// generateNamespaceAwareWrapper generates a Python wrapper script that uses the
// 4-phase Symbol/Definition Context execution model:
//
//	1. Dependency injection  — import external modules into child_global
//	2. Symbol context binding — load accumulated ancestor definitions from PKL files
//	   2b. Data namespace loading — load data values from namespace PKL files
//	   2c. Inline input injection — embed port values directly
//	3. User code execution  — exec(user_code, child_global)
//	4. Output serialization + accumulated symbol context capture + namespace save
func (r *NamespaceRunner) generateNamespaceAwareWrapper(
	nodeID string,
	scriptFilename string,
	inputBindings []dag.InputBinding,
	outputBindings []dag.OutputBinding,
	inputBlock string,
	dependencies []string,
	symbolContextPaths []string,
	inputNSPaths []string,
	outputNSPaths []string,
) string {
	// Format dependencies as Python list of strings
	depListStr := ""
	for i, dep := range dependencies {
		if i > 0 {
			depListStr += ", "
		}
		depListStr += fmt.Sprintf("'%s'", strings.ReplaceAll(dep, "'", "\\'"))
	}
	depBlock := fmt.Sprintf("[%s]", depListStr)

	// Format symbol context paths as Python list.
	// Order matters: paths are loaded sequentially; last entry wins (closest ancestor node wins).
	symCtxListStr := ""
	for i, p := range symbolContextPaths {
		if i > 0 {
			symCtxListStr += ", "
		}
		symCtxListStr += fmt.Sprintf("'%s'", strings.ReplaceAll(p, "'", "\\'"))
	}
	symCtxBlock := fmt.Sprintf("[%s]", symCtxListStr)

	// Format input namespace pickle paths
	inputNSPathsStr := ""
	for i, p := range inputNSPaths {
		if i > 0 {
			inputNSPathsStr += ", "
		}
		inputNSPathsStr += fmt.Sprintf("'%s'", strings.ReplaceAll(p, "'", "\\'"))
	}
	inputNSBlock := fmt.Sprintf("[%s]", inputNSPathsStr)

	// Format output namespace pickle paths
	outputNSPathsStr := ""
	for i, p := range outputNSPaths {
		if i > 0 {
			outputNSPathsStr += ", "
		}
		outputNSPathsStr += fmt.Sprintf("'%s'", strings.ReplaceAll(p, "'", "\\'"))
	}
	outputNSBlock := fmt.Sprintf("[%s]", outputNSPathsStr)

	// Build output binding code for storing results in namespaces
	var outputBlock string
	if len(outputBindings) > 0 {
		outputLines := make([]string, 0, len(outputBindings))
		for _, binding := range outputBindings {
			path := fmt.Sprintf(".limulus_cache/outputs_%s_%s.json", nodeID, binding.PortName)

			outputLines = append(outputLines, fmt.Sprintf(
`try:
    _out_path = "%s"
    with open(_out_path, "w") as _f:
        json.dump(outputs.get('%s'), _f)
except Exception as _e:
    print(f"Limulus Engine Error: Failed to serialize output '%s': {_e}", file=sys.stderr)
    sys.exit(3)`, path, binding.PortName, binding.PortName))
		}
		outputBlock = strings.Join(outputLines, "\n")
	} else {
		outputBlock = "# No outputs to serialize"
	}

	return fmt.Sprintf(`import json
import sys
import os
import pickle
import importlib
import types
import inspect
import re

# ── Limulus Engine: names reserved for internal use (never injected into user scope) ──
_LIMULUS_SYSTEM_KEYS = frozenset({
    "__builtins__", "__file__", "__name__", "__doc__", "__package__",
    "__loader__", "__spec__",
    "_LIMULUS_SYSTEM_KEYS", "_LIMULUS_SYM_CTX_PATHS", "_LIMULUS_SYM_CTX_PATH",
    "_LIMULUS_SYM_CTX_OUT", "_LIMULUS_INPUT_NS_PATHS", "_LIMULUS_OUTPUT_NS_PATHS",
    "_LIMULUS_NS_TO_SAVE", "_LIMULUS_USER_SCRIPT", "_dependencies",
    "json", "pickle", "sys", "os", "types", "importlib",
    "inputs", "outputs",
})

inputs = {}
outputs = {}

# ── Phase 1: Dependency injection ─────────────────────────────────────────────────────
_dependencies = %s
child_global = {
    "__name__": "__main__",
    "__file__": "%s",
    "inputs": inputs,
    "outputs": outputs,
}
for _dep in _dependencies:
    try:
        _mod = importlib.import_module(_dep)
        child_global[_dep] = _mod
    except Exception as _e:
        print(f"Limulus Engine Warning: Failed to import dependency '{_dep}': {_e}", file=sys.stderr)

# ── Phase 2a: Symbol context binding (accumulated definitions from ancestors) ─────────
# Paths are loaded in order; later entries win (closest ancestor node wins).
_LIMULUS_SYM_CTX_PATHS = %s
for _sym_path in _LIMULUS_SYM_CTX_PATHS:
    _abs_path = os.path.abspath(_sym_path) if not os.path.isabs(_sym_path) else _sym_path
    if os.path.exists(_abs_path):
        try:
            with open(_abs_path, "rb") as _f:
                _sym_ctx = pickle.load(_f)
            if isinstance(_sym_ctx, dict):
                for _k, _v in _sym_ctx.items():
                    if _k not in _LIMULUS_SYSTEM_KEYS:
                        if isinstance(_v, dict) and _v.get("__limulus_def__"):
                            _src = _v.get("source", "")
                            if _src:
                                try:
                                    if _src.lstrip().startswith("lambda"):
                                        exec(_k + " = " + _src, child_global)
                                    else:
                                        _match = re.search(r'^(?:def|class)\s+([a-zA-Z_][a-zA-Z0-9_]*)', _src, re.MULTILINE)
                                        _orig_name = _match.group(1) if _match else _k
                                        exec(_src, child_global)
                                        # If source defines a different name, bind alias
                                        if _orig_name != _k and _orig_name in child_global and _orig_name not in child_global.get("_reserved_aliases", {}):
                                            child_global[_k] = child_global[_orig_name]
                                except Exception as _e:
                                    print(f"Limulus Engine Error: Failed to restore definition '{_k}': {_e}", file=sys.stderr)
                                    raise
                            else:
                                child_global[_k] = _v
                        else:
                            child_global[_k] = _v
        except Exception as _e:
            print(f"Limulus Engine Warning: Failed to load symbol context from {_abs_path}: {_e}", file=sys.stderr)

# ── Phase 2b: Data namespace loading from pickle files ───────────────────────────────
_LIMULUS_INPUT_NS_PATHS = %s
for _path in _LIMULUS_INPUT_NS_PATHS:
    if os.path.exists(_path):
        try:
            with open(_path, "rb") as _f:
                _ns_data = pickle.load(_f)
            if isinstance(_ns_data, dict):
                for _k, _v in _ns_data.items():
                    if _k not in _LIMULUS_SYSTEM_KEYS:
                        child_global[_k] = _v
        except Exception as _e:
            print(f"Limulus Engine Warning: Failed to load namespace from {_path}: {_e}", file=sys.stderr)

# ── Phase 2c: Inline input data injection ────────────────────────────────────────────
try:
    inputs = child_global.setdefault("inputs", {})
%s
except Exception as _e:
    print(f"Limulus Engine Error: Failed to inject inline inputs: {_e}", file=sys.stderr)
    sys.exit(1)

# Ensure project directory is on Python search path
sys.path.insert(0, os.path.abspath('.'))

# ── Phase 3: Execute user code ───────────────────────────────────────────────────────
_LIMULUS_USER_SCRIPT = "%s"
try:
    with open(_LIMULUS_USER_SCRIPT, "rb") as _f:
        _code = compile(_f.read(), _LIMULUS_USER_SCRIPT, 'exec')
        exec(_code, child_global)
except Exception:
    import traceback
    traceback.print_exc()
    sys.exit(2)

# ── Phase 4a: Output serialization ──────────────────────────────────────────────────
outputs = child_global.get("outputs", {})
%s

# ── Phase 4b: Accumulated symbol context capture ────────────────────────────────────
# Walks child_global and captures all picklable, non-system, non-module entries.
# The result is the *accumulated* context: ancestor definitions + this node's additions.
# Child code wins if it redefines a name that was inherited (standard Python exec semantics).
_LIMULUS_SYM_CTX_OUT = {}
_active_deps = set(_dependencies)

for _k, _v in list(child_global.items()):
    if _k in _LIMULUS_SYSTEM_KEYS:
        continue
    if isinstance(_v, types.ModuleType):
        if not _v.__name__.startswith('_'):
            _active_deps.add(_v.__name__.split('.')[0])
        continue
    if isinstance(_v, (types.FunctionType, types.MethodType, type)):
        try:
            # Monkey-patch __main__ so inspect.getsource finds the user's script file
            import types as _types_mod, sys as _sys_mod
            _old_main = _sys_mod.modules.get('__main__')
            _fake_main = _types_mod.ModuleType('__main__')
            _fake_main.__file__ = _LIMULUS_USER_SCRIPT
            _fake_main.__builtins__ = __builtins__
            _sys_mod.modules['__main__'] = _fake_main
            _src = inspect.getsource(_v)
            _sys_mod.modules['__main__'] = _old_main
            _LIMULUS_SYM_CTX_OUT[_k] = {"__limulus_def__": True, "name": _k, "source": _src}
            if hasattr(_v, '__module__'):
                _mod_name = _v.__module__.split('.')[0]
                if _mod_name not in ('builtins', '__main__'):
                    _active_deps.add(_mod_name)
            continue
        except Exception:
            pass
        # Fallback dependency tracking for non-source-capturable functions/classes
        if hasattr(_v, '__module__'):
            _mod_name = _v.__module__.split('.')[0]
            if _mod_name not in ('builtins', '__main__'):
                _active_deps.add(_mod_name)
        if hasattr(_v, '__class__') and hasattr(_v.__class__, '__module__'):
            _mod_name = _v.__class__.__module__.split('.')[0]
            if _mod_name not in ('builtins', '__main__'):
                _active_deps.add(_mod_name)
        try:
            pickle.dumps(_v)
            _LIMULUS_SYM_CTX_OUT[_k] = _v
        except Exception:
            pass
        continue
    try:
        pickle.dumps(_v)
        _LIMULUS_SYM_CTX_OUT[_k] = _v
    except Exception:
        pass  # Non-picklable values are silently excluded

_LIMULUS_SYM_CTX_PATH = ".limulus_cache/symctx_%s.pkl"
_limulus_sym_dir = os.path.dirname(_LIMULUS_SYM_CTX_PATH)
if _limulus_sym_dir and not os.path.exists(_limulus_sym_dir):
    os.makedirs(_limulus_sym_dir, exist_ok=True)
try:
    with open(_LIMULUS_SYM_CTX_PATH, "wb") as _f:
        pickle.dump(_LIMULUS_SYM_CTX_OUT, _f)
except Exception as _e:
    print(f"Limulus Engine Warning: Failed to save symbol context to {_LIMULUS_SYM_CTX_PATH}: {_e}", file=sys.stderr)

# ── Phase 4c: Data namespace save ───────────────────────────────────────────────────
_LIMULUS_NS_TO_SAVE = {}
for _k, _v in list(child_global.items()):
    if _k in _LIMULUS_SYSTEM_KEYS:
        continue
    if isinstance(_v, types.ModuleType):
        continue
    try:
        pickle.dumps(_v)
        _LIMULUS_NS_TO_SAVE[_k] = _v
    except Exception:
        pass

_LIMULUS_OUTPUT_NS_PATHS = %s
for _path in _LIMULUS_OUTPUT_NS_PATHS:
    _dir_name = os.path.dirname(_path)
    if _dir_name and not os.path.exists(_dir_name):
        os.makedirs(_dir_name, exist_ok=True)
    try:
        with open(_path, "wb") as _f:
            pickle.dump(_LIMULUS_NS_TO_SAVE, _f)
    except Exception as _e:
        print(f"Limulus Engine Warning: Failed to save namespace to {_path}: {_e}", file=sys.stderr)

# ── Phase 4d: Metadata (active module dependencies) ─────────────────────────────────
try:
    _meta_path = ".limulus_cache/metadata_%s.json"
    with open(_meta_path, "w") as _f:
        json.dump({"dependencies": list(_active_deps)}, _f)
except Exception as _e:
    print(f"Limulus Engine Error: Failed to write metadata: {_e}", file=sys.stderr)
`,
		depBlock,
		strings.ReplaceAll(scriptFilename, "\\", "\\\\"),
		symCtxBlock,
		inputNSBlock,
		inputBlock,
		strings.ReplaceAll(scriptFilename, "\\", "\\\\"),
		outputBlock,
		nodeID,
		outputNSBlock,
		nodeID)
}
