package main

import (
	"encoding/json"
	"fmt"
	"io"
	"limulus/server/dag"
	"limulus/server/runner"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

// CacheMetadata stores last successful MD5 execution values
type CacheMetadata struct {
	NodeMD5s map[string]string `json:"node_md5s"`
}

func main() {
	port := "8080"
	if envPort := os.Getenv("LIMULUS_PORT"); envPort != "" {
		port = envPort
	}

	http.HandleFunc("/api/project/load", enableCORS(handleLoadProject))
	http.HandleFunc("/api/project/save", enableCORS(handleSaveProject))
	http.HandleFunc("/api/node/read_code", enableCORS(handleReadCode))
	http.HandleFunc("/api/node/save_code", enableCORS(handleSaveCode))
	http.HandleFunc("/api/node/run", enableCORS(handleRunNode))

	fmt.Printf("Limulus Computational Server starting on port %s...\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// CORS & Method Middleware
func enableCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

// 1. Load project .lmg file
func handleLoadProject(w http.ResponseWriter, r *http.Request) {
	filePath := r.URL.Query().Get("path")
	if filePath == "" {
		http.Error(w, "Missing 'path' query parameter", http.StatusBadRequest)
		return
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		// If file doesn't exist, we can return a default blank workflow so the editor opens gracefully
		if os.IsNotExist(err) {
			defaultWF := dag.Workflow{
				Version: "1.0.0",
				Name:    filepath.Base(filePath),
				Nodes:   []dag.Node{},
				Edges:   []dag.Edge{},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(defaultWF)
			return
		}
		http.Error(w, fmt.Sprintf("Failed to read file: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// 2. Save project .lmg file
func handleSaveProject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	filePath := r.URL.Query().Get("path")
	if filePath == "" {
		http.Error(w, "Missing 'path' query parameter", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	// Validate JSON
	var wf dag.Workflow
	if err := json.Unmarshal(body, &wf); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON structure: %v", err), http.StatusBadRequest)
		return
	}

	// Ensure target directories exist
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create parent directories: %v", err), http.StatusInternalServerError)
		return
	}

	// Pretty print save
	prettyJSON, err := json.MarshalIndent(wf, "", "  ")
	if err != nil {
		http.Error(w, "JSON formatting failed", http.StatusInternalServerError)
		return
	}

	if err := os.WriteFile(filePath, prettyJSON, 0644); err != nil {
		http.Error(w, fmt.Sprintf("Failed to write project: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"success"}`))
}

// 3. Read specific script code
func handleReadCode(w http.ResponseWriter, r *http.Request) {
	projectPath := r.URL.Query().Get("project_path")
	scriptPath := r.URL.Query().Get("script_path")

	if projectPath == "" || scriptPath == "" {
		http.Error(w, "Missing 'project_path' or 'script_path'", http.StatusBadRequest)
		return
	}

	projectDir := filepath.Dir(projectPath)
	fullScriptPath := filepath.Join(projectDir, scriptPath)

	codeBytes, err := os.ReadFile(fullScriptPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Return blank
			w.Header().Set("Content-Type", "text/plain")
			w.Write([]byte(""))
			return
		}
		http.Error(w, fmt.Sprintf("Failed to read file: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Write(codeBytes)
}

// 4. Save script code
func handleSaveCode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	projectPath := r.URL.Query().Get("project_path")
	scriptPath := r.URL.Query().Get("script_path")

	if projectPath == "" || scriptPath == "" {
		http.Error(w, "Missing 'project_path' or 'script_path'", http.StatusBadRequest)
		return
	}

	codeBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	projectDir := filepath.Dir(projectPath)
	fullScriptPath := filepath.Join(projectDir, scriptPath)

	// Ensure project dir and file hierarchy exists
	if err := os.MkdirAll(filepath.Dir(fullScriptPath), 0755); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create parent directories: %v", err), http.StatusInternalServerError)
		return
	}

	if err := os.WriteFile(fullScriptPath, codeBytes, 0644); err != nil {
		http.Error(w, fmt.Sprintf("Failed to write file: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"success"}`))
}

// 5. Execute Node with SSE Logs Streaming
func handleRunNode(w http.ResponseWriter, r *http.Request) {
	projectPath := r.URL.Query().Get("project_path")
	nodeID := r.URL.Query().Get("node_id")
	runMode := r.URL.Query().Get("run_mode") // "upstream" or "node_only"

	if projectPath == "" || nodeID == "" {
		http.Error(w, "Missing 'project_path' or 'node_id' query parameters", http.StatusBadRequest)
		return
	}

	// Read workflow to trace DAG
	data, err := os.ReadFile(projectPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to read workflow file: %v", err), http.StatusInternalServerError)
		return
	}

	var wf dag.Workflow
	if err := json.Unmarshal(data, &wf); err != nil {
		http.Error(w, fmt.Sprintf("Invalid workflow JSON: %v", err), http.StatusInternalServerError)
		return
	}

	projectDir := filepath.Dir(projectPath)

	// Set headers for Server-Sent Events (SSE)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Send helper function to stream SSE lines
	streamLog := func(source, text string) {
		payload := map[string]string{
			"source": source,
			"text":   text,
		}
		jsonBytes, _ := json.Marshal(payload)
		fmt.Fprintf(w, "data: %s\n\n", string(jsonBytes))
		flusher.Flush()
	}

	// Determine queue based on runMode
	var queue []string
	if runMode == "node_only" {
		queue = []string{nodeID}
	} else {
		// Default to upstream
		var err error
		queue, err = wf.GetExecutionOrderForNode(nodeID)
		if err != nil {
			streamLog("system", fmt.Sprintf("DAG_ERROR: %v", err))
			return
		}
	}

	// Load Cache Metadata
	cacheDir := filepath.Join(projectDir, ".limulus_cache")
	metadataPath := filepath.Join(cacheDir, "cache_metadata.json")
	var cache CacheMetadata
	cache.NodeMD5s = make(map[string]string)

	if metaBytes, err := os.ReadFile(metadataPath); err == nil {
		_ = json.Unmarshal(metaBytes, &cache)
	}

	reExecutedNodes := make(map[string]bool)

	// Run execution queue
	for i, qNodeID := range queue {
		// Find Node Object in workflow
		var nodeObj *dag.Node
		for nIdx := range wf.Nodes {
			if wf.Nodes[nIdx].ID == qNodeID {
				nodeObj = &wf.Nodes[nIdx]
				break
			}
		}

		if nodeObj == nil {
			streamLog("system", fmt.Sprintf("EXECUTION_ERROR: Node %s not found", qNodeID))
			return
		}

		// 1. Caching Checks:
		// We skip executing this node if:
		// - It is an upstream node (not the target node itself, i.e., index < len(queue) - 1)
		// - Its script exists and matches the cached MD5
		// - No parents were re-executed in this run chain
		// - All outputs pickle files exist on disk
		isTarget := i == len(queue)-1
		skipExecution := false

		scriptPath := filepath.Join(projectDir, nodeObj.ScriptPath)
		currentMD5 := ""
		if _, err := os.Stat(scriptPath); err == nil {
			currentMD5, _ = runner.GetMD5(scriptPath)
		}

		if !isTarget {
			// Check outputs
			outputsExist := true
			for _, edge := range wf.Edges {
				if edge.Disabled || edge.From != qNodeID {
					continue
				}
				pklPath := filepath.Join(cacheDir, fmt.Sprintf("node_%s_%s.pkl", qNodeID, edge.FromPort))
				if _, err := os.Stat(pklPath); os.IsNotExist(err) {
					outputsExist = false
					break
				}
			}

			// Check parents
			parentsReExecuted := false
			for _, edge := range wf.Edges {
				if edge.Disabled || edge.To != qNodeID {
					continue
				}
				if reExecutedNodes[edge.From] {
					parentsReExecuted = true
					break
				}
			}

			cachedMD5 := cache.NodeMD5s[qNodeID]
			if outputsExist && !parentsReExecuted && cachedMD5 != "" && cachedMD5 == currentMD5 {
				skipExecution = true
			}
		}

		if skipExecution {
			streamLog("system", fmt.Sprintf("[SKIPPED] %s: Using cached results.", nodeObj.Title))
			continue
		}

		// 2. Perform Run
		logChan := make(chan runner.LogLine)
		errChan := make(chan error, 1)

		// Start execution in goroutine
		go func() {
			errChan <- runner.RunNode(projectDir, &wf, qNodeID, logChan)
		}()

		// Drain logging channel and push to client
		for line := range logChan {
			streamLog(line.Source, line.Text)
		}

		// Wait for execution completion
		runErr := <-errChan
		if runErr != nil {
			streamLog("system", fmt.Sprintf("EXECUTION_FAILED at node %s: %v", nodeObj.Title, runErr))
			return
		}

		// Mark node as re-executed in this run context
		reExecutedNodes[qNodeID] = true

		// Write new MD5 cache if script exists
		if currentMD5 != "" {
			cache.NodeMD5s[qNodeID] = currentMD5
		}
	}

	// 3. Save cache metadata file
	_ = os.MkdirAll(cacheDir, 0755)
	if prettyBytes, err := json.MarshalIndent(cache, "", "  "); err == nil {
		_ = os.WriteFile(metadataPath, prettyBytes, 0644)
	}

	streamLog("system", "COMPLETED_SUCCESS")
}
