package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"limulus/server/dag"
	"limulus/server/runner"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

var universes = runner.NewUniverseManager()

func main() {
	defer universes.Close()
	port := "8080"
	if envPort := os.Getenv("LIMULUS_PORT"); envPort != "" {
		port = envPort
	}

	http.HandleFunc("/api/project/load", enableCORS(handleLoadProject))
	http.HandleFunc("/api/project/save", enableCORS(handleSaveProject))
	http.HandleFunc("/api/project/init_temp", enableCORS(handleInitTempProject))
	http.HandleFunc("/api/project/ensure_scripts", enableCORS(handleEnsureScripts))
	http.HandleFunc("/api/project/relocate", enableCORS(handleRelocateProject))
	http.HandleFunc("/api/health", enableCORS(handleHealth))
	http.HandleFunc("/api/node/read_code", enableCORS(handleReadCode))
	http.HandleFunc("/api/node/save_code", enableCORS(handleSaveCode))
	http.HandleFunc("/api/node/run", enableCORS(handleRunNode))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{Addr: net.JoinHostPort(os.Getenv("LIMULUS_HOST"), port), BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
		_ = server.Close()
	}()
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		log.Printf("Server failed: %v", err)
		return
	}
	fmt.Printf("LIMULUS_LISTEN_ADDR=%s\n", listener.Addr())
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Printf("Server failed: %v", err)
	}
}

type RFWorkflow struct {
	NextNodeID int      `json:"nextNodeId,omitempty"`
	Nodes      []RFNode `json:"nodes"`
	Edges      []RFEdge `json:"edges"`
}

type RFNode struct {
	ID       string         `json:"id"`
	Type     string         `json:"type"`
	Position map[string]any `json:"position"`
	Data     RFNodeData     `json:"data"`
}

type RFNodeData struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	ScriptPath  string `json:"scriptPath"`
	Status      string `json:"status"`
}

type RFEdge struct {
	ID     string         `json:"id"`
	Source string         `json:"source"`
	Target string         `json:"target"`
	Data   map[string]any `json:"data"`
	Style  map[string]any `json:"style"`
}

var nonWordRegexp = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

// CORS & Method Middleware
func enableCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// The desktop app keeps its existing local API. Managed extension servers
		// require an unguessable token and do not grant browser CORS access.
		token := os.Getenv("LIMULUS_TOKEN")
		if token != "" {
			expected := "Bearer " + token
			if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte(expected)) != 1 {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		} else {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}
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

	if !json.Valid(body) {
		http.Error(w, "Invalid JSON structure", http.StatusBadRequest)
		return
	}

	// Ensure target directories exist
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create parent directories: %v", err), http.StatusInternalServerError)
		return
	}

	// Pretty print save
	prettyJSON, err := json.MarshalIndent(json.RawMessage(body), "", "  ")
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

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}

func handleInitTempProject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	tmpDir, err := os.MkdirTemp("", "limulus-project-*")
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create temp project: %v", err), http.StatusInternalServerError)
		return
	}
	projectPath := filepath.Join(tmpDir, "workflow.lmg")
	if err := os.WriteFile(projectPath, []byte("{\"nodes\":[],\"edges\":[]}"), 0644); err != nil {
		http.Error(w, fmt.Sprintf("Failed to initialize temp project: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"project_path": projectPath})
}

func handleEnsureScripts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	projectPath := r.URL.Query().Get("project_path")
	if projectPath == "" {
		http.Error(w, "Missing 'project_path' query parameter", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	updated, err := ensureScriptsForWorkflow(projectPath, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(updated)
}

func handleRelocateProject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	fromPath := r.URL.Query().Get("from_path")
	toPath := r.URL.Query().Get("to_path")
	if fromPath == "" || toPath == "" {
		http.Error(w, "Missing 'from_path' or 'to_path' query parameter", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	updatedBody, err := ensureScriptsForWorkflow(fromPath, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fromDir := filepath.Dir(fromPath)
	toDir := filepath.Dir(toPath)
	if err := os.MkdirAll(toDir, 0755); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create destination directory: %v", err), http.StatusInternalServerError)
		return
	}

	var wf RFWorkflow
	if err := json.Unmarshal(updatedBody, &wf); err != nil {
		http.Error(w, fmt.Sprintf("Failed to parse workflow for relocation: %v", err), http.StatusInternalServerError)
		return
	}
	for _, node := range wf.Nodes {
		if node.Data.ScriptPath == "" {
			continue
		}
		src := filepath.Join(fromDir, node.Data.ScriptPath)
		dst := filepath.Join(toDir, node.Data.ScriptPath)
		if _, err := os.Stat(src); err != nil {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
			http.Error(w, fmt.Sprintf("Failed to create destination script directory: %v", err), http.StatusInternalServerError)
			return
		}
		b, err := os.ReadFile(src)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read source script: %v", err), http.StatusInternalServerError)
			return
		}
		if err := os.WriteFile(dst, b, 0644); err != nil {
			http.Error(w, fmt.Sprintf("Failed to write destination script: %v", err), http.StatusInternalServerError)
			return
		}
	}

	if err := os.WriteFile(toPath, updatedBody, 0644); err != nil {
		http.Error(w, fmt.Sprintf("Failed to save relocated project: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(updatedBody)
}

func ensureScriptsForWorkflow(projectPath string, workflowBody []byte) ([]byte, error) {
	projectDir := filepath.Dir(projectPath)
	if err := os.MkdirAll(projectDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create project directory: %v", err)
	}

	var wf RFWorkflow
	if err := json.Unmarshal(workflowBody, &wf); err != nil {
		return nil, fmt.Errorf("failed to parse workflow: %v", err)
	}

	for i := range wf.Nodes {
		scriptPath := strings.TrimSpace(wf.Nodes[i].Data.ScriptPath)
		if scriptPath == "" {
			scriptPath = filepath.ToSlash(filepath.Join("nodes", fmt.Sprintf("%s.py", sanitizeFilename(wf.Nodes[i].ID))))
			wf.Nodes[i].Data.ScriptPath = scriptPath
		}
		fullScriptPath := filepath.Join(projectDir, scriptPath)
		if err := os.MkdirAll(filepath.Dir(fullScriptPath), 0755); err != nil {
			return nil, fmt.Errorf("failed creating script directory: %v", err)
		}
		if _, err := os.Stat(fullScriptPath); os.IsNotExist(err) {
			defaultCode := ""
			if err := os.WriteFile(fullScriptPath, []byte(defaultCode), 0644); err != nil {
				return nil, fmt.Errorf("failed to create script file: %v", err)
			}
		}
	}

	out, err := json.Marshal(wf)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize workflow: %v", err)
	}
	return out, nil
}

func sanitizeFilename(value string) string {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return "node"
	}
	cleaned := nonWordRegexp.ReplaceAllString(trimmed, "_")
	cleaned = strings.Trim(cleaned, "_")
	if cleaned == "" {
		return "node"
	}
	return cleaned
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

// Execute route-local kernels and stream structured, universe-tagged events.
func handleRunNode(w http.ResponseWriter, r *http.Request) {
	projectPath := r.URL.Query().Get("project_path")
	nodeID := r.URL.Query().Get("node_id")
	mode := r.URL.Query().Get("run_mode")
	if projectPath == "" {
		http.Error(w, "Missing project_path", http.StatusBadRequest)
		return
	}
	data, err := os.ReadFile(projectPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	wf, err := decodeWorkflowToDag(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	events := make(chan runner.Event, 128)
	go func() {
		defer close(events)
		emit := func(e runner.Event) {
			select {
			case events <- e:
			case <-r.Context().Done():
			}
		}
		err := universes.Run(r.Context(), projectPath, &wf, nodeID, mode, emit)
		text := "COMPLETED_SUCCESS"
		if err != nil {
			text = err.Error()
		}
		emit(runner.Event{Kind: "completed", Source: "system", Text: text, Success: err == nil})
	}()
	for event := range events {
		payload, _ := json.Marshal(event)
		if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
			return
		}
		flusher.Flush()
	}
}

func decodeWorkflowToDag(raw []byte) (dag.Workflow, error) {
	var wf dag.Workflow
	looksLikeDag := bytes.Contains(raw, []byte(`"script_path"`)) || bytes.Contains(raw, []byte(`"from"`))
	if err := json.Unmarshal(raw, &wf); err == nil && len(wf.Nodes) > 0 && looksLikeDag {
		return wf, nil
	}

	var rf RFWorkflow
	if err := json.Unmarshal(raw, &rf); err != nil {
		return dag.Workflow{}, err
	}

	var converted dag.Workflow
	converted.Nodes = make([]dag.Node, 0, len(rf.Nodes))
	converted.Edges = make([]dag.Edge, 0, len(rf.Edges))
	for _, n := range rf.Nodes {
		converted.Nodes = append(converted.Nodes, dag.Node{
			ID:          n.ID,
			Type:        n.Type,
			Title:       n.Data.Title,
			ScriptPath:  n.Data.ScriptPath,
			Description: n.Data.Description,
		})
	}
	for _, e := range rf.Edges {
		disabled := false
		if e.Data != nil {
			if d, ok := e.Data["disabled"].(bool); ok {
				disabled = d
			}
		}
		converted.Edges = append(converted.Edges, dag.Edge{
			ID:       e.ID,
			From:     e.Source,
			To:       e.Target,
			FromPort: "out",
			ToPort:   "in",
			Disabled: disabled || edgeLooksDisabled(e.Style),
		})
	}
	return converted, nil
}

func edgeLooksDisabled(style map[string]any) bool {
	if style == nil {
		return false
	}
	rawStroke, ok := style["stroke"].(string)
	if !ok {
		return false
	}
	return strings.Contains(rawStroke, "0.3")
}
