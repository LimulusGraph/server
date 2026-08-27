package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ============================================================================
// PERSISTENCE LAYER - SHARED NAMESPACE STORAGE WITH BRANCH TRACKING
// ============================================================================

// CacheStorage manages persistent storage for shared namespaces and branch state
type CacheStorage struct {
	basePath     string       // Base path for all cache files
	namespaceDir string       // Directory for namespace data
	branchDir    string       // Directory for branch state
	mu           sync.RWMutex // Mutex for concurrent access
}

// NewCacheStorage creates a new cache storage instance
func NewCacheStorage(basePath string) *CacheStorage {
	return &CacheStorage{
		basePath:     basePath,
		namespaceDir: filepath.Join(basePath, "namespaces"),
		branchDir:    filepath.Join(basePath, "branches"),
	}
}

// EnsureDirectories creates all necessary directories for storage
func (cs *CacheStorage) EnsureDirectories() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, dir := range []string{cs.namespaceDir, cs.branchDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return nil
}

// ============================================================================
// NAMESPACE PERSISTENCE
// ============================================================================

// SaveNamespace persists a namespace to disk (including SymbolContext if present)
func (cs *CacheStorage) SaveNamespace(namespaceID string, ns interface{}) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	namespacePath := filepath.Join(cs.namespaceDir, fmt.Sprintf("%s.json", namespaceID))
	data, err := json.MarshalIndent(ns, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal namespace: %w", err)
	}

	return os.WriteFile(namespacePath, data, 0644)
}

// SaveSymbolContext persists a SymbolContext separately from namespace state
func (cs *CacheStorage) SaveSymbolContext(ctxID string, symCtx *SymbolContext) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	symCtxPath := filepath.Join(cs.namespaceDir, fmt.Sprintf("%s_symctx.json", ctxID))
	data, err := json.MarshalIndent(symCtx, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal symbol context: %w", err)
	}

	return os.WriteFile(symCtxPath, data, 0644)
}

// LoadSymbolContext loads a SymbolContext from disk (separate from namespace state)
func (cs *CacheStorage) LoadSymbolContext(ctxID string) (*SymbolContext, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	symCtxPath := filepath.Join(cs.namespaceDir, fmt.Sprintf("%s_symctx.json", ctxID))
	data, err := os.ReadFile(symCtxPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read symbol context %s: %w", ctxID, err)
	}

	var symCtx SymbolContext
	if err := json.Unmarshal(data, &symCtx); err != nil {
		return nil, fmt.Errorf("failed to unmarshal symbol context: %w", err)
	}

	return &symCtx, nil
}

// LoadNamespace loads a namespace from disk
func (cs *CacheStorage) LoadNamespace(namespaceID string) (interface{}, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	namespacePath := filepath.Join(cs.namespaceDir, fmt.Sprintf("%s.json", namespaceID))
	data, err := os.ReadFile(namespacePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read namespace %s: %w", namespaceID, err)
	}

	var ns interface{}
	if err := json.Unmarshal(data, &ns); err != nil {
		return nil, fmt.Errorf("failed to unmarshal namespace: %w", err)
	}

	return ns, nil
}

// DeleteNamespace removes a namespace from disk
func (cs *CacheStorage) DeleteNamespace(namespaceID string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	namespacePath := filepath.Join(cs.namespaceDir, fmt.Sprintf("%s.json", namespaceID))
	if err := os.Remove(namespacePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove namespace %s: %w", namespaceID, err)
	}

	return nil
}

// ListNamespaces returns all namespace IDs in storage
func (cs *CacheStorage) ListNamespaces() ([]string, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	var ids []string
	files, err := os.ReadDir(cs.namespaceDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces: %w", err)
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			id := strings.TrimSuffix(filepath.Base(file.Name()), ".json")
			ids = append(ids, id)
		}
	}

	return ids, nil
}

// ============================================================================
// BRANCH STATE PERSISTENCE
// ============================================================================

// BranchState represents the persistent state of a branch
type BranchState struct {
	ID        string                 `json:"id"`              // Branch identifier
	CreatedAt time.Time              `json:"created_at"`      // Creation timestamp
	UpdatedAt time.Time              `json:"updated_at"`      // Last modification timestamp
	Version   int64                  `json:"version"`         // Monotonic version counter
	State     map[string]interface{} `json:"state,omitempty"` // Shared state dictionary
	Metadata  BranchStateMetadata    `json:"metadata"`        // Metadata about this branch
}

// BranchStateMetadata contains structural information about a branch's state
type BranchStateMetadata struct {
	WorkflowID  string                 `json:"workflow_id"`         // Parent workflow identifier
	NodeCount   int                    `json:"node_count"`          // Number of nodes in this branch
	ActiveNodes []string               `json:"active_nodes"`        // Nodes currently active in execution
	Completed   map[string]bool        `json:"completed,omitempty"` // Map of completed node IDs
	Failed      map[string]string      `json:"failed,omitempty"`    // Map of failed node IDs to error messages
	InputPorts  []string               `json:"input_ports"`         // Input ports bound to this branch
	OutputPorts []string               `json:"output_ports"`        // Output ports exposed by this branch
	Inputs      map[string]interface{} `json:"inputs,omitempty"`    // Direct input bindings (if any)
	Outputs     map[string]interface{} `json:"outputs,omitempty"`   // Direct outputs (if any)
}

// SaveBranchState persists a branch's state to disk
func (cs *CacheStorage) SaveBranchState(branchID string, state BranchState) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	branchPath := filepath.Join(cs.branchDir, fmt.Sprintf("%s.json", branchID))
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal branch state: %w", err)
	}

	return os.WriteFile(branchPath, data, 0644)
}

// LoadBranchState loads a branch's state from disk
func (cs *CacheStorage) LoadBranchState(branchID string) (*BranchState, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	branchPath := filepath.Join(cs.branchDir, fmt.Sprintf("%s.json", branchID))
	data, err := os.ReadFile(branchPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read branch state %s: %w", branchID, err)
	}

	var state BranchState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal branch state: %w", err)
	}

	return &state, nil
}

// DeleteBranchState removes a branch's state from disk
func (cs *CacheStorage) DeleteBranchState(branchID string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	branchPath := filepath.Join(cs.branchDir, fmt.Sprintf("%s.json", branchID))
	if err := os.Remove(branchPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove branch state %s: %w", branchID, err)
	}

	return nil
}

// ListBranches returns all branch IDs in storage
func (cs *CacheStorage) ListBranches() ([]string, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	var ids []string
	files, err := os.ReadDir(cs.branchDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list branches: %w", err)
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			id := strings.TrimSuffix(filepath.Base(file.Name()), ".json")
			ids = append(ids, id)
		}
	}

	return ids, nil
}

// ============================================================================
// OUTPUT VERSIONING STRATEGY
// ============================================================================

// OutputVersion represents a versioned output from a node
type OutputVersion struct {
	NodeID    string            `json:"node_id"`            // Node that produced this output
	PortName  string            `json:"port_name"`          // Port name of the output
	Data      interface{}       `json:"data,omitempty"`     // The actual data (JSON-serializable)
	Version   int64             `json:"version"`            // Output version for conflict resolution
	CreatedAt time.Time         `json:"created_at"`         // Creation timestamp
	Metadata  map[string]string `json:"metadata,omitempty"` // Additional metadata about the output
}

// SaveOutputVersion persists a versioned output to disk
func (cs *CacheStorage) SaveOutputVersion(branchID string, nodeID string, portName string, output OutputVersion) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Create branch-specific outputs directory
	outputsDir := filepath.Join(cs.branchDir, branchID, "outputs")
	if err := os.MkdirAll(outputsDir, 0755); err != nil {
		return fmt.Errorf("failed to create outputs directory: %w", err)
	}

	// Create versioned output file path
	outputPath := filepath.Join(outputsDir, fmt.Sprintf("%s_%s_v%d.json", nodeID, portName, output.Version))
	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal output: %w", err)
	}

	return os.WriteFile(outputPath, data, 0644)
}

// LoadOutputVersion loads a versioned output from disk (loads latest version)
func (cs *CacheStorage) LoadOutputVersion(branchID string, nodeID string, portName string) (*OutputVersion, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	// Create branch-specific outputs directory
	outputsDir := filepath.Join(cs.branchDir, branchID, "outputs")

	// Try to load the latest version first (start from high and work down)
	version := int64(100) // Start with a reasonable upper bound
	for version >= 1 {
		outputPath := filepath.Join(outputsDir, fmt.Sprintf("%s_%s_v%d.json", nodeID, portName, version))
		data, err := os.ReadFile(outputPath)
		if err == nil {
			var output OutputVersion
			if unmarshalErr := json.Unmarshal(data, &output); unmarshalErr != nil {
				return nil, fmt.Errorf("failed to unmarshal output: %w", unmarshalErr)
			}
			return &output, nil
		}

		version--

		// Stop if we've gone below 1
		if version < 1 {
			return nil, fmt.Errorf("no output found for (%s, %s) in branch %s", nodeID, portName, branchID)
		}
	}

	return nil, fmt.Errorf("no output found for (%s, %s) in branch %s", nodeID, portName, branchID)
}

// DeleteOutputVersion removes a specific version of an output from disk
func (cs *CacheStorage) DeleteOutputVersion(branchID string, nodeID string, portName string, version int64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	outputsDir := filepath.Join(cs.branchDir, branchID, "outputs")
	outputPath := filepath.Join(outputsDir, fmt.Sprintf("%s_%s_v%d.json", nodeID, portName, version))

	if err := os.Remove(outputPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove output version: %w", err)
	}

	return nil
}

// ListOutputVersions returns all versions of an output in a branch
func (cs *CacheStorage) ListOutputVersions(branchID string, nodeID string, portName string) ([]int64, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	outputsDir := filepath.Join(cs.branchDir, branchID, "outputs")
	var versions []int64

	files, err := os.ReadDir(outputsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list output versions: %w", err)
	}

	for _, file := range files {
		name := file.Name()
		// Extract version from filename format: nodeID_portName_vVERSION.json
		parts := strings.Split(name, "_")
		if len(parts) >= 4 && parts[2] == "v" {
			versionStr := strings.TrimSuffix(strings.Join(parts[3:], ""), ".json")
			if version, err := strconv.ParseInt(versionStr, 10, 64); err == nil {
				versions = append(versions, version)
			}
		}
	}

	return versions, nil
}

// ============================================================================
// NAMESPACE LIFECYCLE MANAGEMENT
// ============================================================================

// CreateNamespaceLifecycle creates a new namespace with proper lifecycle tracking
func (cs *CacheStorage) CreateNamespaceLifecycle(namespaceID string, name string, branchID string, inputPorts []string, outputPorts []string, state map[string]interface{}) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Create the namespace object
	namespace := struct {
		ID        string                 `json:"id"`
		Name      string                 `json:"name,omitempty"`
		CreatedAt time.Time              `json:"created_at"`
		UpdatedAt time.Time              `json:"updated_at"`
		Version   int64                  `json:"version"`
		State     map[string]interface{} `json:"state,omitempty"`
		Metadata  NamespaceMetadata      `json:"metadata"`
	}{
		ID:        namespaceID,
		Name:      name,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Version:   1,
		State:     state,
		Metadata: NamespaceMetadata{
			BranchID:    branchID,
			NodeCount:   0,
			InputPorts:  inputPorts,
			OutputPorts: outputPorts,
			Inputs:      make(map[string]interface{}),
			Outputs:     make(map[string]interface{}),
		},
	}

	return cs.SaveNamespace(namespaceID, namespace)
}

// UpdateNamespaceLifecycle updates an existing namespace's lifecycle state
func (cs *CacheStorage) UpdateNamespaceLifecycle(namespaceID string, state map[string]interface{}, inputPorts []string, outputPorts []string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Load the current namespace
	currentNS, err := cs.LoadNamespace(namespaceID)
	if err != nil {
		return fmt.Errorf("failed to load namespace: %w", err)
	}

	ns, ok := currentNS.(struct {
		ID        string                 `json:"id"`
		Name      string                 `json:"name,omitempty"`
		CreatedAt time.Time              `json:"created_at"`
		UpdatedAt time.Time              `json:"updated_at"`
		Version   int64                  `json:"version"`
		State     map[string]interface{} `json:"state,omitempty"`
		Metadata  NamespaceMetadata      `json:"metadata"`
	})
	if !ok {
		return fmt.Errorf("invalid namespace structure")
	}

	ns.State = state
	ns.UpdatedAt = time.Now()
	ns.Version++
	ns.Metadata.InputPorts = inputPorts
	ns.Metadata.OutputPorts = outputPorts

	return cs.SaveNamespace(namespaceID, ns)
}

// DeleteNamespaceLifecycle removes a namespace and all its data from storage
func (cs *CacheStorage) DeleteNamespaceLifecycle(namespaceID string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Remove the main namespace file
	namespacePath := filepath.Join(cs.namespaceDir, fmt.Sprintf("%s.json", namespaceID))
	if err := os.Remove(namespacePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove namespace %s: %w", namespaceID, err)
	}

	// Remove any associated output files
	outputsDir := filepath.Join(cs.branchDir, "outputs")
	files, _ := os.ReadDir(outputsDir)
	for _, file := range files {
		name := file.Name()
		if strings.Contains(name, fmt.Sprintf("_%s_", namespaceID)) ||
			strings.HasPrefix(name, fmt.Sprintf("%s_", namespaceID)) {
			filePath := filepath.Join(outputsDir, name)
			os.Remove(filePath)
		}
	}

	return nil
}

// ============================================================================
// NAMESPACE METADATA STRUCTURE (LOCAL COPY FOR STORAGE PACKAGE)
// ============================================================================

// NamespaceMetadata is a local copy for storage package to avoid circular dependencies
type NamespaceMetadata struct {
	BranchID    string                 `json:"branch_id"`              // Parent branch identifier
	NodeCount   int                    `json:"node_count"`             // Number of nodes in this namespace
	InputPorts  []string               `json:"input_ports,omitempty"`  // List of input port names bound to this namespace
	OutputPorts []string               `json:"output_ports,omitempty"` // List of output port names exposed by this namespace
	Inputs      map[string]interface{} `json:"inputs,omitempty"`       // Direct input bindings (if any)
	Outputs     map[string]interface{} `json:"outputs,omitempty"`      // Direct outputs (if any)
}

// SymbolContext is a local copy for storage package to avoid circular dependencies
type SymbolContext struct {
	Definitions map[string]interface{} `json:"definitions,omitempty"` // Map of definition names to their values (functions, classes, constants)
}
