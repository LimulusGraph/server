package branch

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Branch represents an execution branch in the graph computation model
type Branch struct {
	ID          string                 `json:"id"`                     // Unique branch identifier
	Name        string                 `json:"name,omitempty"`         // Human-readable name for the branch
	CreatedAt   time.Time              `json:"created_at"`             // Branch creation timestamp
	UpdatedAt   time.Time              `json:"updated_at"`             // Last modification timestamp
	State       map[string]interface{} `json:"state,omitempty"`        // Shared state across all nodes in this branch
	Metadata    BranchMetadata         `json:"metadata"`               // Metadata about the branch
	InputPorts  []string               `json:"input_ports,omitempty"`  // Input ports bound to this branch
	OutputPorts []string               `json:"output_ports,omitempty"` // Output ports exposed by this branch
}

// BranchMetadata contains structural information about a branch
type BranchMetadata struct {
	WorkflowID  string            `json:"workflow_id"`         // Parent workflow identifier
	NodeCount   int               `json:"node_count"`          // Number of nodes in this branch
	ActiveNodes []string          `json:"active_nodes"`        // Nodes currently active in execution
	Completed   map[string]bool   `json:"completed,omitempty"` // Map of completed node IDs
	Failed      map[string]string `json:"failed,omitempty"`    // Map of failed node IDs to error messages
}

// BranchRef is a lightweight reference to a branch used in graph structures
type BranchRef struct {
	ID       string `json:"id"`                  // Branch identifier
	Version  int64  `json:"version"`             // Expected version for optimistic locking
	ReadOnly bool   `json:"read_only,omitempty"` // Whether this is read-only
}

// BranchManager handles lifecycle operations for branches
type BranchManager struct {
	branches map[string]*Branch // ID -> Branch mapping
	mu       sync.RWMutex       // Mutex for concurrent access
	nextID   int64              // Next branch ID counter
}

// NewBranchManager creates a new branch manager instance
func NewBranchManager() *BranchManager {
	return &BranchManager{
		branches: make(map[string]*Branch),
		nextID:   1,
	}
}

// CreateBranch creates a new execution branch with the given parameters
func (bm *BranchManager) CreateBranch(
	name string,
	workflowID string,
	inputPorts []string,
	outputPorts []string,
	state map[string]interface{},
) (*Branch, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	id := fmt.Sprintf("br-%d", bm.nextID)
	bm.nextID++

	branch := &Branch{
		ID:        id,
		Name:      name,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		State:     state,
		Metadata: BranchMetadata{
			WorkflowID:  workflowID,
			NodeCount:   0,
			ActiveNodes: make([]string, 0),
			Completed:   make(map[string]bool),
			Failed:      make(map[string]string),
		},
		InputPorts:  inputPorts,
		OutputPorts: outputPorts,
	}

	bm.branches[id] = branch
	return branch, nil
}

// GetBranch retrieves a branch by ID
func (bm *BranchManager) GetBranch(id string) (*Branch, bool) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	branch, exists := bm.branches[id]
	return branch, exists
}

// MarkNodeCompleted marks a node as completed in the branch
func (bm *BranchManager) MarkNodeCompleted(branchID string, nodeID string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	branch, exists := bm.branches[branchID]
	if !exists {
		return fmt.Errorf("branch %s not found", branchID)
	}

	branch.Metadata.Completed[nodeID] = true
	branch.Metadata.ActiveNodes = removeString(branch.Metadata.ActiveNodes, nodeID)
	branch.UpdatedAt = time.Now()

	return nil
}

// MarkNodeFailed marks a node as failed in the branch
func (bm *BranchManager) MarkNodeFailed(branchID string, nodeID string, error string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	branch, exists := bm.branches[branchID]
	if !exists {
		return fmt.Errorf("branch %s not found", branchID)
	}

	branch.Metadata.Failed[nodeID] = error
	branch.Metadata.ActiveNodes = removeString(branch.Metadata.ActiveNodes, nodeID)
	branch.UpdatedAt = time.Time{} // Zero value indicates failure timestamp

	return nil
}

// AddNodeToBranch adds a node to the branch's active set
func (bm *BranchManager) AddNodeToBranch(branchID string, nodeID string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	branch, exists := bm.branches[branchID]
	if !exists {
		return fmt.Errorf("branch %s not found", branchID)
	}

	// Only add if not already present
	for _, n := range branch.Metadata.ActiveNodes {
		if n == nodeID {
			return nil // Already added
		}
	}

	branch.Metadata.ActiveNodes = append(branch.Metadata.ActiveNodes, nodeID)
	branch.UpdatedAt = time.Now()

	return nil
}

// RemoveNodeFromBranch removes a node from the branch's active set
func (bm *BranchManager) RemoveNodeFromBranch(branchID string, nodeID string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	branch, exists := bm.branches[branchID]
	if !exists {
		return fmt.Errorf("branch %s not found", branchID)
	}

	// Remove from active nodes if present
	newNodes := make([]string, 0, len(branch.Metadata.ActiveNodes))
	for _, n := range branch.Metadata.ActiveNodes {
		if n != nodeID {
			newNodes = append(newNodes, n)
		}
	}
	branch.Metadata.ActiveNodes = newNodes

	return nil
}

// DeleteBranch removes a branch from the manager
func (bm *BranchManager) DeleteBranch(id string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if _, exists := bm.branches[id]; !exists {
		return fmt.Errorf("branch %s not found", id)
	}

	delete(bm.branches, id)
	return nil
}

// ListBranches returns all branch IDs in the manager
func (bm *BranchManager) ListBranches() []string {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	var ids []string
	for id := range bm.branches {
		ids = append(ids, id)
	}
	return ids
}

// MarshalJSON implements JSON marshaling for Branch
func (b *Branch) MarshalJSON() ([]byte, error) {
	type Alias Branch
	aux := &struct {
		CreatedAt string `json:"created_at"`
		UpdatedAt string `json:"updated_at"`
		Alias
	}{
		CreatedAt: b.CreatedAt.Format(time.RFC3339),
		UpdatedAt: b.UpdatedAt.Format(time.RFC3339),
		Alias:     Alias(*b),
	}

	return json.Marshal(aux)
}

// UnmarshalJSON implements JSON unmarshaling for Branch
func (b *Branch) UnmarshalJSON(data []byte) error {
	type Alias Branch
	aux := &struct {
		CreatedAt string `json:"created_at"`
		UpdatedAt string `json:"updated_at"`
		Alias
	}{
		Alias: Alias(*b),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	b.CreatedAt, _ = time.Parse(time.RFC3339, aux.CreatedAt)
	b.UpdatedAt, _ = time.Parse(time.RFC3339, aux.UpdatedAt)

	return nil
}

// removeString removes a string from a slice (helper function)
func removeString(slice []string, s string) []string {
	result := make([]string, 0, len(slice))
	for _, item := range slice {
		if item != s {
			result = append(result, item)
		}
	}
	return result
}
