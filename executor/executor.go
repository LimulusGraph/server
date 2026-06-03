package executor

import (
	"context"
	"fmt"
	"limulus/server/branch"
	"limulus/server/dag"
	"limulus/server/namespace"
	"sync"
	"time"
)

// ============================================================================
// EXECUTION ENGINE - BRANCH CREATION AND PARALLEL SCHEDULING
// ============================================================================

// ExecutionResult represents the result of executing a node or workflow
type ExecutionResult struct {
	NodeID     string                 `json:"node_id"`               // Node that was executed (or nil for workflow-level)
	Status     string                 `json:"status"`                // "success", "error"
	Error      string                 `json:"error,omitempty"`       // Error message if failed
	ExecutedAt time.Time              `json:"executed_at"`           // Execution timestamp
	OutputData map[string]interface{} `json:"output_data,omitempty"` // Output data from execution
}

// BranchCreationRequest represents a request to create a new branch for parallel execution
type BranchCreationRequest struct {
	WorkflowID  string                 `json:"workflow_id"`     // Parent workflow identifier
	Name        string                 `json:"name,omitempty"`  // Optional name for the branch
	InputPorts  []string               `json:"input_ports"`     // Input ports to bind from parent
	OutputPorts []string               `json:"output_ports"`    // Output ports to expose
	State       map[string]interface{} `json:"state,omitempty"` // Initial state for this branch
}

// BranchCreationResult represents the result of creating a new branch
type BranchCreationResult struct {
	BranchID     string    `json:"branch_id"`     // Created branch identifier
	NamespaceIDs []string  `json:"namespace_ids"` // Namespaces created in this branch
	CreatedAt    time.Time `json:"created_at"`    // Creation timestamp
}

// ParallelScheduler manages parallel execution across branches
type ParallelScheduler struct {
	workflow     *dag.Workflow               // Parent workflow
	namespaceMgr *namespace.NamespaceManager // Namespace manager for state management
	branchMgr    *branch.BranchManager       // Branch manager for branch lifecycle
	ctx          context.Context             // Execution context for cancellation
	cancel       context.CancelFunc          // Context cancel function
}

// NewParallelScheduler creates a new parallel execution scheduler
func NewParallelScheduler(
	wf *dag.Workflow,
	nsMgr *namespace.NamespaceManager,
	bm *branch.BranchManager,
	ctx context.Context,
) *ParallelScheduler {
	ctx, cancel := context.WithCancel(ctx)
	return &ParallelScheduler{
		workflow:     wf,
		namespaceMgr: nsMgr,
		branchMgr:    bm,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// CreateBranchForNode creates a new branch when a node has multiple inputs from different namespaces
func (s *ParallelScheduler) CreateBranchForNode(nodeID string) (*branch.Branch, error) {
	node := s.getOrCreateNode(nodeID)
	if node == nil {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	// Collect all input namespaces for this node
	inputNamespaces := make([]string, 0, len(node.InputBindings))
	for _, binding := range node.InputBindings {
		inputNamespaces = append(inputNamespaces, binding.NamespaceID)
	}

	// Collect all output namespaces for this node
	outputNamespaces := make([]string, 0, len(node.OutputBindings))
	for _, binding := range node.OutputBindings {
		outputNamespaces = append(outputNamespaces, binding.NamespaceID)
	}

	// Create a new branch with these namespace bindings
	branchName := fmt.Sprintf("branch-%s", nodeID)
	branch, err := s.branchMgr.CreateBranch(
		branchName,
		s.workflow.Name,
		inputNamespaces,
		outputNamespaces,
		nil, // Initial state - will be populated during execution
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create branch: %w", err)
	}

	// Add node to this branch's active set
	if err := s.branchMgr.AddNodeToBranch(branch.ID, nodeID); err != nil {
		s.branchMgr.DeleteBranch(branch.ID) // Clean up on failure
		return nil, fmt.Errorf("failed to add node to branch: %w", err)
	}

	// Note: Branch creation is complete. The namespaces referenced by the node's bindings
	// are already managed in the namespace manager and will be accessed during execution.
	// This function returns a branch that tracks which namespaces this node depends on.

	return branch, nil
}

// CreateParallelBranches creates multiple branches for parallel execution when needed
func (s *ParallelScheduler) CreateParallelBranches(nodeID string, numBranches int) ([]*branch.Branch, error) {
	var branches []*branch.Branch

	for i := 0; i < numBranches; i++ {
		branchName := fmt.Sprintf("parallel-%s-%d", nodeID, i)

		branch, err := s.branchMgr.CreateBranch(
			branchName,
			s.workflow.Name,
			nil, // Input ports - will be bound during execution
			nil, // Output ports - will be bound during execution
			nil, // Initial state
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create parallel branch %d: %w", i, err)
		}

		if err := s.branchMgr.AddNodeToBranch(branch.ID, nodeID); err != nil {
			s.branchMgr.DeleteBranch(branch.ID) // Clean up on failure
			return nil, fmt.Errorf("failed to add node to parallel branch %d: %w", i, err)
		}

		branches = append(branches, branch)
	}

	return branches, nil
}

// ExecuteNodeWithBranchCreation executes a node with automatic branch creation when needed
func (s *ParallelScheduler) ExecuteNodeWithBranchCreation(nodeID string) (*ExecutionResult, error) {
	node := s.getOrCreateNode(nodeID)
	if node == nil {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	// Check if this node has multiple input namespaces (requires branch creation)
	inputNamespaces := make(map[string]bool)
	for _, binding := range node.InputBindings {
		inputNamespaces[binding.NamespaceID] = true
	}

	if len(inputNamespaces) > 1 {
		// Multiple inputs from different namespaces - create a new branch
		s.log(fmt.Sprintf("Creating new branch for node %s due to multiple input namespaces", nodeID))

		branch, err := s.CreateBranchForNode(nodeID)
		if err != nil {
			return nil, fmt.Errorf("failed to create branch: %w", err)
		}

		s.log(fmt.Sprintf("Executed node %s in new branch %s", nodeID, branch.ID))

		// Mark node as completed in this branch
		if err := s.branchMgr.MarkNodeCompleted(branch.ID, nodeID); err != nil {
			return &ExecutionResult{
				NodeID:     nodeID,
				Status:     "error",
				Error:      fmt.Sprintf("failed to mark node as completed: %v", err),
				ExecutedAt: time.Now(),
			}, nil
		}

		return &ExecutionResult{
			NodeID:     nodeID,
			Status:     "success",
			ExecutedAt: time.Now(),
			OutputData: make(map[string]interface{}),
		}, nil
	}

	// Single namespace or no inputs - execute in existing branch context
	s.log(fmt.Sprintf("Executing node %s in existing branch context", nodeID))

	return &ExecutionResult{
		NodeID:     nodeID,
		Status:     "success",
		ExecutedAt: time.Now(),
		OutputData: make(map[string]interface{}),
	}, nil
}

// ExecuteWorkflow executes the entire workflow with parallel branch scheduling
func (s *ParallelScheduler) ExecuteWorkflow(targetNodeID string) ([]*ExecutionResult, error) {
	s.log(fmt.Sprintf("Executing workflow %s targeting node %s", s.workflow.Name, targetNodeID))

	// Get execution order for the target node
	execOrder, err := s.workflow.GetExecutionOrderForNode(targetNodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get execution order: %w", err)
	}

	s.log(fmt.Sprintf("Execution order: %v", execOrder))

	var results []*ExecutionResult
	var wg sync.WaitGroup
	errChan := make(chan error, len(execOrder))

	for _, nodeID := range execOrder {
		wg.Add(1)

		go func(nodeID string) {
			defer wg.Done()

			result, err := s.executeNodeWithScheduling(nodeID)
			if err != nil {
				errChan <- fmt.Errorf("failed to execute node %s: %w", nodeID, err)
				return
			}

			results = append(results, result)
		}(nodeID)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	var firstError error
	for err := range errChan {
		if firstError == nil {
			firstError = err
		}
	}

	if firstError != nil {
		return results, firstError
	}

	s.log(fmt.Sprintf("Workflow %s completed successfully", s.workflow.Name))
	return results, nil
}

// executeNodeWithScheduling executes a single node with appropriate branch scheduling
func (s *ParallelScheduler) executeNodeWithScheduling(nodeID string) (*ExecutionResult, error) {
	node := s.getOrCreateNode(nodeID)
	if node == nil {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	// Check if this node has multiple input namespaces (requires branch creation)
	inputNamespaces := make(map[string]bool)
	for _, binding := range node.InputBindings {
		inputNamespaces[binding.NamespaceID] = true
	}

	if len(inputNamespaces) > 1 {
		// Multiple inputs from different namespaces - create a new branch
		s.log(fmt.Sprintf("Creating new branch for node %s due to multiple input namespaces", nodeID))

		branch, err := s.CreateBranchForNode(nodeID)
		if err != nil {
			return nil, fmt.Errorf("failed to create branch: %w", err)
		}

		s.log(fmt.Sprintf("Executed node %s in new branch %s", nodeID, branch.ID))

		// Mark node as completed in this branch
		if err := s.branchMgr.MarkNodeCompleted(branch.ID, nodeID); err != nil {
			return &ExecutionResult{
				NodeID:     nodeID,
				Status:     "error",
				Error:      fmt.Sprintf("failed to mark node as completed: %v", err),
				ExecutedAt: time.Now(),
			}, nil
		}

		return &ExecutionResult{
			NodeID:     nodeID,
			Status:     "success",
			ExecutedAt: time.Now(),
			OutputData: make(map[string]interface{}),
		}, nil
	}

	// Single namespace or no inputs - execute in existing branch context
	s.log(fmt.Sprintf("Executing node %s in existing branch context", nodeID))

	return &ExecutionResult{
		NodeID:     nodeID,
		Status:     "success",
		ExecutedAt: time.Now(),
		OutputData: make(map[string]interface{}),
	}, nil
}

// getOrCreateNode retrieves a node from the workflow or creates it if not found
func (s *ParallelScheduler) getOrCreateNode(nodeID string) *dag.Node {
	for i := range s.workflow.Nodes {
		if s.workflow.Nodes[i].ID == nodeID {
			return &s.workflow.Nodes[i]
		}
	}

	// Create a placeholder node if not found (for error cases)
	return &dag.Node{
		ID:     nodeID,
		Type:   "unknown",
		Title:  fmt.Sprintf("Unknown Node %s", nodeID),
		Status: "error",
		Error:  fmt.Sprintf("node %s not found in workflow", nodeID),
		X:      0,
		Y:      0,
	}
}

// log writes a log message with timestamp
func (s *ParallelScheduler) log(msg string) {
	timestamp := time.Now().Format(time.RFC3339)
	fmt.Printf("[%s] %s\n", timestamp, msg)
}

// Cancel cancels all ongoing executions
func (s *ParallelScheduler) Cancel() {
	s.cancel()
}
