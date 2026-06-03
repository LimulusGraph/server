package dag

import (
	"errors"
	"limulus/server/namespace"
)

// ============================================================================
// CORE DATA STRUCTURES - REDESIGNED FOR SHARED NAMESPACE MODEL
// ============================================================================

// Node represents a computational node in the graph with namespace bindings
type Node struct {
	ID          string `json:"id"`                    // Unique node identifier
	Type        string `json:"type"`                  // Node type (e.g., "transform", "filter")
	Title       string `json:"title"`                 // Human-readable title
	ScriptPath  string `json:"script_path"`           // Path to the execution script
	Description string `json:"description,omitempty"` // Optional description

	// Namespace bindings - replaces old port-based input/output model
	InputBindings  []InputBinding  `json:"input_bindings"`  // List of input namespace bindings
	OutputBindings []OutputBinding `json:"output_bindings"` // List of output namespace bindings

	// Execution metadata
	Status   string                 `json:"status,omitempty"`   // "idle", "running", "success", "error"
	Error    string                 `json:"error,omitempty"`    // Error message if execution failed
	Metadata map[string]interface{} `json:"metadata,omitempty"` // Additional node-specific metadata

	// Position (for visualization)
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// InputBinding represents how an input port maps to a namespace location
type InputBinding struct {
	PortName    string `json:"port_name"`      // The logical port name on this node (e.g., "data", "labels")
	NamespaceID string `json:"namespace_id"`   // Source namespace identifier that provides this input
	Path        string `json:"path,omitempty"` // Optional path within namespace state (e.g., "results/processed.csv")
	ReadOnly    bool   `json:"read_only"`      // Whether this binding is read-only
}

// OutputBinding represents how an output port maps to a namespace location
type OutputBinding struct {
	PortName    string `json:"port_name"`      // The logical port name on this node (e.g., "output", "predictions")
	NamespaceID string `json:"namespace_id"`   // Destination namespace identifier for this output
	Path        string `json:"path,omitempty"` // Optional path within namespace state
}

// Edge represents a connection between nodes with namespace-aware metadata
type Edge struct {
	ID   string `json:"id"`   // Unique edge identifier
	From string `json:"from"` // Source node ID
	To   string `json:"to"`   // Target node ID

	// Port names for backward compatibility (old model)
	FromPort string `json:"from_port,omitempty"` // Source port name on From node
	ToPort   string `json:"to_port,omitempty"`   // Target port name on To node

	// Namespace mapping - replaces old port-based model
	NamespaceMapping map[string]string `json:"namespace_mapping,omitempty"` // Maps source ports to destination namespace IDs

	// Execution metadata
	Disabled bool                   `json:"disabled"`           // Whether this edge is disabled
	Metadata map[string]interface{} `json:"metadata,omitempty"` // Additional edge-specific metadata
}

// Workflow represents a computational graph with shared namespace support
type Workflow struct {
	Version string `json:"version"` // Workflow version (e.g., "2.0.0" for namespace model)
	Name    string `json:"name"`    // Human-readable workflow name

	// Core graph structures
	Nodes []Node `json:"nodes"` // List of nodes in the workflow
	Edges []Edge `json:"edges"` // List of edges connecting nodes

	// Branch management - replaces old pickle-based caching
	Branches map[string]*BranchRef `json:"branches,omitempty"` // Map of branch IDs to branch references

	// Namespace manager reference (for execution engine)
	NamespaceManager *namespace.NamespaceManager `json:"-"` // Internal namespace manager (not serialized)

	// Execution state
	Status string `json:"status,omitempty"` // "idle", "running", "completed", "failed"
	Error  string `json:"error,omitempty"`  // Error message if workflow execution failed
}

// BranchRef is a lightweight reference to an execution branch
type BranchRef struct {
	ID       string `json:"id"`                  // Branch identifier
	Version  int64  `json:"version"`             // Expected version for optimistic locking
	ReadOnly bool   `json:"read_only,omitempty"` // Whether this is read-only
}

// ============================================================================
// UTILITY FUNCTIONS - ADAPTED FOR NAMESPACE MODEL
// ============================================================================

// BuildAdjacencyLists returns active adjacency lists for forward and backward traversal.
func (w *Workflow) BuildAdjacencyLists() (map[string][]string, map[string][]string) {
	adj := make(map[string][]string)
	revAdj := make(map[string][]string)

	for _, node := range w.Nodes {
		adj[node.ID] = []string{}
		revAdj[node.ID] = []string{}
	}

	for _, edge := range w.Edges {
		// Ignore disabled links
		if edge.Disabled {
			continue
		}
		// Confirm nodes exist
		if _, exists := adj[edge.From]; !exists {
			continue
		}
		if _, exists := adj[edge.To]; !exists {
			continue
		}
		adj[edge.From] = append(adj[edge.From], edge.To)
		revAdj[edge.To] = append(revAdj[edge.To], edge.From)
	}

	return adj, revAdj
}

// TopologicalSort returns nodes in topological order using Kahn's algorithm.
// Returns an error if a cycle is detected.
func (w *Workflow) TopologicalSort() ([]string, error) {
	adj, _ := w.BuildAdjacencyLists()
	inDegree := make(map[string]int)

	for nodeId := range adj {
		inDegree[nodeId] = 0
	}
	for _, neighbors := range adj {
		for _, v := range neighbors {
			inDegree[v]++
		}
	}

	var queue []string
	for nodeId, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, nodeId)
		}
	}

	var order []string
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		order = append(order, u)

		for _, v := range adj[u] {
			inDegree[v]--
			if inDegree[v] == 0 {
				queue = append(queue, v)
			}
		}
	}

	if len(order) != len(w.Nodes) {
		return nil, errors.New("cycle detected in workflow connections")
	}

	return order, nil
}

// GetAncestors returns a set of all active node IDs that must run before the target node (transitive dependencies).
func (w *Workflow) GetAncestors(targetID string) (map[string]bool, error) {
	_, revAdj := w.BuildAdjacencyLists()
	if _, exists := revAdj[targetID]; !exists {
		return nil, errors.New("target node not found in workflow")
	}

	visited := make(map[string]bool)
	var dfs func(string)
	dfs = func(u string) {
		visited[u] = true
		for _, parent := range revAdj[u] {
			if !visited[parent] {
				dfs(parent)
			}
		}
	}

	dfs(targetID)
	// Remove target node itself from the ancestor list
	delete(visited, targetID)
	return visited, nil
}

// GetExecutionOrderForNode returns the topological execution path required to run a specific node,
// including its stale/necessary ancestors.
func (w *Workflow) GetExecutionOrderForNode(targetID string) ([]string, error) {
	fullOrder, err := w.TopologicalSort()
	if err != nil {
		return nil, err
	}

	ancestors, err := w.GetAncestors(targetID)
	if err != nil {
		return nil, err
	}

	// Filter full sorted order to only contain ancestors and the target node itself
	var execOrder []string
	for _, nodeId := range fullOrder {
		if ancestors[nodeId] || nodeId == targetID {
			execOrder = append(execOrder, nodeId)
		}
	}

	return execOrder, nil
}

// GetInputNamespacesForNode returns all namespace IDs that provide inputs to a node.
func (w *Workflow) GetInputNamespacesForNode(nodeID string) ([]string, error) {
	for _, node := range w.Nodes {
		if node.ID == nodeID {
			namespaceIDs := make([]string, 0, len(node.InputBindings))
			for _, binding := range node.InputBindings {
				namespaceIDs = append(namespaceIDs, binding.NamespaceID)
			}
			return namespaceIDs, nil
		}
	}

	return nil, errors.New("node not found in workflow")
}

// GetOutputNamespacesForNode returns all namespace IDs that receive outputs from a node.
func (w *Workflow) GetOutputNamespacesForNode(nodeID string) ([]string, error) {
	for _, node := range w.Nodes {
		if node.ID == nodeID {
			namespaceIDs := make([]string, 0, len(node.OutputBindings))
			for _, binding := range node.OutputBindings {
				namespaceIDs = append(namespaceIDs, binding.NamespaceID)
			}
			return namespaceIDs, nil
		}
	}

	return nil, errors.New("node not found in workflow")
}

// GetNamespaceForPort returns the namespace ID for a specific port on a node.
func (w *Workflow) GetNamespaceForPort(nodeID string, portName string) (string, error) {
	for _, node := range w.Nodes {
		if node.ID == nodeID {
			for _, binding := range node.InputBindings {
				if binding.PortName == portName {
					return binding.NamespaceID, nil
				}
			}
			for _, binding := range node.OutputBindings {
				if binding.PortName == portName {
					return binding.NamespaceID, nil
				}
			}
		}
	}

	return "", errors.New("port not found on any node")
}
