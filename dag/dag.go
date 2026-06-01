package dag

import (
	"errors"
)

type CachedOutput struct {
	Status string `json:"status"` // "idle", "running", "success", "error"
	Stdout string `json:"stdout"`
	Stderr string `json:"stderr"`
}

type Node struct {
	ID           string       `json:"id"`
	Type         string       `json:"type"`
	Title        string       `json:"title"`
	ScriptPath   string       `json:"script_path"`
	Description  string       `json:"description"`
	X            float64      `json:"x"`
	Y            float64      `json:"y"`
	CachedOutput CachedOutput `json:"cached_output"`
}

type Edge struct {
	ID       string `json:"id"`
	From     string `json:"from"`
	To       string `json:"to"`
	FromPort string `json:"from_port"`
	ToPort   string `json:"to_port"`
	Disabled bool   `json:"disabled"`
}

type Workflow struct {
	Version string `json:"version"`
	Name    string `json:"name"`
	Nodes   []Node `json:"nodes"`
	Edges   []Edge `json:"edges"`
}

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
