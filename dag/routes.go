package dag

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// Route is one complete root-to-leaf path. Parallel edges define distinct routes.
type Route struct {
	ID    string `json:"id"`
	Nodes []Node `json:"nodes"`
	Edges []Edge `json:"edges"`
}

// Routes validates the graph and enumerates universes in declaration order.
// A bound prevents accidentally starting exponentially many Python processes.
func (w *Workflow) Routes(limit int) ([]Route, error) {
	nodes := map[string]Node{}
	incoming := map[string]int{}
	outgoing := map[string][]Edge{}
	for _, n := range w.Nodes {
		if n.ID == "" {
			return nil, fmt.Errorf("node ID is empty")
		}
		if _, exists := nodes[n.ID]; exists {
			return nil, fmt.Errorf("duplicate node %s", n.ID)
		}
		nodes[n.ID] = n
	}
	for _, e := range w.Edges {
		if e.Disabled {
			continue
		}
		if _, ok := nodes[e.From]; !ok {
			return nil, fmt.Errorf("edge %s has missing source", e.ID)
		}
		if _, ok := nodes[e.To]; !ok {
			return nil, fmt.Errorf("edge %s has missing target", e.ID)
		}
		outgoing[e.From] = append(outgoing[e.From], e)
		incoming[e.To]++
	}
	if _, err := w.TopologicalSort(); err != nil {
		return nil, err
	}
	var routes []Route
	seenRoutes := map[string]bool{}
	var walk func(string, []Node, []Edge) error
	walk = func(id string, path []Node, edges []Edge) error {
		path = append(append([]Node{}, path...), nodes[id])
		if len(outgoing[id]) == 0 {
			if len(routes) >= limit {
				return fmt.Errorf("graph exceeds the %d universe limit", limit)
			}
			// Include paths and edge ports, but exclude presentation and execution metadata.
			identity := struct {
				Nodes []string
				Edges []string
			}{}
			for _, n := range path {
				identity.Nodes = append(identity.Nodes, n.ID, n.ScriptPath)
			}
			for _, e := range edges {
				identity.Edges = append(identity.Edges, e.ID, e.FromPort, e.ToPort)
			}
			data, _ := json.Marshal(identity)
			id := fmt.Sprintf("%x", sha256.Sum256(data))[:16]
			if seenRoutes[id] {
				return fmt.Errorf("parallel edges must have distinct IDs")
			}
			seenRoutes[id] = true
			routes = append(routes, Route{ID: id, Nodes: path, Edges: edges})
			return nil
		}
		for _, e := range outgoing[id] {
			next := append(append([]Edge{}, edges...), e)
			if err := walk(e.To, path, next); err != nil {
				return err
			}
		}
		return nil
	}
	for _, n := range w.Nodes {
		if incoming[n.ID] == 0 {
			if err := walk(n.ID, nil, nil); err != nil {
				return nil, err
			}
		}
	}
	return routes, nil
}
