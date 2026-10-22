package dag

import "testing"

func TestRoutes(t *testing.T) {
	w := Workflow{Nodes: []Node{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}}, Edges: []Edge{
		{ID: "ab", From: "a", To: "b"}, {ID: "ac", From: "a", To: "c"},
		{ID: "bd", From: "b", To: "d"}, {ID: "cd", From: "c", To: "d"},
	}}
	routes, err := w.Routes(10)
	if err != nil || len(routes) != 2 {
		t.Fatalf("%v %v", routes, err)
	}
	if routes[0].ID == routes[1].ID {
		t.Fatal("route IDs collide")
	}
	if _, err := w.Routes(1); err == nil {
		t.Fatal("expected route limit")
	}
	w.Edges[1].Disabled = true
	routes, err = w.Routes(10)
	// c becomes a root, so both complete universes still exist.
	if err != nil || len(routes) != 2 || routes[1].Nodes[0].ID != "c" {
		t.Fatalf("%v %v", routes, err)
	}
	w.Edges = append(w.Edges, Edge{From: "d", To: "a"})
	if _, err := w.Routes(10); err == nil {
		t.Fatal("expected cycle error")
	}
}
func TestInvalidRoutes(t *testing.T) {
	for _, wf := range []Workflow{
		{Nodes: []Node{{ID: "a"}, {ID: "a"}}},
		{Nodes: []Node{{ID: "a"}}, Edges: []Edge{{From: "a", To: "missing"}}},
	} {
		if _, err := wf.Routes(10); err == nil {
			t.Fatal("expected invalid graph error")
		}
	}
}

func TestParallelEdgesNeedDistinctIdentity(t *testing.T) {
	w := Workflow{Nodes: []Node{{ID: "a"}, {ID: "b"}}, Edges: []Edge{{From: "a", To: "b"}, {From: "a", To: "b"}}}
	if _, err := w.Routes(10); err == nil {
		t.Fatal("ambiguous routes could share a kernel")
	}
	w.Edges[0].ID = "first"
	w.Edges[1].ID = "second"
	routes, err := w.Routes(10)
	if err != nil || len(routes) != 2 || routes[0].ID == routes[1].ID {
		t.Fatalf("%v %v", routes, err)
	}
}
