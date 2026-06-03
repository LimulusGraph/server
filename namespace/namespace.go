package namespace

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Namespace represents a shared state container for graph execution branches
type Namespace struct {
	ID        string                 `json:"id"`              // Unique namespace identifier (branch ID)
	Name      string                 `json:"name,omitempty"`  // Human-readable name for the branch
	CreatedAt time.Time              `json:"created_at"`      // Namespace creation timestamp
	UpdatedAt time.Time              `json:"updated_at"`      // Last modification timestamp
	Version   int64                  `json:"version"`         // Monotonic version counter for conflict resolution
	State     map[string]interface{} `json:"state,omitempty"` // Shared state dictionary (JSON-serializable)
	Metadata  NamespaceMetadata      `json:"metadata"`        // Metadata about this namespace
}

// NamespaceMetadata contains structural information about the namespace
type NamespaceMetadata struct {
	BranchID    string   `json:"branch_id"`              // Parent branch identifier
	NodeCount   int      `json:"node_count"`             // Number of nodes in this namespace
	InputPorts  []string `json:"input_ports,omitempty"`  // List of input port names bound to this namespace
	OutputPorts []string `json:"output_ports,omitempty"` // List of output port names exposed by this namespace

	// Port-to-namespace mapping metadata
	PortMappings map[string]PortMappingMetadata `json:"port_mappings,omitempty"` // Maps port names to their namespace binding details
}

// PortMappingMetadata contains detailed information about a specific port's namespace binding
type PortMappingMetadata struct {
	NamespaceID string    `json:"namespace_id"`   // The namespace providing/receiving this port
	Path        string    `json:"path,omitempty"` // Path within namespace state for this port
	ReadOnly    bool      `json:"read_only"`      // Whether this port binding is read-only
	CreatedAt   time.Time `json:"created_at"`     // When this mapping was established
}

// NamespaceRef is a lightweight reference to a namespace used in graph structures
type NamespaceRef struct {
	ID       string `json:"id"`                  // Namespace identifier
	Version  int64  `json:"version"`             // Expected version for optimistic locking
	ReadOnly bool   `json:"read_only,omitempty"` // Whether this is a read-only reference
}

// InputBinding represents how an input port maps to a namespace location
type InputBinding struct {
	PortName    string `json:"port_name"`      // The port name on the target node
	NamespaceID string `json:"namespace_id"`   // Source namespace identifier
	Path        string `json:"path,omitempty"` // Optional path within namespace state (e.g., "data/processed.csv")
	ReadOnly    bool   `json:"read_only"`      // Whether this binding is read-only
}

// OutputBinding represents how an output port maps to a namespace location
type OutputBinding struct {
	PortName    string `json:"port_name"`      // The port name on the source node
	NamespaceID string `json:"namespace_id"`   // Destination namespace identifier for this output
	Path        string `json:"path,omitempty"` // Optional path within namespace state
}

// NamespaceManager handles lifecycle operations for namespaces
type NamespaceManager struct {
	namespaces map[string]*Namespace // ID -> Namespace mapping
	mu         sync.RWMutex          // Mutex for concurrent access
	nextID     int64                 // Next namespace ID counter
}

// NewNamespaceManager creates a new namespace manager instance
func NewNamespaceManager() *NamespaceManager {
	return &NamespaceManager{
		namespaces: make(map[string]*Namespace),
		nextID:     1,
	}
}

// CreateNamespace creates a new namespace with the given parameters
func (nm *NamespaceManager) CreateNamespace(
	name string,
	branchID string,
	state map[string]interface{},
	inputPorts []string,
	outputPorts []string,
) (*Namespace, error) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	id := fmt.Sprintf("ns-%d", nm.nextID)
	nm.nextID++

	namespace := &Namespace{
		ID:        id,
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
		},
	}

	nm.namespaces[id] = namespace
	return namespace, nil
}

// GetNamespace retrieves a namespace by ID
func (nm *NamespaceManager) GetNamespace(id string) (*Namespace, bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	namespace, exists := nm.namespaces[id]
	return namespace, exists
}

// UpdateNamespace updates an existing namespace's state and metadata
func (nm *NamespaceManager) UpdateNamespace(
	id string,
	state map[string]interface{},
) (*Namespace, error) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	namespace, exists := nm.namespaces[id]
	if !exists {
		return nil, fmt.Errorf("namespace %s not found", id)
	}

	namespace.State = state
	namespace.UpdatedAt = time.Now()
	namespace.Version++ // Increment version for conflict resolution

	return namespace, nil
}

// BindInput binds an input port to a namespace location with full metadata
func (nm *NamespaceManager) BindInput(
	namespaceID string,
	portName string,
	path string,
	readOnly bool,
) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	ns, exists := nm.namespaces[namespaceID]
	if !exists {
		return fmt.Errorf("namespace %s not found", namespaceID)
	}

	// Update metadata with port mapping info
	mapping := PortMappingMetadata{
		NamespaceID: namespaceID,
		Path:        path,
		ReadOnly:    readOnly,
		CreatedAt:   time.Now(),
	}

	if ns.Metadata.PortMappings == nil {
		ns.Metadata.PortMappings = make(map[string]PortMappingMetadata)
	}
	ns.Metadata.PortMappings[portName] = mapping
	ns.Metadata.InputPorts = append(ns.Metadata.InputPorts, portName)
	ns.UpdatedAt = time.Now()

	return nil
}

// BindOutput binds an output port to a namespace location with full metadata
func (nm *NamespaceManager) BindOutput(
	namespaceID string,
	portName string,
	path string,
) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	ns, exists := nm.namespaces[namespaceID]
	if !exists {
		return fmt.Errorf("namespace %s not found", namespaceID)
	}

	// Update metadata with port mapping info
	mapping := PortMappingMetadata{
		NamespaceID: namespaceID,
		Path:        path,
		CreatedAt:   time.Now(),
	}

	if ns.Metadata.PortMappings == nil {
		ns.Metadata.PortMappings = make(map[string]PortMappingMetadata)
	}
	ns.Metadata.PortMappings[portName] = mapping
	ns.Metadata.OutputPorts = append(ns.Metadata.OutputPorts, portName)
	ns.UpdatedAt = time.Now()

	return nil
}

// UnbindInput removes an input binding from a namespace
func (nm *NamespaceManager) UnbindInput(namespaceID string, portName string) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	ns, exists := nm.namespaces[namespaceID]
	if !exists {
		return fmt.Errorf("namespace %s not found", namespaceID)
	}

	delete(ns.Metadata.PortMappings, portName)
	ns.Metadata.InputPorts = removeString(ns.Metadata.InputPorts, portName)
	ns.UpdatedAt = time.Now()

	return nil
}

// UnbindOutput removes an output binding from a namespace
func (nm *NamespaceManager) UnbindOutput(namespaceID string, portName string) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	ns, exists := nm.namespaces[namespaceID]
	if !exists {
		return fmt.Errorf("namespace %s not found", namespaceID)
	}

	delete(ns.Metadata.PortMappings, portName)
	ns.Metadata.OutputPorts = removeString(ns.Metadata.OutputPorts, portName)
	ns.UpdatedAt = time.Now()

	return nil
}

// IncrementVersion increments the namespace version for optimistic locking
func (nm *NamespaceManager) IncrementVersion(id string) (int64, error) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	namespace, exists := nm.namespaces[id]
	if !exists {
		return 0, fmt.Errorf("namespace %s not found", id)
	}

	namespace.Version++
	namespace.UpdatedAt = time.Now()
	return namespace.Version, nil
}

// DeleteNamespace removes a namespace from the manager
func (nm *NamespaceManager) DeleteNamespace(id string) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if _, exists := nm.namespaces[id]; !exists {
		return fmt.Errorf("namespace %s not found", id)
	}

	delete(nm.namespaces, id)
	return nil
}

// ListNamespaces returns all namespace IDs in the manager
func (nm *NamespaceManager) ListNamespaces() []string {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	var ids []string
	for id := range nm.namespaces {
		ids = append(ids, id)
	}
	return ids
}

// ListNamespacesWithMetadata returns all namespaces with their full metadata
func (nm *NamespaceManager) ListNamespacesWithMetadata() []*Namespace {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	var namespaces []*Namespace
	for _, ns := range nm.namespaces {
		namespaces = append(namespaces, ns)
	}
	return namespaces
}

// GetPortMappings returns all port mappings for a namespace
func (nm *NamespaceManager) GetPortMappings(namespaceID string) ([]PortMappingMetadata, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	ns, exists := nm.namespaces[namespaceID]
	if !exists {
		return nil, fmt.Errorf("namespace %s not found", namespaceID)
	}

	var mappings []PortMappingMetadata
	for _, mapping := range ns.Metadata.PortMappings {
		mappings = append(mappings, mapping)
	}
	return mappings, nil
}

// MarshalJSON implements JSON marshaling for Namespace
func (n *Namespace) MarshalJSON() ([]byte, error) {
	type Alias Namespace
	aux := &struct {
		CreatedAt string `json:"created_at"`
		UpdatedAt string `json:"updated_at"`
		Alias
	}{
		CreatedAt: n.CreatedAt.Format(time.RFC3339),
		UpdatedAt: n.UpdatedAt.Format(time.RFC3339),
		Alias:     Alias(*n),
	}

	return json.Marshal(aux)
}

// UnmarshalJSON implements JSON unmarshaling for Namespace
func (n *Namespace) UnmarshalJSON(data []byte) error {
	type Alias Namespace
	aux := &struct {
		CreatedAt string `json:"created_at"`
		UpdatedAt string `json:"updated_at"`
		Alias
	}{
		Alias: Alias(*n),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	n.CreatedAt, _ = time.Parse(time.RFC3339, aux.CreatedAt)
	n.UpdatedAt, _ = time.Parse(time.RFC3339, aux.UpdatedAt)

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
