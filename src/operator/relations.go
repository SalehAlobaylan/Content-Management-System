package operator

import (
	"fmt"
	"sort"
	"strings"
)

const (
	cardinalityOneToOne  = "one_to_one"
	cardinalityOneToMany = "one_to_many"
)

// Validate ensures the code-owned graph has every property needed to make a
// tenant-safe, bounded read. A model or browser can select neither a join nor
// a URL: both remain registry data reviewed with the CMS source.
func (registry AdapterRegistry) Validate() error {
	if len(registry.adapters) == 0 {
		return fmt.Errorf("%w: adapter registry is empty", ErrInvalidContract)
	}
	for key, descriptor := range registry.adapters {
		if key != descriptor.Key || strings.TrimSpace(descriptor.Version) == "" || strings.TrimSpace(descriptor.Domain) == "" || strings.TrimSpace(descriptor.RequiredPermission) == "" || descriptor.MaxRows < 1 || descriptor.MaxRows > normalQuestionBudget.MaxRowsPerDomain || descriptor.MaxAge <= 0 || !strings.HasPrefix(descriptor.DeepLinkBase, "/") || strings.HasPrefix(descriptor.DeepLinkBase, "//") {
			return fmt.Errorf("%w: unsafe adapter registration %q", ErrInvalidContract, key)
		}
	}

	seen := map[string]struct{}{}
	for from, edges := range registry.edges {
		if strings.TrimSpace(from) == "" {
			return fmt.Errorf("%w: relationship registry contains an empty source", ErrInvalidContract)
		}
		for _, edge := range edges {
			identity := edge.From + "->" + edge.To
			if edge.From != from || strings.TrimSpace(edge.To) == "" || strings.TrimSpace(edge.RequiredPermission) == "" || edge.TenantKey != "tenant_id" || (edge.Cardinality != cardinalityOneToOne && edge.Cardinality != cardinalityOneToMany) || edge.MaxDepth < 1 || edge.MaxDepth > normalQuestionBudget.MaxRelationDepth || edge.MaxRows < 1 || edge.MaxRows > normalQuestionBudget.MaxRowsPerDomain || edge.MaxAge <= 0 {
				return fmt.Errorf("%w: unsafe relationship registration %q", ErrInvalidContract, identity)
			}
			if _, duplicate := seen[identity]; duplicate {
				return fmt.Errorf("%w: duplicate relationship registration %q", ErrInvalidContract, identity)
			}
			seen[identity] = struct{}{}
		}
	}
	return nil
}

// BoundedTraversal returns only permission-allowed, registered edges reachable
// within the fixed read budget. It is deliberately structural: adapters still
// own their tenant-filtered queries, while this method prevents an unregistered
// or over-depth relationship from becoming eligible for collection.
func (registry AdapterRegistry) BoundedTraversal(start string, access AccessSnapshot, budget ReadBudget) ([]RelationshipEdge, error) {
	if strings.TrimSpace(start) == "" || budget.MaxAdapters < 1 || budget.MaxAdapters > normalQuestionBudget.MaxAdapters || budget.MaxRelationDepth < 1 || budget.MaxRelationDepth > normalQuestionBudget.MaxRelationDepth || budget.MaxRowsPerDomain < 1 || budget.MaxRowsPerDomain > normalQuestionBudget.MaxRowsPerDomain {
		return nil, fmt.Errorf("%w: invalid relationship read budget", ErrInvalidContract)
	}
	if err := registry.Validate(); err != nil {
		return nil, err
	}

	type node struct {
		kind  string
		depth int
	}
	queue := []node{{kind: start, depth: 0}}
	seenNodes := map[string]int{start: 0}
	edges := make([]RelationshipEdge, 0, budget.MaxAdapters)
	for len(queue) > 0 && len(edges) < budget.MaxAdapters {
		current := queue[0]
		queue = queue[1:]
		candidates := registry.EdgesFrom(current.kind)
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].To < candidates[j].To })
		for _, edge := range candidates {
			childDepth := current.depth + 1
			if len(edges) >= budget.MaxAdapters || childDepth > budget.MaxRelationDepth || childDepth > edge.MaxDepth || edge.MaxRows > budget.MaxRowsPerDomain || !access.HasPermission(edge.RequiredPermission) {
				continue
			}
			edges = append(edges, edge)
			if seenDepth, seen := seenNodes[edge.To]; !seen || childDepth < seenDepth {
				seenNodes[edge.To] = childDepth
				queue = append(queue, node{kind: edge.To, depth: childDepth})
			}
		}
	}
	return edges, nil
}
