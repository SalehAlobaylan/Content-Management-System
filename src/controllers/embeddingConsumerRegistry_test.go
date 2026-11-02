package controllers

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// This is a supplementary inventory, not a proof of SQL control flow or vector
// compatibility. Runtime fixtures must still prove both operands fail closed.
// AST traversal ignores comments, inventories SQL operator literals and known
// in-memory cosine/dot-product calls per function, and requires an explicit
// registry entry plus a local identity gate or a documented guarded boundary.
// A new function in an already registered file is therefore still new work.
type semanticConsumerRegistration struct {
	Keys []string
	// DelegatedGuard is an invoked helper whose function contains an identity
	// gate. This documents input-loading contracts, not arbitrary file names.
	DelegatedGuard string
	// GuardedCallers is only for math helpers receiving already-guarded vectors.
	// Every actual caller must be listed and must itself satisfy the gate check.
	GuardedCallers []string
}

var knownSemanticConsumers = map[string]semanticConsumerRegistration{
	"storyClassifier.go:classifyContentTopic":              {Keys: []string{"story_classify"}},
	"adminStoriesController.go:topicRepresentativeTexts":   {Keys: []string{"story_related"}},
	"feedStories.go:relatedCandidatesForPage":              {Keys: []string{"story_related"}},
	"feedStories.go:relatedKNNIDsChecked":                  {Keys: []string{"story_related"}},
	"internalDiscoveryController.go:noveltyFactor":         {Keys: []string{"discovery_dense"}},
	"internalDiscoveryController.go:scoreSuggestionRows":   {Keys: []string{"discovery_dense"}},
	"intelController.go:promoteForProfile":                 {Keys: []string{"discovery_dense"}},
	"internalContentController.go:runKNNQuery":             {Keys: []string{"knn_dense", "related_dense"}},
	"intelligenceController.go:GetSimilarContent":          {Keys: []string{"related_dense"}},
	"redundancyRunner.go:redundancyCandidates":             {Keys: []string{"redundancy_dense"}},
	"redundancyRunner.go:redundancyLanes":                  {Keys: []string{"redundancy_dense", "redundancy_image"}},
	"preferenceAutopilotRunner.go:findNearDuplicateTopics": {Keys: []string{"proposal_dedup"}, DelegatedGuard: "preferenceTopicsController.go:activeTopicVectors"},
	"preferenceTopicsController.go:remapCatalogTopics":     {Keys: []string{"topic_map"}},
	"preferenceTopicsController.go:remapSingleTopic":       {Keys: []string{"topic_map"}},
	"preferenceMappingMaintenance.go:limitedMapSweep":      {Keys: []string{"topic_map"}},
	"preferenceMappingMaintenance.go:reevaluateCorpusPage": {Keys: []string{"topic_map"}},
	"preferenceTopicsController.go:topTopicMatches": {Keys: []string{"topic_map"}, GuardedCallers: []string{
		"preferenceTopicsController.go:remapCatalogTopics",
		"preferenceTopicsController.go:remapSingleTopic",
		"preferenceMappingMaintenance.go:limitedMapSweep",
		"preferenceMappingMaintenance.go:reevaluateCorpusPage",
	}},
	// The scorer constructor loads current-space topics/category means;
	// ensureEmbedding checks the proposal against that frozen scorer space.
	"preferenceAutopilotScorer.go:proposalScorer.score": {Keys: []string{"proposal_dedup"}, DelegatedGuard: "preferenceAutopilotScorer.go:proposalScorer.ensureEmbedding"},
	"preferenceAutopilotScorer.go:proposalScorer.maxTopicCosine": {Keys: []string{"proposal_dedup"}, GuardedCallers: []string{
		"preferenceAutopilotScorer.go:proposalScorer.score",
	}},
	"preferenceAutopilotScorer.go:proposalScorer.topTwoCategoryCosine": {Keys: []string{"proposal_dedup"}, GuardedCallers: []string{
		"preferenceAutopilotScorer.go:proposalScorer.score",
	}},
}

type semanticFunction struct {
	Name        string
	Comparisons []string
	Calls       map[string]bool
	HasGate     bool
}

var semanticSpacePredicate = regexp.MustCompile(`(?i)\b\w*space_id\s*=`)

func semanticCallName(expr ast.Expr) string {
	switch n := expr.(type) {
	case *ast.Ident:
		return n.Name
	case *ast.SelectorExpr:
		return n.Sel.Name
	}
	return ""
}

func semanticIdentityExpression(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		name := ""
		switch x := n.(type) {
		case *ast.Ident:
			name = x.Name
		case *ast.SelectorExpr:
			name = x.Sel.Name
		}
		found = found || strings.Contains(strings.ToLower(name), "spaceid")
		return true
	})
	return found
}

func semanticFunctionName(fn *ast.FuncDecl) string {
	if fn.Recv != nil && len(fn.Recv.List) != 0 {
		receiver := fn.Recv.List[0].Type
		if pointer, ok := receiver.(*ast.StarExpr); ok {
			receiver = pointer.X
		}
		if name, ok := receiver.(*ast.Ident); ok {
			return name.Name + "." + fn.Name.Name
		}
	}
	return fn.Name.Name
}

func inspectSemanticSource(path string, source []byte) (map[string]semanticFunction, error) {
	f, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
	if err != nil {
		return nil, err
	}
	functions := map[string]semanticFunction{}
	for _, declaration := range f.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		name := "<package>"
		var body ast.Node = declaration
		if ok {
			if fn.Body == nil {
				continue
			}
			name, body = semanticFunctionName(fn), fn.Body
		}
		id := filepath.ToSlash(path) + ":" + name
		info := semanticFunction{Name: name, Calls: map[string]bool{}}
		ast.Inspect(body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.BasicLit:
				if x.Kind == token.STRING {
					value, _ := strconv.Unquote(x.Value)
					if strings.Contains(value, "<=>") {
						info.Comparisons = append(info.Comparisons, "SQL <=>")
					}
					info.HasGate = info.HasGate || semanticSpacePredicate.MatchString(value)
				}
			case *ast.CallExpr:
				called := semanticCallName(x.Fun)
				info.Calls[called] = true
				lower := strings.ToLower(called)
				if strings.Contains(lower, "cosine") || lower == "dotproduct" || called == "topTopicMatches" {
					info.Comparisons = append(info.Comparisons, called)
				}
			case *ast.IfStmt:
				// An actual condition, not a mention in a comment, SELECT list,
				// struct field, or unrelated function elsewhere in this file.
				ast.Inspect(x.Cond, func(n ast.Node) bool {
					if comparison, ok := n.(*ast.BinaryExpr); ok && (comparison.Op == token.EQL || comparison.Op == token.NEQ) &&
						(semanticIdentityExpression(comparison.X) || semanticIdentityExpression(comparison.Y)) {
						info.HasGate = true
					}
					return true
				})
			}
			return true
		})
		if previous, exists := functions[id]; exists {
			info.Comparisons = append(previous.Comparisons, info.Comparisons...)
		}
		functions[id] = info
	}
	return functions, nil
}

func semanticInventoryProblems(functions map[string]semanticFunction, registrations map[string]semanticConsumerRegistration) []string {
	var problems []string
	for id, fn := range functions {
		if len(fn.Comparisons) == 0 {
			continue
		}
		registration, registered := registrations[id]
		if !registered {
			problems = append(problems, fmt.Sprintf("%s: unregistered similarity consumer (%s)", id, strings.Join(fn.Comparisons, ", ")))
			continue
		}
		if fn.HasGate {
			continue
		}
		if helperID := registration.DelegatedGuard; helperID != "" {
			helper, exists := functions[helperID]
			called := helper.Name[strings.LastIndex(helper.Name, ".")+1:]
			if exists && helper.HasGate && fn.Calls[called] {
				continue
			}
		}
		if len(registration.GuardedCallers) > 0 {
			allowed := map[string]bool{}
			for _, caller := range registration.GuardedCallers {
				allowed[caller] = true
			}
			calledName := fn.Name[strings.LastIndex(fn.Name, ".")+1:]
			callers, guarded := 0, true
			for callerID, caller := range functions {
				if !caller.Calls[calledName] {
					continue
				}
				callers++
				callerRegistration, exists := registrations[callerID]
				helper, helperExists := functions[callerRegistration.DelegatedGuard]
				helperName := helper.Name[strings.LastIndex(helper.Name, ".")+1:]
				delegated := helperExists && helper.HasGate && caller.Calls[helperName]
				if !allowed[callerID] || !exists || (!caller.HasGate && !delegated) {
					guarded = false
				}
			}
			if callers > 0 && guarded {
				continue
			}
		}
		problems = append(problems, fmt.Sprintf("%s: no function-local identity gate or valid documented guarded boundary", id))
	}
	sort.Strings(problems)
	return problems
}

func TestNoUnregisteredSimilarityConsumers(t *testing.T) {
	registered := registeredConsumerKeys()
	for function, registration := range knownSemanticConsumers {
		for _, key := range registration.Keys {
			if !registered[key] {
				t.Errorf("%s maps to unknown surface consumer key %q", function, key)
			}
		}
	}
	functions := map[string]semanticFunction{}
	for _, root := range []string{".", "../intelligence"} {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			found, err := inspectSemanticSource(path, data)
			if err != nil {
				return err
			}
			for id, function := range found {
				functions[id] = function
			}
			return nil
		})
		if err != nil {
			t.Fatalf("inventory %s: %v", root, err)
		}
	}
	for _, problem := range semanticInventoryProblems(functions, knownSemanticConsumers) {
		t.Error(problem)
	}
}

func TestSimilarityInventoryDetectsNewConsumers(t *testing.T) {
	registration := map[string]semanticConsumerRegistration{"known.go:known": {Keys: []string{"related_dense"}}}
	cases := []struct{ name, source, want string }{
		{"new SQL function in known file", `package p
func known(){ query("embedding <=> other WHERE embedding_space_id = ?") }
func added(){ query("embedding <=> other") }`, "known.go:added: unregistered"},
		{"new in-memory cosine", `package p
func known(){ query("embedding <=> other WHERE embedding_space_id = ?") }
func added(){ cosine(a,b) }`, "known.go:added: unregistered"},
		{"new dot product", `package p
func added(){ dotProduct(a,b) }`, "known.go:added: unregistered"},
		{"guard in a different function does not count", `package p
func known(){ cosine(a,b) }
func unrelated(){ if a.SpaceID != b.SpaceID { return } }`, "known.go:known: no function-local identity gate"},
		{"comments do not register comparisons", `package p
// cosine(a,b), embedding <=> other
func known(){}`, ""},
		{"local identity gate", `package p
func known(){ if a.SpaceID != b.SpaceID { return }; cosine(a,b) }`, ""},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			functions, err := inspectSemanticSource("known.go", []byte(test.source))
			if err != nil {
				t.Fatal(err)
			}
			problems := strings.Join(semanticInventoryProblems(functions, registration), "\n")
			if (test.want == "" && problems != "") || (test.want != "" && !strings.Contains(problems, test.want)) {
				t.Fatalf("want %q, got %q", test.want, problems)
			}
		})
	}
}
