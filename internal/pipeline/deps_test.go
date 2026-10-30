package pipeline

import (
	"os/exec"
	"strings"
	"testing"
)

const (
	modulePath   = "github.com/umars28/file-processor"
	pipelinePath = modulePath + "/internal/pipeline"
)

func TestPipelineHasNoInfrastructureDependencies(t *testing.T) {
	out, err := exec.CommandContext(t.Context(), "go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("go list -deps: %v", err)
	}
	forbidden := []string{"net/http", "database/sql"}
	for _, dep := range strings.Fields(string(out)) {
		if isOwnPackage(dep) {
			continue
		}
		if dep == modulePath || strings.HasPrefix(dep, modulePath+"/") {
			t.Errorf("internal/pipeline depends on project package %s", dep)
			continue
		}
		if root, _, _ := strings.Cut(dep, "/"); strings.Contains(root, ".") {
			t.Errorf("internal/pipeline depends on third-party package %s", dep)
			continue
		}
		for _, f := range forbidden {
			if dep == f || strings.HasPrefix(dep, f+"/") {
				t.Errorf("internal/pipeline depends on %s", dep)
			}
		}
	}
}

func isOwnPackage(dep string) bool {
	return dep == pipelinePath || strings.HasPrefix(dep, pipelinePath+"/")
}
