package lanes_test

import (
	"os/exec"
	"strings"
	"testing"
)

// TestLaneTerminatorLinksNoKafka pins the dependency boundary that the
// observability split exists to create.
//
// This package is imported by sibling bridges that speak no Kafka at all. When
// the Kafka client instrumentation lived in the observability package that
// this one imports, nine Kafka packages linked into each of those binaries:
// build weight, dependency-scanner findings against code that is unreachable
// there, and a third-party attribution obligation for software those binaries
// never call.
//
// Nothing about that failure is visible at compile time, which is why it is
// asserted here rather than left to review.
func TestLaneTerminatorLinksNoKafka(t *testing.T) {
	for _, pkg := range []string{
		"github.com/lightwebinc/teranode-bridge/lanes",
		"github.com/lightwebinc/teranode-bridge/internal/obs",
	} {
		out, err := exec.Command("go", "list", "-deps", pkg).Output()
		if err != nil {
			t.Skipf("go list unavailable: %v", err)
		}
		for _, dep := range strings.Split(string(out), "\n") {
			if strings.Contains(dep, "franz-go") {
				t.Errorf("%s links %s; the Kafka instrumentation belongs in internal/obs/kafka, which only the Kafka user imports", pkg, dep)
			}
		}
	}
}
