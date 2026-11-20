package placement

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

var guardPatterns = []string{
	`"app=redis"`,
	`"app=rabbitmq"`,
	`n.Type == "cache"`,
	`n.Type == "mq"`,
	`case "message_queue"`,
}

var guardExempt = map[string]struct{}{
	"pkg/placement/guard_allowlist.go": {},
	"pkg/placement/guard_test.go":      {},
}

func TestArchitectureGuard_RoleFromTypePatterns(t *testing.T) {
	t.Parallel()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	ganymedeRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	roots := []string{
		filepath.Join(ganymedeRoot, "pkg"),
		filepath.Join(ganymedeRoot, "images"),
	}
	for _, root := range roots {
		if _, err := os.Stat(root); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatal(err)
		}
		if err := walkGuard(ganymedeRoot, root, func(rel string, content string) {
			if _, skip := guardExempt[rel]; skip {
				return
			}
			if strings.Contains(rel, "_test.go") {
				return
			}
			hits := countPatterns(content)
			if hits == 0 {
				return
			}
			allowed, ok := ArchitectureGuardAllowlist[rel]
			if !ok {
				t.Errorf("new file with role-from-type hits: %s (%d)", rel, hits)
				return
			}
			if hits > allowed {
				t.Errorf("%s: hits %d exceed allowlist %d", rel, hits, allowed)
			}
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func countPatterns(content string) int {
	n := 0
	for _, p := range guardPatterns {
		n += strings.Count(content, p)
	}
	return n
}

func walkGuard(ganymedeRoot, root string, visit func(rel, content string)) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, err := filepath.Rel(ganymedeRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		visit(rel, string(b))
		return nil
	})
}
