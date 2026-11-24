package sim

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestNoForbiddenAPIs statically enforces the determinism rules for simulated
// packages: no real time, goroutines, channels, real networking, or global RNG.
func TestNoForbiddenAPIs(t *testing.T) {
	root := filepath.Join("..", "..")
	dirs := []string{
		"internal/sim",
		"internal/raft",
		"internal/storage",
		"internal/kv",
		"internal/scenarios",
	}

	bannedImports := map[string]bool{
		"math/rand":    true,
		"math/rand/v2": true,
		"crypto/rand":  true,
		"net":          true,
	}
	bannedOSFuncs := map[string]bool{
		"Getenv":    true,
		"LookupEnv": true,
		"Setenv":    true,
	}
	bannedTimeFuncs := map[string]bool{
		"Now":       true,
		"Sleep":     true,
		"After":     true,
		"Tick":      true,
		"AfterFunc": true,
		"NewTimer":  true,
		"NewTicker": true,
	}

	problems := 0
	report := func(path string, format string, args ...any) {
		problems++
		t.Errorf("%s: "+format, append([]any{path}, args...)...)
	}

	for _, dir := range dirs {
		dir := dir
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			isTest := strings.HasSuffix(path, "_test.go")
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
			if err != nil {
				report(path, "parse: %v", err)
				return nil
			}
			imports := make(map[string]string)
			for _, imp := range file.Imports {
				p, err := strconv.Unquote(imp.Path.Value)
				if err != nil {
					continue
				}
				if bannedImports[p] {
					report(path, "forbidden import %q", p)
				}
				if !isTest && (p == "os" || p == "syscall") {
					report(path, "forbidden filesystem/system import %q in simulated code", p)
				}
				local := filepath.Base(p)
				if imp.Name != nil {
					local = imp.Name.Name
				}
				if local != "_" && local != "." {
					imports[local] = p
				}
			}
			ast.Inspect(file, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.GoStmt:
					report(path, "forbidden go statement")
				case *ast.SelectStmt:
					report(path, "forbidden select statement")
				case *ast.SendStmt:
					report(path, "forbidden channel send")
				case *ast.ChanType:
					report(path, "forbidden channel type")
				case *ast.CallExpr:
					sel, ok := node.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					ident, ok := sel.X.(*ast.Ident)
					if !ok {
						return true
					}
					pkg := imports[ident.Name]
					switch pkg {
					case "time":
						if bannedTimeFuncs[sel.Sel.Name] {
							report(path, "forbidden real-time call time.%s", sel.Sel.Name)
						}
					case "os":
						if bannedOSFuncs[sel.Sel.Name] {
							report(path, "forbidden environment call os.%s", sel.Sel.Name)
						}
					}
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	if problems > 0 {
		t.Fatalf("%d forbidden API violation(s)", problems)
	}
}
