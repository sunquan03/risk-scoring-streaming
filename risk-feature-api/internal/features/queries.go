package features

import (
	"embed"
	"fmt"
	"strings"
)

//go:embed sql/*.sql
var sqlFS embed.FS

var aggregationSQL = map[string]string{}

func init() {
	entries, err := sqlFS.ReadDir("sql")
	if err != nil {
		panic(fmt.Sprintf("features: reading embedded sql: %v", err))
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		raw, err := sqlFS.ReadFile("sql/" + name)
		if err != nil {
			panic(fmt.Sprintf("features: reading %s: %v", name, err))
		}
		aggregationSQL[strings.TrimSuffix(name, ".sql")] = prepareQuery(string(raw))
	}
}

func prepareQuery(raw string) string {
	var b strings.Builder
	for _, line := range strings.Split(raw, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}

	query := strings.TrimSpace(b.String())
	query = strings.TrimSuffix(query, ";")
	return strings.ReplaceAll(query, ":client_id", "?")
}
