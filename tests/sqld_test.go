package tests

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
)

// TestMatchesRealSqld sends the same batches to the handler and to a real
// sqld (SQLD_URL=http://localhost:8085) and compares the responses, ignoring
// timings. Skipped when SQLD_URL is unset.
func TestMatchesRealSqld(t *testing.T) {
	sqldURL := os.Getenv("SQLD_URL")
	if sqldURL == "" {
		t.Skip("SQLD_URL not set")
	}
	e := newEnv(t, 0)

	for _, body := range []string{
		`{"statements": ["SELECT 42 AS num, 'hello' AS msg, NULL AS empty, 1.5 AS f"]}`,
		`{"statements": ["SELECT 1", "SELECT * FROM missing_table_123", "SELECT 2"]}`,
		`{"statements": [{"q": "SELECT ?, :n", "params": [1]}]}`,
		`{"statements": [{"q": "SELECT :a, @b", "params": {":a": 1, "b": "x"}}]}`,
	} {
		got, want := normalized(t, e.srv.URL, body), normalized(t, sqldURL, body)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s\n handler: %v\n    sqld: %v", body, got, want)
		}
	}
}

func normalized(t *testing.T, url, body string) any {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	stripTimings(v)
	return map[string]any{"status": resp.StatusCode, "body": v}
}

func stripTimings(v any) {
	switch x := v.(type) {
	case map[string]any:
		delete(x, "query_duration_ms")
		for _, c := range x {
			stripTimings(c)
		}
	case []any:
		for _, c := range x {
			stripTimings(c)
		}
	}
}
