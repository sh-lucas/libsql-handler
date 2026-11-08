package tests

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// The handler must work behind http.StripPrefix whether the stripped prefix
// keeps the leading slash ("/libsql") or not ("/libsql/").
func TestMountedUnderPrefix(t *testing.T) {
	for _, strip := range []string{"/libsql", "/libsql/"} {
		t.Run(strip, func(t *testing.T) {
			e := newEnv(t, 0)
			mux := http.NewServeMux()
			mux.Handle("/libsql/", http.StripPrefix(strip, e.h))
			srv := httptest.NewServer(mux)
			t.Cleanup(srv.Close)
			base := srv.URL + "/libsql"

			for _, p := range []string{"/version", "/v2", "/v3"} {
				resp, err := http.Get(base + p)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("GET %s: status %d", p, resp.StatusCode)
				}
			}

			for _, p := range []string{"/v2/pipeline", "/v3/pipeline"} {
				s := &hstream{t: t, url: base, path: p}
				if got := s.scalar("SELECT 41 + 1").String(); got != `integer:"42"` {
					t.Fatalf("%s: got %s", p, got)
				}
				s.run(closeReq)
			}

			if status, _, rows, _ := cursor(t, base, nil, execSQL("SELECT 1")); status != http.StatusOK || len(rows) == 0 {
				t.Fatalf("cursor: status %d, %d entries", status, len(rows))
			}

			// v1 (DBeaver) posts to the base URL itself, with and without the slash.
			for _, p := range []string{"", "/"} {
				status, body := postJSON(t, base, p, map[string]any{"statements": []string{"SELECT 1"}})
				if status != http.StatusOK {
					t.Fatalf("v1 POST %q: status %d: %s", p, status, body)
				}
			}
		})
	}
}
