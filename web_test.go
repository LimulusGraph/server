package main

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWebAndAPIRoutes(t *testing.T) {
	t.Setenv("LIMULUS_TOKEN", "")
	handler := newHandler()
	for _, path := range []string{"/", "/api/health", "/api/missing", "/missing.js"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		want := http.StatusOK
		switch path {
		case "/":
			if _, err := fs.Stat(webFiles, "web/dist/index.html"); err != nil {
				want = http.StatusServiceUnavailable
			}
		case "/api/missing":
			want = http.StatusNotFound
		case "/missing.js":
			want = http.StatusNotFound
			if _, err := fs.Stat(webFiles, "web/dist/index.html"); err != nil {
				want = http.StatusServiceUnavailable
			}
		}
		if response.Code != want {
			t.Errorf("%s: got %d, want %d", path, response.Code, want)
		}
		if path == "/" && want == http.StatusOK && !strings.Contains(response.Body.String(), `id="root"`) {
			t.Fatal("missing editor root")
		}
	}
	entries, _ := fs.ReadDir(webFiles, "web/dist/assets")
	for _, entry := range entries {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/assets/"+entry.Name(), nil))
		if response.Code != http.StatusOK || response.Body.Len() == 0 {
			t.Fatalf("asset %s not served", entry.Name())
		}
	}
}

func TestLocalBrowserOrigin(t *testing.T) {
	t.Setenv("LIMULUS_TOKEN", "")
	for _, origin := range []string{"", "http://example.com", "https://untrusted.invalid", "null"} {
		req := httptest.NewRequest("GET", "http://example.com/api/health", nil)
		req.Header.Set("Origin", origin)
		response := httptest.NewRecorder()
		newHandler().ServeHTTP(response, req)
		want := http.StatusForbidden
		if origin == "" || origin == "http://example.com" {
			want = http.StatusOK
		}
		if response.Code != want {
			t.Errorf("origin %q: got %d, want %d", origin, response.Code, want)
		}
	}
}

func TestManagedServerHidesEditor(t *testing.T) {
	t.Setenv("LIMULUS_TOKEN", "secret")
	response := httptest.NewRecorder()
	newHandler().ServeHTTP(response, httptest.NewRequest("GET", "/", nil))
	if response.Code != http.StatusNotFound {
		t.Fatal(response.Code)
	}
}
