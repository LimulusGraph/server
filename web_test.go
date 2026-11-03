package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHeadlessRoutes(t *testing.T) {
	t.Setenv("LIMULUS_TOKEN", "")
	for _, route := range []struct {
		path   string
		status int
	}{
		{"/", http.StatusNotFound},
		{"/assets/main.js", http.StatusNotFound},
		{"/api/health", http.StatusOK},
		{"/api/missing", http.StatusNotFound},
	} {
		response := httptest.NewRecorder()
		newHandler().ServeHTTP(response, httptest.NewRequest("GET", route.path, nil))
		if response.Code != route.status {
			t.Errorf("%s: got %d, want %d", route.path, response.Code, route.status)
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
