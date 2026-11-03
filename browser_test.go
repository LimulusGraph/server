package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestBrowserURL(t *testing.T) {
	for _, tc := range []struct{ ip, want string }{
		{"127.0.0.1", "http://127.0.0.1:8123"},
		{"0.0.0.0", "http://127.0.0.1:8123"},
		{"::", "http://[::1]:8123"},
		{"::1", "http://[::1]:8123"},
	} {
		if got := browserURL(&net.TCPAddr{IP: net.ParseIP(tc.ip), Port: 8123}); got != tc.want {
			t.Errorf("got %s, want %s", got, tc.want)
		}
	}
}

func TestBrowserCommands(t *testing.T) {
	url := "http://127.0.0.1:8123"
	for _, tc := range []struct {
		platform, command string
		args              []string
	}{
		{"darwin", "open", []string{url}},
		{"linux", "xdg-open", []string{url}},
		{"windows", "rundll32", []string{"url.dll,FileProtocolHandler", url}},
	} {
		name, args := browserCommand(tc.platform, url)
		if name != tc.command || !reflect.DeepEqual(args, tc.args) {
			t.Fatalf("%s: %s %v", tc.platform, name, args)
		}
	}
}

type browserTransport func(*http.Request) (*http.Response, error)

func (f browserTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestBrowserWaitsForHealth(t *testing.T) {
	requests, opened := 0, 0
	client := &http.Client{Transport: browserTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != "http://localhost:8123/api/health" {
			t.Fatal(r.URL)
		}
		requests++
		status := http.StatusServiceUnavailable
		if requests == 2 {
			status = http.StatusOK
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}
	waitForBrowser(context.Background(), "http://localhost:8123", client, func(_ context.Context, url string) error {
		opened++
		if requests != 2 || url != "http://localhost:8123" {
			t.Fatal("opened before ready or wrong URL")
		}
		return nil
	})
	if opened != 1 {
		t.Fatalf("opened %d times", opened)
	}
}

func TestBrowserDisabledForManagedAndHeadlessServers(t *testing.T) {
	for _, key := range []string{"LIMULUS_TOKEN", "LIMULUS_NO_BROWSER"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "1")
			openBrowserWhenReady(context.Background(), "http://invalid", func(context.Context, string) error {
				t.Fatal("unexpected browser launch")
				return nil
			})
		})
	}
}
