package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"time"
)

func browserURL(addr net.Addr) string {
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return ""
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		if ip.To4() != nil {
			host = "127.0.0.1"
		} else {
			host = "::1"
		}
	}
	return "http://" + net.JoinHostPort(host, port)
}

func browserCommand(platform, url string) (string, []string) {
	switch platform {
	case "darwin":
		return "open", []string{url}
	case "windows":
		return "rundll32", []string{"url.dll,FileProtocolHandler", url}
	default:
		return "xdg-open", []string{url}
	}
}

// Wait for HTTP readiness instead of racing the server or using a fixed delay.
func openBrowserWhenReady(ctx context.Context, url string, open func(context.Context, string) error) {
	if os.Getenv("LIMULUS_TOKEN") != "" || os.Getenv("LIMULUS_NO_BROWSER") == "1" {
		return
	}
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: time.Second}
	defer client.CloseIdleConnections()
	waitForBrowser(ctx, url, client, open)
}

func waitForBrowser(ctx context.Context, url string, client *http.Client, open func(context.Context, string) error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/api/health", nil)
		if err != nil {
			return
		}
		response, err := client.Do(req)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				if err := open(ctx, url); err != nil {
					log.Printf("Could not open browser: %v. Open %s manually.", err, url)
				}
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func launchBrowser(ctx context.Context, url string) error {
	name, args := browserCommand(runtime.GOOS, url)
	if err := exec.CommandContext(ctx, name, args...).Run(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}
