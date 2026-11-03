package main

import (
	"embed"
	"io/fs"
	"net/http"
	"os"
)

// Keep the README so API-only builds also work before the frontend is built.
//
//go:embed web
var webFiles embed.FS

func webHandler() http.Handler {
	bundle, err := fs.Sub(webFiles, "web/dist")
	available := err == nil
	if available {
		_, err = fs.Stat(bundle, "index.html")
		available = err == nil
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Token-protected servers belong to the VS Code extension, not a browser session.
		if os.Getenv("LIMULUS_TOKEN") != "" {
			http.NotFound(w, r)
			return
		}
		if !available {
			http.Error(w, "Editor not built. Run npm ci && npm run build in client, then rebuild/restart the server.", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-cache")
		http.FileServer(http.FS(bundle)).ServeHTTP(w, r)
	})
}
