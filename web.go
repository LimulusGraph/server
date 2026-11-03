package main

import "net/http"

// The server exposes only the execution API. Clients own all browser assets.
// Deliberately ignore any legacy web/dist directory left in a local checkout.
func webHandler() http.Handler { return http.NotFoundHandler() }
