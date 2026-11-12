package main

// Regenerate docs/DEPRECATED.md from the compat-tagged source. Paths are
// relative to this directory (go generate runs each directive in its own
// package directory).
//go:generate go run . -root ../../driver -out ../../docs/DEPRECATED.md
