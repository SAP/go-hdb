//go:build (!go1.28 && !go_hdb_compat_go1_28) || (go1.28 && !go1.29 && go_hdb_compat_go1_28)

package driver

// Unregister is deprecated.
//
// Deprecated: Unregister no longer performs any action; it exists only to keep
// existing callers compiling.
func Unregister() error { return nil }
