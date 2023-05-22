//go:build go1.20

package driver

import (
	"golang.org/x/exp/slog"
)

func (c *connAttrs) Logger() *slog.Logger {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c._logger
}

// SetEmptyDateAsNull sets the EmptyDateAsNull flag of the connector.
func (c *connAttrs) SetLogger(logger *slog.Logger) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c._logger = logger
}
