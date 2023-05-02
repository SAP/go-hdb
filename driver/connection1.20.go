//go:build go1.20

// Delete after go1.19 is out of maintenance.

package driver

import (
	"database/sql/driver"
	"errors"
)

func (c *conn) isBad() bool { return errors.Is(c.lastError, driver.ErrBadConn) }
