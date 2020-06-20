/*
Copyright 2020 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dial

import (
	"context"
	"net"
	"time"
)

// DialerOptions contains optional parameters that might be used by a Dialer.
type DialerOptions struct {
	Timeout, TCPKeepAlive time.Duration
}

// The Dialer interface needs to be implemented by custom Dialers. A Dialer for providing a custom driver connection
// to the database can be set in the driver.Connector object.
type Dialer interface {
	DialContext(ctx context.Context, address string, options DialerOptions) (net.Conn, error)
}

// DefaultDialer is the default driver Dialer implementation.
var DefaultDialer Dialer = &dialer{}

// default dialer implementation
type dialer struct{}

func (d *dialer) DialContext(ctx context.Context, address string, options DialerOptions) (net.Conn, error) {
	dialer := net.Dialer{Timeout: options.Timeout, KeepAlive: options.TCPKeepAlive}
	return dialer.DialContext(ctx, "tcp", address)
}
