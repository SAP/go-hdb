/*
Copyright 2014 SAP SE

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

package protocol

import (
	"net/url"
	"strconv"
)

const (
	queryLocale     = "locale"
	queryBufferSize = "bufferSize"
	queryFetchSize  = "fetchSize"
	queryTimeout    = "timeout"
)

const (
	defFetchSize = 128
	defTimeout   = 300
)

type sessionPrm struct {
	host, username, password       string
	locale                         string
	bufferSize, fetchSize, timeout int
}

func newSessionPrm(dsn string) (*sessionPrm, error) {

	url, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	var username, password string
	if url.User != nil {
		username = url.User.Username()
		password, _ = url.User.Password()
	}

	values := url.Query()

	bufferSize, err := strconv.Atoi(values.Get(queryBufferSize))

	fetchSize, err := strconv.Atoi(values.Get(queryFetchSize))
	if err != nil {
		fetchSize = defFetchSize
	}
	timeout, err := strconv.Atoi(values.Get(queryTimeout))
	if err != nil {
		timeout = defTimeout
	}

	if trace {
		logger.Printf("bufferSize %d fetchSize %d timeout %d", bufferSize, fetchSize, timeout)
	}

	return &sessionPrm{
		host:       url.Host,
		username:   username,
		password:   password,
		locale:     values.Get(queryLocale),
		bufferSize: bufferSize,
		fetchSize:  fetchSize,
		timeout:    timeout,
	}, nil
}
